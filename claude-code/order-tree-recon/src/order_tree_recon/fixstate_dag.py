"""FIX order-state events -> the per-level canonical tables, in Deephaven (doc 14 section 12).

The input is one **event table** per system (:data:`order_tree_recon.fixstate.EVENT_COLUMNS`):

* :class:`order_tree_recon.amps_sow.AmpsSowSource` -- a live blink table fed by
  ``sow_and_subscribe`` on every AMPS SOW topic of the system;
* :func:`file_event_tables` -- SOW dumps / recorded subscriptions read from files (the demo);
* anything else with those columns (a test, a replay).

::

    events --pick (python UDF, one call per message)--> T<tag> columns
      |-> SOW mirror: last_by(Topic, SowKey | order id), DELETE removes, stale epochs drop
      |     '-> latest state per order, levels, canonical columns   (states_dag.system_levels)
      '-> id history: every id tag of every UPSERT                   (states_dag.system_ids)
                                         '-> dag.build_order_tree(level_tables, extra_ids=...)

Aggregations keep their state when the input is a blink table, so the mirror and the id
history hold every order without the events being retained. The python twin is
:func:`order_tree_recon.fixstate.fold_events`; the embedded-engine test asserts they agree.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from deephaven import merge, new_table
from deephaven.column import int_col, long_col, string_col
from deephaven.table import Table

from order_tree_recon import fixstate, states_dag
from order_tree_recon.config import Topology
from order_tree_recon.fixstate import SNAPSHOT_END, UPSERT, FixStateConfig

__all__ = [
    "event_table",
    "file_event_tables",
    "parsed_events",
    "sow_mirror",
    "fix_level_tables",
    "build_from_fix_states",
]

Events = Union[Table, Sequence[Table]]


def event_table(events: Sequence[Mapping[str, Any]]) -> Table:
    """A static event table (:data:`~order_tree_recon.fixstate.EVENT_COLUMNS`)."""
    return new_table([
        string_col("Topic", [str(e.get("Topic", "")) for e in events]),
        string_col("SowKey", [str(e.get("SowKey", "")) for e in events]),
        string_col("Action", [str(e.get("Action", UPSERT)) for e in events]),
        int_col("Epoch", [int(e.get("Epoch", 1)) for e in events]),
        long_col("Arrival", [int(e.get("Arrival", i + 1)) for i, e in enumerate(events)]),
        string_col("RawFix", [str(e.get("RawFix", "")) for e in events]),
    ])


def file_event_tables(config: FixStateConfig, base_dir: str) -> Tuple[Dict[str, Table], Dict[str, List[str]]]:
    """Each system's ``files`` as a static event table (see :func:`fixstate.file_events`)."""
    events, files = fixstate.file_events(config, base_dir)
    return {system: event_table(rows) for system, rows in events.items()}, files


def parsed_events(config: FixStateConfig, system: str, events: Table) -> Table:
    """``events`` plus one ``T<tag>`` text column per tag the mapping reads for ``system``.

    ``pick`` is a closure over the system's tag list; Deephaven resolves python names in a
    formula from the calling scope, so it runs once per event (``update`` materializes) and
    its ``Sequence[str]`` result arrives as a ``String[]`` column.
    """
    tags = config.tags(system)

    def pick(raw: Optional[str]) -> Sequence[str]:
        return fixstate.fix_values(raw, tags)

    columns = [f"{fixstate.tag_column(tag)} = OtrPicked[{i}]" for i, tag in enumerate(tags)]
    return events.update(["OtrPicked = pick(RawFix)"]).update(columns).drop_columns(["OtrPicked"])


def sow_mirror(parsed: Table, order_id_tag: str = "37") -> Table:
    """The current SOW records among ``parsed`` events (see :func:`fixstate.sow_current`).

    Latest event per ``(Topic, SowKey | order id)``; ``DELETE`` removes the record; a record
    older than its topic's latest ``SNAPSHOT_END`` epoch is gone from the SOW.
    """
    oid = fixstate.tag_column(order_id_tag)
    records = (
        parsed.where(f"Action != `{SNAPSHOT_END}`")
        .update_view([f"OtrSowKey = (!isNull(SowKey) && !SowKey.trim().isEmpty()) ? SowKey.trim()"
                      f" : (isNull({oid}) ? `` : {oid}.trim())"])
        .where("OtrSowKey != ``")
        .last_by(["Topic", "OtrSowKey"])
    )
    done = parsed.where(f"Action == `{SNAPSHOT_END}`").last_by(["Topic"]).view(["Topic", "OtrDoneEpoch = Epoch"])
    return (
        records.natural_join(done, on=["Topic"], joins=["OtrDoneEpoch"])
        .where(f"Action == `{UPSERT}` && (isNull(OtrDoneEpoch) || Epoch >= OtrDoneEpoch)")
        .drop_columns(["OtrDoneEpoch"])
    )


def _merged(events: Optional[Events]) -> Optional[Table]:
    if events is None:
        return None
    if isinstance(events, Table):
        return events
    tables = list(events)
    return tables[0] if len(tables) == 1 else merge(tables)


def fix_level_tables(
    topology: Topology,
    config: FixStateConfig,
    events: Mapping[str, Events],
) -> Tuple[Dict[str, Table], Table, Dict[str, Table]]:
    """FIX order-state events -> per-level tables, the id history and each system's SOW mirror.

    Args:
        topology: The level topology the config was validated against.
        config: From :func:`order_tree_recon.fixstate.load_fix_config`.
        events: ``{system: event table, or a list of them}`` -- static, append-only or blink.
            A system missing here contributes empty levels.

    Returns:
        ``(level_tables, extra_ids, current)``: the first two go to
        :func:`order_tree_recon.dag.build_order_tree`; ``current[system]`` is the mirror
        (one row per live SOW record, with its ``T<tag>`` columns) for inspection.
    """
    level_tables: Dict[str, Table] = {}
    id_tables: List[Table] = []
    current: Dict[str, Table] = {}
    for system in config.systems:
        table = _merged(events.get(system.system))
        if table is None:
            table = event_table([])
        parsed = parsed_events(config, system.system, table)
        mirror = sow_mirror(parsed, config.order_id_tags[system.system])
        level_tables.update(states_dag.system_levels(system, mirror))
        id_tables.append(states_dag.system_ids(system, parsed.where(f"Action == `{UPSERT}`")))
        current[system.system] = mirror
    missing = [name for name in topology.names if name not in level_tables]
    if missing:
        raise ValueError(f"fix config maps no system to level(s) {missing}")
    return level_tables, merge(id_tables), current


def build_from_fix_states(
    topology: Topology,
    config: FixStateConfig,
    events: Mapping[str, Events],
    qty_tol: Optional[float] = None,
) -> Dict[str, Any]:
    """FIX order-state events -> every ``otr_*`` table (``fix_level_tables`` + the DAG)."""
    from order_tree_recon import dag

    level_tables, extra_ids, _ = fix_level_tables(topology, config, events)
    return dag.build_order_tree(topology, level_tables, qty_tol, extra_ids=extra_ids)
