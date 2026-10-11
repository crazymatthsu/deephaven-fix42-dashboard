"""Raw FIX messages -> the per-level canonical tables, in Deephaven (doc 14 section 12).

The input is **any table with a string column** ``RawFix`` (one FIX message per row):

* :func:`fix_file_tables` -- static tables read from drop-copy log files (the demo);
* a Kafka topic consumed with ``deephaven.stream.kafka.consumer`` and its value as a string;
* an append-only input table, a table publisher fed by your FIX engine, ...

Every step is a Deephaven operation, so a ticking message table produces ticking order
state, a ticking order tree and ticking breaks. The python twin of this module is
:func:`order_tree_recon.fix.fold_system`; the embedded-engine test asserts both produce
the same rows.

::

    RawFix --pick (python UDF, one call per message)--> T35, T37, T11, T39, T14, ...
      execs = MsgType 8 with an order id
        |-> per state tag:  where(T != "").last_by(Key)    latest non-empty value
        |-> per sticky tag: where(T != "").first_by(Key)   first non-empty value
        '-> per id tag:     (System, Id, OrderId)          every id the order carried
      orders = distinct Key, natural_join every value -> Level from the ``when`` rules
        '-> one canonical view per level  -->  dag.build_order_tree(..., extra_ids=ids)
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from deephaven import merge, new_table
from deephaven.column import string_col
from deephaven.table import Table

from order_tree_recon import fix
from order_tree_recon.config import Topology
from order_tree_recon.fix import FixConfig, FixSystem

__all__ = [
    "RAW_COLUMN",
    "fix_messages_table",
    "fix_file_tables",
    "fix_level_tables",
    "build_from_fix",
]

#: The one column a message table must have.
RAW_COLUMN = "RawFix"


def fix_messages_table(messages: Sequence[str], sources: Optional[Sequence[str]] = None) -> Table:
    """A static message table: ``RawFix`` (and ``Source``, e.g. the file, when given)."""
    columns = [string_col(RAW_COLUMN, list(messages))]
    if sources is not None:
        columns.append(string_col("Source", list(sources)))
    return new_table(columns)


def fix_file_tables(fix_config: FixConfig, base_dir: str) -> Tuple[Dict[str, Table], Dict[str, List[str]]]:
    """Read each system's ``files`` globs under ``base_dir`` into a message table.

    Returns:
        ``({system: table}, {system: [files read]})`` -- a system whose globs match
        nothing gets an empty table (its levels are then simply empty).
    """
    tables: Dict[str, Table] = {}
    files_by_system: Dict[str, List[str]] = {}
    for system in fix_config.systems:
        messages: List[str] = []
        sources: List[str] = []
        _, files = fix.read_fix_files(system.files, base_dir)
        for path in files:
            lines = fix.read_fix_lines(path)
            messages.extend(lines)
            sources.extend([path.replace("\\", "/").rsplit("/", 1)[-1]] * len(lines))
        tables[system.system] = fix_messages_table(messages, sources)
        files_by_system[system.system] = files
    return tables, files_by_system


def _picked(system: FixSystem, messages: Table) -> Table:
    """One ``T<tag>`` string column per configured tag, plus ``Key`` (the order id).

    ``pick`` is a closure over this system's tag list. Deephaven resolves python names in
    a formula from the calling scope, so it is called once per message by ``update``
    (materialized: never re-parsed), and its ``Sequence[str]`` result arrives as a
    ``String[]`` column.
    """
    tags = system.tags

    def pick(raw: Optional[str]) -> Sequence[str]:
        return fix.pick_values(raw or "", tags)

    columns = [f"T{tag} = Picked_[{i}]" for i, tag in enumerate(tags)]
    return (
        messages.update(["Picked_ = pick(RawFix)"])
        .update(columns + [f"Key = T{system.order_id_tag}"])
        .drop_columns(["Picked_"])
    )


def _when(rule: fix.FixLevelRule) -> str:
    if rule.when is None:
        return "true"
    column = f"First{rule.when.tag}"
    if rule.when.equals is None:
        return f"{column} != ``"
    return f"{column} == `{rule.when.equals}`"


def _number(column: str) -> str:
    """``""`` -> null, else the double (``fix.pick_values`` only lets plain numbers through)."""
    return f"{column} == `` ? NULL_DOUBLE : Double.parseDouble({column})"


def _system_tables(system: FixSystem, messages: Table) -> Tuple[Dict[str, Table], Table]:
    """Fold one system's messages into ``({level: canonical table}, ids)``."""
    execs = _picked(system, messages).where(["T35 == `8`", "Key != ``"])
    orders = execs.select_distinct(["Key"])
    value_columns: List[str] = []
    for tag in system.state_tags:
        latest = execs.where(f"T{tag} != ``").last_by(["Key"]).view(["Key", f"Last{tag} = T{tag}"])
        orders = orders.natural_join(latest, on=["Key"], joins=[f"Last{tag}"])
        value_columns.append(f"Last{tag}")
    for tag in system.sticky_tags:
        first = execs.where(f"T{tag} != ``").first_by(["Key"]).view(["Key", f"First{tag} = T{tag}"])
        orders = orders.natural_join(first, on=["Key"], joins=[f"First{tag}"])
        value_columns.append(f"First{tag}")

    level = "(String) null"
    for rule in reversed(system.levels):
        level = f"{_when(rule)} ? `{rule.level}` : ({level})"
    orders = orders.update([f"{c} = isNull({c}) ? `` : {c}" for c in value_columns] + [f"Level = {level}"])

    moniker = f"First{system.moniker_tag}" if system.moniker_tag else "``"
    destination = f"Last{system.destination_tag}" if system.destination_tag else "``"
    common = [
        "OrderId = Key",
        "AltId = Last11",
        f"Moniker = {moniker}",
        "Symbol = Last55",
        "Side = Last54",
        f"OrderQty = {_number('Last38')}",
        f"CumQty = {_number('Last14')}",
        f"LeavesQty = {_number('Last151')}",
        "AvgPx = (Last14 != `` && Double.parseDouble(Last14) > 0 && Last6 != ``)"
        " ? Double.parseDouble(Last6) : NULL_DOUBLE",
        "Status = Last39",
        f"Destination = {destination}",
    ]
    levels: Dict[str, Table] = {}
    for rule in system.levels:
        link = f"First{rule.link_tag}" if rule.link_tag else "``"
        levels[rule.level] = orders.where(f"Level == `{rule.level}`").view([f"LinkId = {link}"] + common)

    ids = merge(
        [
            execs.where(f"T{tag} != ``").view([f"System = `{system.system}`", f"Id = T{tag}", "OrderId = Key"])
            for tag in system.id_tags
        ]
    )
    return levels, ids


def fix_level_tables(
    topology: Topology,
    fix_config: FixConfig,
    messages: Mapping[str, Table],
) -> Tuple[Dict[str, Table], Table]:
    """One canonical table per topology level, and the id-history table, from FIX messages.

    Args:
        topology: The level topology the config was validated against.
        fix_config: From :func:`order_tree_recon.fix.load_fix_config`.
        messages: ``{system: table with a RawFix column}`` -- static or ticking. A system
            missing here contributes empty levels.

    Returns:
        ``(level_tables, extra_ids)``: pass both to :func:`order_tree_recon.dag.build_order_tree`.
    """
    level_tables: Dict[str, Table] = {}
    id_tables: List[Table] = []
    for system in fix_config.systems:
        table = messages.get(system.system)
        if table is None:
            table = fix_messages_table([])
        levels, ids = _system_tables(system, table)
        level_tables.update(levels)
        id_tables.append(ids)
    missing = [name for name in topology.names if name not in level_tables]
    if missing:
        raise ValueError(f"fix config maps no system to level(s) {missing}")
    extra_ids = merge(id_tables).select_distinct(["System", "Id", "OrderId"])
    return level_tables, extra_ids


def build_from_fix(
    topology: Topology,
    fix_config: FixConfig,
    messages: Mapping[str, Table],
    qty_tol: Optional[float] = None,
) -> Dict[str, Any]:
    """FIX message tables -> every ``otr_*`` table (``fix_level_tables`` + ``build_order_tree``)."""
    from order_tree_recon import dag

    level_tables, extra_ids = fix_level_tables(topology, fix_config, messages)
    return dag.build_order_tree(topology, level_tables, qty_tol, extra_ids=extra_ids)
