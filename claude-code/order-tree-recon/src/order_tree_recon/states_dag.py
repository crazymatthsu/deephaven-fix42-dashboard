"""Latest order state -> the per-level canonical tables, in Deephaven (doc 14 section 12).

The generic half of the order-state path: a table with one row per order-state message and
one column per mapped field (text or typed) goes in, one canonical table per level comes out.
:mod:`order_tree_recon.fixstate_dag` puts FIX tags into such columns (``T<tag>``) and the
AMPS SOW mirror in front of it. The python twin is :func:`order_tree_recon.states.fold_system`;
the embedded-engine test asserts both produce the same rows.

::

    messages
      |-> OtrKey = the order id;  drop rows without one
      |-> latest state per order:  sorted_last by (sequence, arrival)  or  last_by
      |     '-> level from the ``when`` rules, canonical columns (text / numbers / codes)
      |           '-> one view per level  ------------------------------------------------.
      '-> ids: (System, Id, OrderId) for every id field of every message  ----------------+
                                                    dag.build_order_tree(..., extra_ids=...)
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import jpy
from deephaven import agg, dtypes, merge, new_table
from deephaven.column import string_col
from deephaven.table import Table

from order_tree_recon import states
from order_tree_recon.config import Topology
from order_tree_recon.states import StateConfig, StateSystem

__all__ = [
    "state_table",
    "check_columns",
    "system_levels",
    "system_ids",
    "state_level_tables",
    "build_from_states",
]

_NUMERIC = (dtypes.int8, dtypes.int16, dtypes.int32, dtypes.int64, dtypes.float32, dtypes.float64)
#: Compiled once and referenced by name in the formulas below (Deephaven resolves it from
#: this module's scope): ``String.matches`` would recompile the pattern on every value.
OTR_NUMBER_PATTERN = jpy.get_type("java.util.regex.Pattern").compile(states.NUMBER_PATTERN)


def state_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> Table:
    """A static, all-text table with ``columns`` (``""`` where a row lacks one)."""
    return new_table([string_col(name, [str(row.get(name, "")) for row in rows]) for name in columns])


def _types(table: Table) -> Dict[str, Any]:
    return {column.name: column.data_type for column in table.columns}


def check_columns(system: StateSystem, table: Table) -> Dict[str, Any]:
    """The table's column types, after checking it has every field the mapping reads.

    Raises:
        ValueError: naming the missing columns.
    """
    types = _types(table)
    missing = [name for name in system.field_names if name not in types]
    if missing:
        raise ValueError(f"{system.system} messages have no column(s) {missing}; columns: {sorted(types)}")
    return types


def _text(name: str, types: Mapping[str, Any]) -> str:
    """A column as trimmed text (``""`` for null) -- what ``states.text`` does."""
    if types[name] == dtypes.string:
        return f"(isNull({name}) ? `` : {name}.trim())"
    return f"(isNull({name}) ? `` : String.valueOf({name}))"


def _number(name: str, types: Mapping[str, Any]) -> str:
    """A column as a double: a numeric column as is, text only if it is a plain decimal."""
    dtype = types[name]
    if dtype == dtypes.string:
        return (f"(isNull({name}) ? NULL_DOUBLE : (OTR_NUMBER_PATTERN.matcher({name}.trim()).matches()"
                f" ? Double.parseDouble({name}.trim()) : NULL_DOUBLE))")
    if dtype in _NUMERIC:
        return f"(isNull({name}) ? NULL_DOUBLE : (double) {name})"
    raise ValueError(f"column {name} is {dtype}; a quantity, price or sequence must be text or numeric")


def _coded(column: str, codes: Sequence[Tuple[str, str]]) -> str:
    expr = column
    for code, name in reversed(list(codes)):
        expr = f"({column} == `{code}` ? `{name}` : {expr})"
    return expr


def _when(rule: states.StateLevelRule, types: Mapping[str, Any]) -> str:
    if rule.when is None:
        return "true"
    value = _text(rule.when.field, types)
    if rule.when.equals is None:
        return f"{value} != ``"
    return f"{value} == `{rule.when.equals}`"


def _ordering(system: StateSystem, types: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    """``(helper formulas, order_by columns)`` for ``sequence`` then ``arrival``.

    A text column orders numerically when it is a number and as text otherwise (states'
    ``_order_key``: a null number sorts first); a numeric or timestamp column as is.
    """
    formulas: List[str] = []
    order_by: List[str] = []
    for i, name in enumerate(f for f in (system.sequence, system.arrival) if f):
        if types[name] == dtypes.string:
            formulas += [f"OtrOrd{i}Num = {_number(name, types)}"]
            order_by += [f"OtrOrd{i}Num"]
            if name == system.sequence:
                formulas += [f"OtrOrd{i}Text = {_text(name, types)}"]
                order_by += [f"OtrOrd{i}Text"]
        else:
            order_by.append(name)
    return formulas, order_by


def system_levels(system: StateSystem, messages: Table) -> Dict[str, Table]:
    """One system's messages -> ``{level: canonical table}`` (latest state per order)."""
    types = check_columns(system, messages)
    helpers, order_by = _ordering(system, types)
    updates = messages.update_view([f"OtrKey = {_text(system.order_id, types)}"] + helpers).where("OtrKey != ``")
    if order_by:
        hidden = {"OtrKey"} | {c for c in order_by if c.startswith("OtrOrd")}
        columns = [c for c in updates.column_names if c not in hidden]
        latest = updates.agg_by([agg.sorted_last(order_by=order_by, cols=columns)], by=["OtrKey"])
    else:
        latest = updates.last_by(["OtrKey"])

    level = "(String) null"
    for rule in reversed(system.levels):
        level = f"{_when(rule, types)} ? `{rule.level}` : ({level})"

    def text_of(target: str) -> str:
        source = system.source(target)
        return _text(source, types) if source else "``"

    def number_of(target: str) -> str:
        source = system.source(target)
        return _number(source, types) if source else "NULL_DOUBLE"

    avg_px = number_of("AvgPx")
    if system.avg_px_needs_fill:
        avg_px = f"(!isNull(OtrCumQty) && OtrCumQty > 0) ? {avg_px} : NULL_DOUBLE"
    staged = latest.update([
        f"OtrLevel = {level}",
        f"OtrAltId = {_text(system.alt_id, types) if system.alt_id else '``'}",
        f"OtrMoniker = {text_of('Moniker')}",
        f"OtrSymbol = {text_of('Symbol')}",
        f"OtrSideText = {text_of('Side')}",
        f"OtrStatusText = {text_of('Status')}",
        f"OtrDestination = {text_of('Destination')}",
        f"OtrOrderQty = {number_of('OrderQty')}",
        f"OtrCumQty = {number_of('CumQty')}",
        f"OtrLeavesQty = {number_of('LeavesQty')}",
        f"OtrAvgPx = {avg_px}",
    ])
    common = [
        "OrderId = OtrKey",
        "AltId = OtrAltId",
        "Moniker = OtrMoniker",
        "Symbol = OtrSymbol",
        f"Side = {_coded('OtrSideText', system.side_codes)}",
        "OrderQty = OtrOrderQty",
        "CumQty = OtrCumQty",
        "LeavesQty = OtrLeavesQty",
        "AvgPx = OtrAvgPx",
        f"Status = {_coded('OtrStatusText', system.status_codes)}",
        "Destination = OtrDestination",
    ]
    levels: Dict[str, Table] = {}
    for rule in system.levels:
        link = _text(rule.link, types) if rule.link else "``"
        levels[rule.level] = staged.where(f"OtrLevel == `{rule.level}`").view([f"LinkId = {link}"] + common)
    return levels


def system_ids(system: StateSystem, messages: Table) -> Table:
    """``System, Id, OrderId`` for every id field of every message, distinct.

    ``count_by`` rather than ``select_distinct``: an aggregation keeps its result when
    ``messages`` is a blink table, so this also works straight on a live stream.
    """
    types = check_columns(system, messages)
    key = _text(system.order_id, types)
    parts = [
        messages.view([f"System = `{system.system}`", f"Id = {_text(name, types)}", f"OrderId = {key}"])
        .where(["Id != ``", "OrderId != ``"])
        for name in system.id_fields
    ]
    return merge(parts).count_by("OtrCount", by=["System", "Id", "OrderId"]).view(["System", "Id", "OrderId"])


def state_level_tables(
    topology: Topology,
    config: StateConfig,
    messages: Mapping[str, Table],
) -> Tuple[Dict[str, Table], Table]:
    """One canonical table per topology level, and the id table, from order-state messages.

    Args:
        topology: The level topology the config was validated against.
        config: A validated :class:`~order_tree_recon.states.StateConfig`.
        messages: ``{system: message table}`` -- static or ticking, text or typed columns.
            A system missing here contributes empty levels.

    Returns:
        ``(level_tables, extra_ids)``: pass both to :func:`order_tree_recon.dag.build_order_tree`.
    """
    level_tables: Dict[str, Table] = {}
    id_tables: List[Table] = []
    for system in config.systems:
        table = messages.get(system.system)
        if table is None:
            table = state_table([], system.field_names)
        level_tables.update(system_levels(system, table))
        id_tables.append(system_ids(system, table))
    missing = [name for name in topology.names if name not in level_tables]
    if missing:
        raise ValueError(f"the mapping covers no system for level(s) {missing}")
    return level_tables, merge(id_tables)


def build_from_states(
    topology: Topology,
    config: StateConfig,
    messages: Mapping[str, Table],
    qty_tol: Optional[float] = None,
) -> Dict[str, Any]:
    """Order-state message tables -> every ``otr_*`` table (``state_level_tables`` + the DAG)."""
    from order_tree_recon import dag

    level_tables, extra_ids = state_level_tables(topology, config, messages)
    return dag.build_order_tree(topology, level_tables, qty_tol, extra_ids=extra_ids)
