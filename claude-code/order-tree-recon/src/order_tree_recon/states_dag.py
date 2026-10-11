"""Order-state messages -> the per-level canonical tables, in Deephaven (doc 14 section 12).

The input is **one table per system with one row per order-state message**, whatever it
came from:

* :func:`state_file_tables` -- CSV / JSON Lines files read at startup (the demo; every
  column text);
* a Kafka topic consumed with ``deephaven.stream.kafka.consumer`` and ``json_spec`` (typed
  columns -- numeric quantities, a numeric or timestamp sequence -- work as they are);
* an append-only input table, a table publisher, a JDBC poll, ...

Each system's columns are mapped by a :class:`order_tree_recon.states.StateConfig`. Every
step is a Deephaven operation, so a ticking message table gives a ticking order tree. The
python twin is :func:`order_tree_recon.states.fold_system`; the embedded-engine test
asserts both produce the same rows.

::

    messages (one table per system)
      |-> OtrKey = the order id;  drop rows without one
      |-> latest state per order:  last_by(OtrKey)  or  sorted_last by the sequence field
      |     '-> level from the ``when`` rules, canonical columns (text / numbers / codes)
      |           '-> one view per level  ------------------------------------------------.
      '-> id history: (System, Id, OrderId) for every id field of every message  --------+
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
    "SOURCE_COLUMN",
    "state_table",
    "state_file_tables",
    "state_level_tables",
    "build_from_states",
]

#: The column :func:`state_file_tables` adds: the file each message came from.
SOURCE_COLUMN = "OtrSourceFile"

_NUMERIC = (dtypes.int8, dtypes.int16, dtypes.int32, dtypes.int64, dtypes.float32, dtypes.float64)
#: Compiled once and referenced by name in the formulas below (Deephaven resolves it from
#: this module's scope): ``String.matches`` would recompile the pattern on every value.
OTR_NUMBER_PATTERN = jpy.get_type("java.util.regex.Pattern").compile(states.NUMBER_PATTERN)


def state_table(rows: Sequence[Mapping[str, str]], columns: Sequence[str]) -> Table:
    """A static, all-text message table with ``columns`` (``""`` where a row lacks one)."""
    return new_table([string_col(name, [str(row.get(name, "")) for row in rows]) for name in columns])


def state_file_tables(
    config: StateConfig, base_dir: str
) -> Tuple[Dict[str, Table], Dict[str, List[str]], Dict[str, int]]:
    """Read each system's ``files`` globs under ``base_dir`` into a message table.

    Every column a file has is kept (as text), every field the mapping reads is
    guaranteed to exist (empty when no file has it), and ``OtrSourceFile`` names the file.

    Returns:
        ``({system: table}, {system: [files read]}, {system: unusable lines})``.
    """
    tables: Dict[str, Table] = {}
    files_by_system: Dict[str, List[str]] = {}
    bad_by_system: Dict[str, int] = {}
    for system in config.systems:
        rows: List[Dict[str, str]] = []
        columns: List[str] = []
        bad = 0
        files = states.match_files(system.files, base_dir)
        for path in files:
            file_rows, file_bad = states.read_state_file(path)
            bad += file_bad
            name = path.replace("\\", "/").rsplit("/", 1)[-1]
            for row in file_rows:
                row[SOURCE_COLUMN] = name
                for column in row:
                    if column not in columns:
                        columns.append(column)
            rows.extend(file_rows)
        columns += [f for f in system.field_names if f not in columns]
        if SOURCE_COLUMN not in columns:
            columns.append(SOURCE_COLUMN)
        tables[system.system] = state_table(rows, columns)
        files_by_system[system.system] = files
        bad_by_system[system.system] = bad
    return tables, files_by_system, bad_by_system


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
    raise ValueError(f"column {name} is {dtype}; a quantity or price must be text or numeric")


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


def _system_tables(system: StateSystem, messages: Table) -> Tuple[Dict[str, Table], Table]:
    """Fold one system's messages into ``({level: canonical table}, ids)``."""
    types = {column.name: column.data_type for column in messages.columns}
    missing = [name for name in system.field_names if name not in types]
    if missing:
        raise ValueError(f"{system.system} messages have no column(s) {missing}; columns: {sorted(types)}")

    keyed = [f"OtrKey = {_text(system.order_id, types)}"]
    order_by: List[str] = []
    if system.sequence is not None:
        if types[system.sequence] == dtypes.string:
            # Numbers numerically, anything else (ISO timestamps) as text -- states._sequence_key.
            keyed += [f"OtrSeqNum = {_number(system.sequence, types)}", f"OtrSeqText = {_text(system.sequence, types)}"]
            order_by = ["OtrSeqNum", "OtrSeqText"]
        else:
            order_by = [system.sequence]
    updates = messages.update_view(keyed).where("OtrKey != ``")
    if order_by:
        columns = [c for c in updates.column_names if c not in ("OtrKey", "OtrSeqNum", "OtrSeqText")]
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
        f"OtrAvgPx = {number_of('AvgPx')}",
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

    ids = merge([
        updates.view([f"System = `{system.system}`", f"Id = {_text(name, types)}", "OrderId = OtrKey"]).where("Id != ``")
        for name in system.id_fields
    ])
    return levels, ids


def state_level_tables(
    topology: Topology,
    config: StateConfig,
    messages: Mapping[str, Table],
) -> Tuple[Dict[str, Table], Table]:
    """One canonical table per topology level, and the id-history table, from order states.

    Args:
        topology: The level topology the config was validated against.
        config: From :func:`order_tree_recon.states.load_state_config`.
        messages: ``{system: message table}`` -- static or ticking, text or typed columns.
            A system missing here contributes empty levels.

    Returns:
        ``(level_tables, extra_ids)``: pass both to :func:`order_tree_recon.dag.build_order_tree`.

    Raises:
        ValueError: a table lacks a column the mapping reads, or a quantity column is
            neither text nor numeric.
    """
    level_tables: Dict[str, Table] = {}
    id_tables: List[Table] = []
    for system in config.systems:
        table = messages.get(system.system)
        if table is None:
            table = state_table([], system.field_names)
        levels, ids = _system_tables(system, table)
        level_tables.update(levels)
        id_tables.append(ids)
    missing = [name for name in topology.names if name not in level_tables]
    if missing:
        raise ValueError(f"state config maps no system to level(s) {missing}")
    return level_tables, merge(id_tables).select_distinct(["System", "Id", "OrderId"])


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
