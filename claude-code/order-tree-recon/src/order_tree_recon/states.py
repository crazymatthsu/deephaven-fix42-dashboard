"""Order-state messages -> the canonical order-tree levels: mapping config, files, fold (doc 14 §12).

Each system sends **order states**: one message per change, each the order's *complete*
current state (ids, parent id, quantities, status). So an order's state is simply its
latest message -- by arrival, or by a ``sequence`` field when messages can arrive out of
order -- and every id it was ever sent with is kept as its id history, so a child that
still names an amended-away id links.

Pure python: :mod:`order_tree_recon.states_dag` does the same with Deephaven operations
(``last_by`` / ``sorted_last``, ``where``, ``view``) and the embedded-engine test asserts
both produce identical rows. Every value is handled as text here, as it is in a file:

* text -- trimmed (Java ``String.trim`` rules); a missing field is ``""``;
* numbers -- a plain decimal (:data:`NUMBER_PATTERN`) or null: a bad value never stops a table;
* codes -- ``side_codes`` / ``status_codes`` translate a system's codes (``"fix"`` = FIX 4.2);
* the level -- the first ``levels`` rule whose ``when`` matches the latest state.

The mapping config (JSON)
-------------------------
One entry per system: which field is the order id, which canonical column each field
fills, and how to tell its levels apart. :data:`DEFAULT_STATE_CONFIG` maps the mock tables'
own field names and is what ``state-samples/state_config.json`` holds::

    {"system": "ALGO", "files": ["algo*.jsonl"], "order_id": "AlgoOrderId", "sequence": "Version",
     "fields": {"Symbol": "Symbol", "Side": "Side", "OrderQty": "Qty", "CumQty": "Filled",
                "LeavesQty": "Open", "AvgPx": "AvgPrice", "Status": "State", "Destination": "Strategy"},
     "levels": [{"level": "ALGO_CHILD", "when": {"field": "ParentAlgoOrderId"}, "link": "ParentAlgoOrderId"},
                {"level": "ALGO_PARENT", "link": "RaptorOrderId"}]}

``when`` is ``{"field": F}`` (non-empty) or ``{"field": F, "equals": "V"}``; a level without
``when`` is the system's default and comes last. ``link`` is the field holding the parent's
id; root levels have none, every other level needs one.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon.config import Topology

__all__ = [
    "TARGETS",
    "NUMBER_TARGETS",
    "NUMBER_PATTERN",
    "FIX_SIDE_CODES",
    "FIX_STATUS_CODES",
    "DEFAULT_STATE_CONFIG",
    "StateWhen",
    "StateLevelRule",
    "StateSystem",
    "StateConfig",
    "text",
    "number",
    "legal_name",
    "parse_state_config",
    "load_state_config",
    "match_files",
    "read_state_file",
    "read_state_files",
    "fold_system",
    "fold_all",
]

#: Canonical columns a system's ``fields`` may fill (OrderId / AltId / LinkId have their own keys).
TARGETS: Tuple[str, ...] = (
    "Moniker", "Symbol", "Side", "OrderQty", "CumQty", "LeavesQty", "AvgPx", "Status", "Destination",
)
NUMBER_TARGETS: Tuple[str, ...] = ("OrderQty", "CumQty", "LeavesQty", "AvgPx")
#: A plain decimal number. Whatever matches parses to the same double in python and in
#: Java's ``Double.parseDouble``; anything else (``1,000``, ``n/a``, ``inf``) reads as null.
NUMBER_PATTERN = "[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?"
_NUMBER_RE = re.compile(NUMBER_PATTERN)

#: ``"side_codes": "fix"`` -- FIX 4.2 Side (54).
FIX_SIDE_CODES: Dict[str, str] = {
    "1": "BUY", "2": "SELL", "3": "BUY_MINUS", "4": "SELL_PLUS",
    "5": "SELL_SHORT", "6": "SELL_SHORT_EXEMPT", "7": "UNDISCLOSED", "8": "CROSS", "9": "CROSS_SHORT",
}
#: ``"status_codes": "fix"`` -- FIX 4.2 OrdStatus (39).
FIX_STATUS_CODES: Dict[str, str] = {
    "0": "NEW", "1": "PARTIALLY_FILLED", "2": "FILLED", "3": "DONE_FOR_DAY", "4": "CANCELED",
    "5": "REPLACED", "6": "PENDING_CANCEL", "7": "STOPPED", "8": "REJECTED", "9": "SUSPENDED",
    "A": "PENDING_NEW", "B": "CALCULATED", "C": "EXPIRED", "D": "ACCEPTED_FOR_BIDDING",
    "E": "PENDING_REPLACE",
}

#: The mapping the sample files (states_mock.py, state-samples/) follow -- the mock tables'
#: own field names. Your systems' will differ; that is all the config is for.
DEFAULT_STATE_CONFIG: Dict[str, Any] = {
    "systems": [
        {"system": "RAPTOR", "files": ["raptor*.csv"], "order_id": "OrderId", "alt_id": "ClOrdID",
         "side_codes": "fix",
         "fields": {"Moniker": "Moniker", "Symbol": "Symbol", "Side": "Side", "OrderQty": "OrderQty",
                    "CumQty": "CumQty", "LeavesQty": "LeavesQty", "AvgPx": "AvgPx", "Status": "OrdStatus"},
         "levels": [{"level": "CLIENT"}]},
        {"system": "ALGO", "files": ["algo*.jsonl"], "order_id": "AlgoOrderId", "sequence": "Version",
         "fields": {"Symbol": "Symbol", "Side": "Side", "OrderQty": "Qty", "CumQty": "Filled",
                    "LeavesQty": "Open", "AvgPx": "AvgPrice", "Status": "State", "Destination": "Strategy"},
         "levels": [{"level": "ALGO_CHILD", "when": {"field": "ParentAlgoOrderId"}, "link": "ParentAlgoOrderId"},
                    {"level": "ALGO_PARENT", "link": "RaptorOrderId"}]},
        {"system": "OR", "files": ["router*.csv"], "order_id": "RouterOrderId", "sequence": "UpdateTime",
         "fields": {"Symbol": "Symbol", "Side": "Side", "OrderQty": "Qty", "CumQty": "CumQty",
                    "LeavesQty": "LeavesQty", "AvgPx": "AvgPx", "Status": "Status", "Destination": "Venue"},
         "levels": [{"level": "OR_CHILD", "when": {"field": "ParentRouterOrderId"}, "link": "ParentRouterOrderId"},
                    {"level": "OR_PARENT", "link": "UpstreamOrderId"}]},
    ]
}

_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
#: A value written into a Deephaven formula between backticks.
_LITERAL_RE = re.compile(r"^[^`\\\r\n]+$")
_SYSTEM_KEYS = frozenset({
    "system", "files", "order_id", "alt_id", "id_fields", "fields", "side_codes", "status_codes",
    "sequence", "levels",
})
_LEVEL_KEYS = frozenset({"level", "when", "link"})
_JAVA_SPACE = "".join(chr(i) for i in range(33))


# --------------------------------------------------------------------------------------
# Values
# --------------------------------------------------------------------------------------


def text(value: Any) -> str:
    """A field as trimmed text: ``None`` -> ``""``; trims what Java's ``String.trim`` does."""
    return "" if value is None else str(value).strip(_JAVA_SPACE)


def number(value: Any) -> Optional[float]:
    """A field as a number: a plain decimal, else ``None``."""
    raw = text(value)
    return float(raw) if _NUMBER_RE.fullmatch(raw) else None


def legal_name(name: str) -> str:
    """A file's column name as a Deephaven column name: ``Order Id`` -> ``Order_Id``, ``1x`` -> ``_1x``."""
    out = re.sub(r"[^A-Za-z0-9_]", "_", name.strip(_JAVA_SPACE))
    return out if out and not out[0].isdigit() else "_" + out


def _json_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


# --------------------------------------------------------------------------------------
# The mapping config
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class StateWhen:
    field: str
    equals: Optional[str] = None


@dataclass(frozen=True)
class StateLevelRule:
    level: str
    when: Optional[StateWhen] = None
    link: Optional[str] = None


@dataclass(frozen=True)
class StateSystem:
    system: str
    order_id: str
    levels: Tuple[StateLevelRule, ...]
    fields: Tuple[Tuple[str, str], ...] = ()
    alt_id: Optional[str] = None
    id_fields: Tuple[str, ...] = ()
    side_codes: Tuple[Tuple[str, str], ...] = ()
    status_codes: Tuple[Tuple[str, str], ...] = ()
    sequence: Optional[str] = None
    files: Tuple[str, ...] = ()

    def source(self, target: str) -> Optional[str]:
        """The field that fills canonical column ``target`` (``None``: left empty)."""
        return dict(self.fields).get(target)

    @property
    def field_names(self) -> Tuple[str, ...]:
        """Every field the mapping reads, in a fixed order."""
        names: List[str] = [self.order_id]
        names += [self.alt_id] if self.alt_id else []
        names += list(self.id_fields) + [source for _, source in self.fields]
        names += [self.sequence] if self.sequence else []
        for rule in self.levels:
            names += [rule.link] if rule.link else []
            names += [rule.when.field] if rule.when else []
        return _unique(names)


@dataclass(frozen=True)
class StateConfig:
    systems: Tuple[StateSystem, ...] = field(default_factory=tuple)

    def __getitem__(self, name: str) -> StateSystem:
        for system in self.systems:
            if system.system == name:
                return system
        raise KeyError(name)


def _unique(items: Iterable[str]) -> Tuple[str, ...]:
    seen: List[str] = []
    for item in items:
        if item not in seen:
            seen.append(item)
    return tuple(seen)


def _field(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _FIELD_RE.match(value):
        raise ValueError(f"{where}: expected a field name (letters, digits, _), got {value!r}")
    return value


def _literal(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _LITERAL_RE.match(value):
        raise ValueError(f"{where}: expected a non-empty value without ` or \\, got {value!r}")
    return value


def _codes(value: Any, defaults: Dict[str, str], where: str) -> Tuple[Tuple[str, str], ...]:
    if value is None:
        return ()
    if value == "fix":
        return tuple(defaults.items())
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{where}: expected \"fix\" or an object {{\"code\": \"NAME\"}}")
    return tuple((_literal(k, where), _literal(v, where)) for k, v in value.items())


def parse_state_config(obj: Mapping[str, Any], topology: Topology) -> StateConfig:
    """Validate a mapping config against the level topology.

    Every topology level must be mapped exactly once, by the system that owns it; root
    levels take no ``link`` and every other level needs one; a level without ``when`` is
    the system's default and must come last; field names are identifiers (they become
    Deephaven column names).

    Raises:
        ValueError: naming the system / level and the field at fault.
    """
    if not isinstance(obj, Mapping) or not isinstance(obj.get("systems"), list) or not obj["systems"]:
        raise ValueError("state config: expected {\"systems\": [ ... ]} with at least one system")
    systems: List[StateSystem] = []
    mapped: Dict[str, str] = {}
    for i, raw in enumerate(obj["systems"]):
        if not isinstance(raw, Mapping):
            raise ValueError(f"state config systems[{i}]: expected an object")
        unknown = set(raw) - _SYSTEM_KEYS
        if unknown:
            raise ValueError(f"state config systems[{i}]: unknown key(s) {sorted(unknown)}")
        name = raw.get("system")
        if name not in topology.systems:
            raise ValueError(f"state config systems[{i}]: system {name!r} is not in the topology {list(topology.systems)}")
        where = f"state config [{name}]"
        order_id = _field(raw.get("order_id"), f"{where} order_id")
        alt_id = _field(raw["alt_id"], f"{where} alt_id") if raw.get("alt_id") is not None else None
        fields = raw.get("fields", {})
        if not isinstance(fields, Mapping):
            raise ValueError(f"{where}: 'fields' must be an object {{\"CanonicalColumn\": \"YourField\"}}")
        for target in fields:
            if target not in TARGETS:
                raise ValueError(f"{where}: fields.{target} is not a canonical column {list(TARGETS)}")
        mapped_fields = tuple((t, _field(fields[t], f"{where} fields.{t}")) for t in TARGETS if t in fields)
        id_fields = raw.get("id_fields", [order_id] + ([alt_id] if alt_id else []))
        if not isinstance(id_fields, list) or not id_fields:
            raise ValueError(f"{where}: 'id_fields' must be a non-empty list of field names")
        sequence = _field(raw["sequence"], f"{where} sequence") if raw.get("sequence") is not None else None
        files = raw.get("files", [])
        if isinstance(files, str) or not isinstance(files, list) or not all(isinstance(f, str) for f in files):
            raise ValueError(f"{where}: 'files' must be a list of glob patterns")

        levels = raw.get("levels")
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"{where}: 'levels' must be a non-empty list")
        rules: List[StateLevelRule] = []
        for j, lv in enumerate(levels):
            if not isinstance(lv, Mapping):
                raise ValueError(f"{where} levels[{j}]: expected an object")
            unknown = set(lv) - _LEVEL_KEYS
            if unknown:
                raise ValueError(f"{where} levels[{j}]: unknown key(s) {sorted(unknown)}")
            level_name = lv.get("level")
            try:
                spec = topology[level_name]
            except KeyError:
                raise ValueError(f"{where} levels[{j}]: level {level_name!r} is not in the topology") from None
            if spec.system != name:
                raise ValueError(f"{where}: level {level_name} belongs to system {spec.system}, not {name}")
            if level_name in mapped:
                raise ValueError(f"{where}: level {level_name} is already mapped by {mapped[level_name]}")
            mapped[level_name] = name
            link = lv.get("link")
            if spec.is_root and link is not None:
                raise ValueError(f"{where}: {level_name} is a root level and takes no link")
            if not spec.is_root and link is None:
                raise ValueError(f"{where}: {level_name} needs a link (the field holding its parent's id)")
            when = None
            if lv.get("when") is not None:
                w = lv["when"]
                if not isinstance(w, Mapping) or "field" not in w or set(w) - {"field", "equals"}:
                    raise ValueError(f"{where}: {level_name}.when must be {{\"field\": F}} or {{\"field\": F, \"equals\": V}}")
                equals = _literal(w["equals"], f"{where} {level_name}.when.equals") if w.get("equals") is not None else None
                when = StateWhen(_field(w["field"], f"{where} {level_name}.when.field"), equals)
            elif j != len(levels) - 1:
                raise ValueError(f"{where}: {level_name} has no 'when', so it must be the last level of {name}")
            rules.append(StateLevelRule(level_name, when, _field(link, f"{where} {level_name}.link") if link is not None else None))

        systems.append(StateSystem(
            system=name,
            order_id=order_id,
            levels=tuple(rules),
            fields=mapped_fields,
            alt_id=alt_id,
            id_fields=tuple(_field(f, f"{where} id_fields") for f in id_fields),
            side_codes=_codes(raw.get("side_codes"), FIX_SIDE_CODES, f"{where} side_codes"),
            status_codes=_codes(raw.get("status_codes"), FIX_STATUS_CODES, f"{where} status_codes"),
            sequence=sequence,
            files=tuple(files),
        ))
    missing = [lvl for lvl in topology.names if lvl not in mapped]
    if missing:
        raise ValueError(f"state config: topology level(s) {missing} are not mapped by any system")
    names = [s.system for s in systems]
    if len(set(names)) != len(names):
        raise ValueError(f"state config: a system is listed twice ({names})")
    return StateConfig(tuple(systems))


def load_state_config(path: Optional[str], topology: Topology) -> StateConfig:
    """Load and validate a JSON mapping config; ``None`` / ``""`` -> :data:`DEFAULT_STATE_CONFIG`."""
    if not path:
        return parse_state_config(DEFAULT_STATE_CONFIG, topology)
    with open(path, "r", encoding="utf-8-sig") as handle:
        try:
            obj = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(f"state config {path}: not valid JSON ({exc})") from None
    return parse_state_config(obj, topology)


# --------------------------------------------------------------------------------------
# Files: CSV (header row), JSON Lines (one object per line), JSON (an array of objects)
# --------------------------------------------------------------------------------------

Row = Dict[str, str]


def read_state_file(path: str) -> Tuple[List[Row], int]:
    """One file's messages as text rows (column names made legal), and the bad-line count.

    ``.csv``: a header row, then one message per row. ``.jsonl`` / ``.ndjson``: one JSON
    object per line. ``.json``: an array of objects. A line that is not an object is
    counted, not fatal. Nested values are kept as compact JSON text.
    """
    extension = os.path.splitext(path)[1].lower()
    rows: List[Row] = []
    bad = 0
    if extension == ".csv":
        with open(path, "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if header is None:
                return rows, 0
            names = [legal_name(name) for name in header]
            for record in reader:
                if not any(cell.strip() for cell in record):
                    continue
                if len(record) > len(names):
                    bad += 1
                    continue
                rows.append({name: (record[i] if i < len(record) else "") for i, name in enumerate(names)})
        return rows, bad
    if extension in (".jsonl", ".ndjson", ".json"):
        with open(path, "r", encoding="utf-8-sig") as handle:
            if extension == ".json":
                try:
                    objects: List[Any] = json.load(handle)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}: not valid JSON ({exc})") from None
                if not isinstance(objects, list):
                    raise ValueError(f"{path}: expected a JSON array of objects")
            else:
                objects = []
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        objects.append(json.loads(line))
                    except json.JSONDecodeError:
                        objects.append(None)
        for obj in objects:
            if isinstance(obj, dict):
                rows.append({legal_name(str(k)): _json_text(v) for k, v in obj.items()})
            else:
                bad += 1
        return rows, bad
    raise ValueError(f"{path}: unsupported file type {extension!r} (use .csv, .jsonl, .ndjson or .json)")


def match_files(patterns: Sequence[str], base_dir: str) -> List[str]:
    """The files matching ``patterns`` under ``base_dir``: sorted per pattern, de-duplicated."""
    files: List[str] = []
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(base_dir, pattern))):
            if os.path.isfile(path) and path not in files:
                files.append(path)
    return files


def read_state_files(patterns: Sequence[str], base_dir: str) -> Tuple[List[Row], List[str], List[str], int]:
    """Read every file matching ``patterns`` under ``base_dir`` (:func:`match_files`).

    Returns:
        ``(rows, columns, files, bad)``: rows in file order (each with every column, ``""``
        where a file or message lacks it), columns in first-seen order, the files read,
        and the number of unusable lines.
    """
    files = match_files(patterns, base_dir)
    rows: List[Row] = []
    columns: List[str] = []
    bad = 0
    for path in files:
        file_rows, file_bad = read_state_file(path)
        bad += file_bad
        for row in file_rows:
            for name in row:
                if name not in columns:
                    columns.append(name)
        rows.extend(file_rows)
    return [{name: row.get(name, "") for name in columns} for row in rows], columns, files, bad


# --------------------------------------------------------------------------------------
# The fold (python twin of states_dag.state_level_tables)
# --------------------------------------------------------------------------------------


def _sequence_key(value: Any) -> Tuple[float, str]:
    """Numbers order numerically, anything else (ISO timestamps) as text; empty first.

    The engine sorts on the same two keys (a null number sorts first).
    """
    num = number(value)
    return (float("-inf") if num is None else num, text(value))


def _coded(value: str, codes: Tuple[Tuple[str, str], ...]) -> str:
    return dict(codes).get(value, value) if codes else value


def _level_of(system: StateSystem, row: Mapping[str, Any]) -> Optional[StateLevelRule]:
    for rule in system.levels:
        if rule.when is None:
            return rule
        value = text(row.get(rule.when.field))
        if (rule.when.equals is None and value) or (rule.when.equals is not None and value == rule.when.equals):
            return rule
    return None


def fold_system(system: StateSystem, rows: Iterable[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """Fold one system's messages into canonical rows (with ``Level``) and its id history.

    Returns:
        ``(rows, ids)``: one canonical row per order that maps to a level (others are
        dropped), and ``(System, Id, OrderId)`` for every id each order was sent with.
    """
    latest: Dict[str, Mapping[str, Any]] = {}
    latest_key: Dict[str, Tuple[float, str]] = {}
    ids: List[Tuple[str, str, str]] = []
    seen = set()
    for row in rows:
        key = text(row.get(system.order_id))
        if not key:
            continue
        if system.sequence is None:
            latest[key] = row
        else:
            seq = _sequence_key(row.get(system.sequence))
            if key not in latest or seq >= latest_key[key]:  # a tie goes to the later message
                latest[key] = row
                latest_key[key] = seq
        for name in system.id_fields:
            value = text(row.get(name))
            if value and (key, value) not in seen:
                seen.add((key, value))
                ids.append((system.system, value, key))

    out: List[Dict[str, Any]] = []
    for key, row in latest.items():
        rule = _level_of(system, row)
        if rule is None:
            continue
        canonical: Dict[str, Any] = {
            "Level": rule.level,
            "OrderId": key,
            "AltId": text(row.get(system.alt_id)) if system.alt_id else "",
            "LinkId": text(row.get(rule.link)) if rule.link else "",
        }
        for target in TARGETS:
            source = system.source(target)
            if target in NUMBER_TARGETS:
                canonical[target] = number(row.get(source)) if source else None
            else:
                canonical[target] = text(row.get(source)) if source else ""
        canonical["Side"] = _coded(canonical["Side"], system.side_codes)
        canonical["Status"] = _coded(canonical["Status"], system.status_codes)
        out.append(canonical)
    return out, ids


def fold_all(config: StateConfig, messages: Mapping[str, Iterable[Mapping[str, Any]]]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """:func:`fold_system` over every configured system (a missing system folds to nothing)."""
    rows: List[Dict[str, Any]] = []
    ids: List[Tuple[str, str, str]] = []
    for system in config.systems:
        r, i = fold_system(system, messages.get(system.system, ()))
        rows.extend(r)
        ids.extend(i)
    return rows, ids
