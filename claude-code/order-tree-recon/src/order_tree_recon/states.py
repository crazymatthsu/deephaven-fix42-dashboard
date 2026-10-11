"""Latest order state -> the canonical order-tree levels: the generic fold (doc 14 section 12).

Each system publishes **order states**: every message is one order's complete current state.
This module folds such messages, given as rows of named fields, into one canonical row per
order and level. :mod:`order_tree_recon.fixstate` compiles a FIX tag mapping into a
:class:`StateConfig` whose fields are the tag columns ``T<tag>``; this module knows nothing
about FIX.

* the order's state is its **latest** message: the highest ``sequence`` value, then the
  highest ``arrival`` value, then the later row;
* text is trimmed (Java ``String.trim`` rules), a missing field is ``""``;
* numbers are plain decimals (:data:`NUMBER_PATTERN`) or null -- never an error; with
  ``avg_px_needs_fill`` (FIX sends ``6=0`` on an unfilled order) ``AvgPx`` is null until
  ``CumQty > 0``;
* ``side_codes`` / ``status_codes`` translate a system's codes (FIX 4.2 tables below);
* the level is the first ``levels`` rule whose ``when`` matches the latest state.

Pure python: :mod:`order_tree_recon.states_dag` does the same with Deephaven operations and
the embedded-engine test asserts both produce identical rows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from order_tree_recon.config import Topology

__all__ = [
    "TARGETS",
    "NUMBER_TARGETS",
    "NUMBER_PATTERN",
    "FIX_SIDE_CODES",
    "FIX_STATUS_CODES",
    "StateWhen",
    "StateLevelRule",
    "StateSystem",
    "StateConfig",
    "text",
    "number",
    "parse_state_config",
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

_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
#: A value written into a Deephaven formula between backticks.
_LITERAL_RE = re.compile(r"^[^`\\\r\n]+$")
_SYSTEM_KEYS = frozenset({
    "system", "order_id", "alt_id", "id_fields", "fields", "side_codes", "status_codes", "sequence",
    "arrival", "avg_px_needs_fill", "levels",
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


# --------------------------------------------------------------------------------------
# The mapping
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
    #: Orders one order's messages; numbers numerically, anything else as text.
    sequence: Optional[str] = None
    #: A numeric arrival counter, the tie-break after ``sequence`` (the SOW mirror's
    #: output is not in arrival order, so the FIX path always sets one).
    arrival: Optional[str] = None
    #: ``AvgPx`` only once something filled (FIX reports ``6=0`` on an unfilled order).
    avg_px_needs_fill: bool = False

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
        names += [self.arrival] if self.arrival else []
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


def parse_state_config(obj: Mapping[str, Any], topology: Topology, label: str = "state config") -> StateConfig:
    """Validate a field mapping against the level topology.

    Every topology level must be mapped exactly once, by the system that owns it; root
    levels take no ``link`` and every other level needs one; a level without ``when`` is
    the system's default and must come last; field names are identifiers (they become
    Deephaven column names).

    Raises:
        ValueError: naming the system / level and the field at fault (prefixed ``label``).
    """
    if not isinstance(obj, Mapping) or not isinstance(obj.get("systems"), list) or not obj["systems"]:
        raise ValueError(f"{label}: expected {{\"systems\": [ ... ]}} with at least one system")
    systems: List[StateSystem] = []
    mapped: Dict[str, str] = {}
    for i, raw in enumerate(obj["systems"]):
        if not isinstance(raw, Mapping):
            raise ValueError(f"{label} systems[{i}]: expected an object")
        unknown = set(raw) - _SYSTEM_KEYS
        if unknown:
            raise ValueError(f"{label} systems[{i}]: unknown key(s) {sorted(unknown)}")
        name = raw.get("system")
        if name not in topology.systems:
            raise ValueError(f"{label} systems[{i}]: system {name!r} is not in the topology {list(topology.systems)}")
        where = f"{label} [{name}]"
        order_id = _field(raw.get("order_id"), f"{where} order_id")
        alt_id = _field(raw["alt_id"], f"{where} alt_id") if raw.get("alt_id") is not None else None
        fields = raw.get("fields", {})
        if not isinstance(fields, Mapping):
            raise ValueError(f"{where}: 'fields' must be an object {{\"CanonicalColumn\": \"field\"}}")
        for target in fields:
            if target not in TARGETS:
                raise ValueError(f"{where}: {target} is not a canonical column {list(TARGETS)}")
        mapped_fields = tuple(
            (t, _field(fields[t], f"{where} {t}")) for t in TARGETS if fields.get(t) is not None
        )
        id_fields = raw.get("id_fields", [order_id] + ([alt_id] if alt_id else []))
        if not isinstance(id_fields, list) or not id_fields:
            raise ValueError(f"{where}: the id list must be non-empty")
        sequence = _field(raw["sequence"], f"{where} sequence") if raw.get("sequence") is not None else None
        arrival = _field(raw["arrival"], f"{where} arrival") if raw.get("arrival") is not None else None
        needs_fill = raw.get("avg_px_needs_fill", False)
        if not isinstance(needs_fill, bool):
            raise ValueError(f"{where}: avg_px_needs_fill must be true or false")

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
                raise ValueError(f"{where}: {level_name} is a root level and takes no parent link")
            if not spec.is_root and link is None:
                raise ValueError(f"{where}: {level_name} needs a parent link (where its parent's id is)")
            when = None
            if lv.get("when") is not None:
                w = lv["when"]
                if not isinstance(w, Mapping) or "field" not in w or set(w) - {"field", "equals"}:
                    raise ValueError(f"{where}: {level_name}.when is malformed")
                equals = _literal(w["equals"], f"{where} {level_name}.when.equals") if w.get("equals") is not None else None
                when = StateWhen(_field(w["field"], f"{where} {level_name}.when"), equals)
            elif j != len(levels) - 1:
                raise ValueError(f"{where}: {level_name} has no 'when', so it must be the last level of {name}")
            rules.append(StateLevelRule(level_name, when, _field(link, f"{where} {level_name} link") if link is not None else None))

        systems.append(StateSystem(
            system=name,
            order_id=order_id,
            levels=tuple(rules),
            fields=mapped_fields,
            alt_id=alt_id,
            id_fields=tuple(_field(f, f"{where} ids") for f in id_fields),
            side_codes=_codes(raw.get("side_codes"), FIX_SIDE_CODES, f"{where} side_codes"),
            status_codes=_codes(raw.get("status_codes"), FIX_STATUS_CODES, f"{where} status_codes"),
            sequence=sequence,
            arrival=arrival,
            avg_px_needs_fill=needs_fill,
        ))
    missing = [lvl for lvl in topology.names if lvl not in mapped]
    if missing:
        raise ValueError(f"{label}: topology level(s) {missing} are not mapped by any system")
    names = [s.system for s in systems]
    if len(set(names)) != len(names):
        raise ValueError(f"{label}: a system is listed twice ({names})")
    return StateConfig(tuple(systems))


# --------------------------------------------------------------------------------------
# The fold (python twin of states_dag.state_level_tables)
# --------------------------------------------------------------------------------------


def _sequence_key(value: Any) -> Tuple[float, str]:
    """Numbers order numerically, anything else (ISO timestamps) as text; empty first.

    The engine sorts on the same two keys (a null number sorts first).
    """
    num = number(value)
    return (float("-inf") if num is None else num, text(value))


def _order_key(system: StateSystem, row: Mapping[str, Any]) -> Tuple[Any, ...]:
    key: Tuple[Any, ...] = ()
    if system.sequence is not None:
        key += _sequence_key(row.get(system.sequence))
    if system.arrival is not None:
        arrival = number(row.get(system.arrival))
        key += (float("-inf") if arrival is None else arrival,)
    return key


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
    """Fold one system's messages into canonical rows (with ``Level``) and their ids.

    Returns:
        ``(rows, ids)``: one canonical row per order that maps to a level (others are
        dropped), and ``(System, Id, OrderId)`` for every id field of every message.
    """
    latest: Dict[str, Mapping[str, Any]] = {}
    latest_key: Dict[str, Tuple[Any, ...]] = {}
    ids: List[Tuple[str, str, str]] = []
    seen = set()
    for row in rows:
        key = text(row.get(system.order_id))
        if not key:
            continue
        order = _order_key(system, row)
        if key not in latest or order >= latest_key[key]:  # a tie goes to the later row
            latest[key] = row
            latest_key[key] = order
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
        if system.avg_px_needs_fill and not (canonical["CumQty"] or 0.0) > 0:
            canonical["AvgPx"] = None
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
