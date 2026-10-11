"""Real FIX messages -> the canonical order-tree levels: parser, mapping config, fold (doc 14 §12).

Pure python: the Deephaven side (:mod:`order_tree_recon.fix_dag`) calls :func:`pick_values`
inside a formula and implements :func:`fold_system` with joins, so the two cannot drift --
the embedded-engine test asserts they produce identical rows.

How messages become orders
--------------------------
Drop-copy tapes are mostly ExecutionReports (35=8), and an ExecutionReport carries the
order's *cumulative* state: OrdStatus (39), OrderQty (38), CumQty (14), LeavesQty (151),
AvgPx (6). So an order's current state is simply its **latest** report, keyed by a stable
order id -- OrderID (37) by default, which survives cancel/replace in FIX 4.2. No stateful
fold is needed; a bust or a correction is followed by a report that restates the truth.

* state fields -- the latest *non-empty* value per order (a fill that omits Symbol keeps it);
* link / moniker / role tags -- the *first* non-empty value per order: firms often stamp
  custom tags on the first report only, and a parent link does not legitimately change;
* every id the order was ever known by (OrderID, each ClOrdID, each OrigClOrdID) is indexed,
  so a downstream link that names an *amended-away* ClOrdID still resolves.

Only messages with MsgType 8 are used. 35=D/G/F requests carry no fills and are ignored.

The mapping config (JSON)
-------------------------
One entry per system, each listing its levels. A level is chosen by an optional ``when``
condition (first match wins; a level without ``when`` is the default and comes last). This
is :data:`DEFAULT_FIX_CONFIG`, the convention the sample tapes follow::

    {"systems": [
      {"system": "RAPTOR", "files": ["raptor*.log"], "moniker_tag": "1",
       "levels": [{"level": "CLIENT"}]},
      {"system": "ALGO", "files": ["algo*.log"], "destination_tag": "847",
       "levels": [{"level": "ALGO_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                  {"level": "ALGO_PARENT", "link_tag": "11"}]},
      {"system": "OR", "files": ["router*.log"],
       "levels": [{"level": "OR_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                  {"level": "OR_PARENT", "link_tag": "11"}]}]}

Read it as: a cross-system parent link is the downstream order's own ClOrdID (11) -- the id
the upstream system sent it, which the upstream's drop copy reports as its OrderID or one
of its ClOrdIDs -- and an in-system child names its parent in custom tag 20001. Your firm's
tags will differ; that is all the config is for.

Per-system keys and defaults: ``order_id_tag`` "37"; ``id_tags`` ["37", "11", "41"];
``moniker_tag`` "1" (Account); ``destination_tag`` "100" (ExDestination); ``files`` [] (globs,
relative to the FIX directory). ``when`` is ``{"tag": T}`` (the tag is present) or
``{"tag": T, "equals": "V"}``. Root levels take no ``link_tag``; every other level must.
``moniker_tag`` / ``destination_tag`` may be ``null`` (the column stays empty).
"""

from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon.config import Topology

__all__ = [
    "SIDE_NAMES",
    "ORD_STATUS_NAMES",
    "NUMERIC_TAGS",
    "DEFAULT_FIX_CONFIG",
    "FixWhen",
    "FixLevelRule",
    "FixSystem",
    "FixConfig",
    "parse_fix",
    "pick_values",
    "parse_fix_config",
    "load_fix_config",
    "read_fix_lines",
    "read_fix_files",
    "fold_system",
    "fold_all",
]

#: FIX 4.2 Side (54) -> the names the canonical tables use.
SIDE_NAMES: Dict[str, str] = {
    "1": "BUY", "2": "SELL", "3": "BUY_MINUS", "4": "SELL_PLUS",
    "5": "SELL_SHORT", "6": "SELL_SHORT_EXEMPT", "7": "UNDISCLOSED", "8": "CROSS", "9": "CROSS_SHORT",
}
#: FIX 4.2 OrdStatus (39) -> names.
ORD_STATUS_NAMES: Dict[str, str] = {
    "0": "NEW", "1": "PARTIALLY_FILLED", "2": "FILLED", "3": "DONE_FOR_DAY", "4": "CANCELED",
    "5": "REPLACED", "6": "PENDING_CANCEL", "7": "STOPPED", "8": "REJECTED", "9": "SUSPENDED",
    "A": "PENDING_NEW", "B": "CALCULATED", "C": "EXPIRED", "D": "ACCEPTED_FOR_BIDDING",
    "E": "PENDING_REPLACE",
}
#: Tags whose values must be numbers; anything unparsable reads as empty, never as an error.
NUMERIC_TAGS = frozenset({"14", "151", "6", "38"})
#: A plain decimal number. Stricter than python's ``float`` (no ``1_000``, ``inf``, ``nan``),
#: so whatever passes parses to the same double in Java's ``Double.parseDouble``.
_NUMBER_RE = re.compile(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")

#: The mapping the mock tapes (fix_mock.py) and fix-samples/ use. Your company's will differ.
DEFAULT_FIX_CONFIG: Dict[str, Any] = {
    "systems": [
        {"system": "RAPTOR", "files": ["raptor*.log"], "moniker_tag": "1",
         "levels": [{"level": "CLIENT"}]},
        {"system": "ALGO", "files": ["algo*.log"], "destination_tag": "847",
         "levels": [{"level": "ALGO_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                    {"level": "ALGO_PARENT", "link_tag": "11"}]},
        {"system": "OR", "files": ["router*.log"],
         "levels": [{"level": "OR_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                    {"level": "OR_PARENT", "link_tag": "11"}]},
    ]
}

_TAG_RE = re.compile(r"^[0-9]+$")
_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_SYSTEM_KEYS = frozenset({"system", "files", "order_id_tag", "id_tags", "moniker_tag", "destination_tag", "levels"})
_LEVEL_KEYS = frozenset({"level", "when", "link_tag"})


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


def parse_fix(raw: str) -> Dict[str, str]:
    """Parse one FIX message into ``{tag: value}`` (first occurrence of a tag wins).

    Tolerates the forms FIX logs come in: SOH (``\\x01``), ``|`` or ``^A`` delimiters, a log
    prefix before ``8=FIX`` (timestamps, session names), CRLF and a trailing delimiter.
    """
    text = (raw or "").strip()
    start = text.find("8=FIX")
    if start > 0:
        text = text[start:]
    if "\x01" in text:
        parts = text.split("\x01")
    elif "^A" in text:
        parts = text.split("^A")
    else:
        parts = text.split("|")
    out: Dict[str, str] = {}
    for part in parts:
        tag, sep, value = part.partition("=")
        tag = tag.strip()
        if sep and tag.isdigit() and tag not in out:
            out[tag] = value
    return out


def _normalize(tag: str, value: str) -> str:
    if not value:
        return ""
    if tag == "54":
        return SIDE_NAMES.get(value, value)
    if tag == "39":
        return ORD_STATUS_NAMES.get(value, value)
    if tag in NUMERIC_TAGS:
        return value if _NUMBER_RE.match(value) else ""
    return value


def pick_values(raw: str, tags: Sequence[str]) -> List[str]:
    """The normalized values of ``tags`` in one message (``""`` where absent).

    Side and OrdStatus codes become names; a malformed number becomes ``""`` (a bad message
    must not stop the table). This one function is what the Deephaven formula calls.
    """
    fields = parse_fix(raw)
    return [_normalize(tag, fields.get(tag, "")) for tag in tags]


# --------------------------------------------------------------------------------------
# The mapping config
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FixWhen:
    tag: str
    equals: Optional[str] = None


@dataclass(frozen=True)
class FixLevelRule:
    level: str
    when: Optional[FixWhen] = None
    link_tag: Optional[str] = None


@dataclass(frozen=True)
class FixSystem:
    system: str
    levels: Tuple[FixLevelRule, ...]
    files: Tuple[str, ...] = ()
    order_id_tag: str = "37"
    id_tags: Tuple[str, ...] = ("37", "11", "41")
    moniker_tag: Optional[str] = "1"
    destination_tag: Optional[str] = "100"

    @property
    def state_tags(self) -> Tuple[str, ...]:
        """Latest-non-empty fields: ClOrdID, OrdStatus, quantities, price, instrument."""
        tags = ["11", "39", "38", "14", "151", "6", "55", "54"]
        if self.destination_tag:
            tags.append(self.destination_tag)
        return _unique(tags)

    @property
    def sticky_tags(self) -> Tuple[str, ...]:
        """First-non-empty fields: the moniker, the link tags and the ``when`` tags."""
        tags: List[str] = [self.moniker_tag] if self.moniker_tag else []
        for rule in self.levels:
            if rule.link_tag:
                tags.append(rule.link_tag)
            if rule.when:
                tags.append(rule.when.tag)
        return _unique(tags)

    @property
    def tags(self) -> Tuple[str, ...]:
        """Every tag picked from a message, in a fixed order (MsgType and the key first)."""
        return _unique(["35", self.order_id_tag] + list(self.state_tags) + list(self.sticky_tags) + list(self.id_tags))


@dataclass(frozen=True)
class FixConfig:
    systems: Tuple[FixSystem, ...] = field(default_factory=tuple)

    def __getitem__(self, name: str) -> FixSystem:
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


def _tag(value: Any, where: str) -> str:
    text = str(value) if isinstance(value, int) and not isinstance(value, bool) else value
    if not isinstance(text, str) or not _TAG_RE.match(text):
        raise ValueError(f"{where}: expected a FIX tag number, got {value!r}")
    return text


def parse_fix_config(obj: Mapping[str, Any], topology: Topology) -> FixConfig:
    """Validate a mapping config against the level topology.

    Every topology level must be mapped exactly once, by a system with the same name as the
    level's system; root levels take no ``link_tag`` and every other level needs one; a level
    without ``when`` is the system's default and must come last.

    Raises:
        ValueError: naming the system / level and the field at fault.
    """
    if not isinstance(obj, Mapping) or not isinstance(obj.get("systems"), list) or not obj["systems"]:
        raise ValueError("fix config: expected {\"systems\": [ ... ]} with at least one system")
    systems: List[FixSystem] = []
    mapped: Dict[str, str] = {}
    for i, raw in enumerate(obj["systems"]):
        if not isinstance(raw, Mapping):
            raise ValueError(f"fix config systems[{i}]: expected an object")
        unknown = set(raw) - _SYSTEM_KEYS
        if unknown:
            raise ValueError(f"fix config systems[{i}]: unknown key(s) {sorted(unknown)}")
        name = raw.get("system")
        if name not in topology.systems:
            raise ValueError(f"fix config systems[{i}]: system {name!r} is not in the topology {list(topology.systems)}")
        where = f"fix config [{name}]"
        rules: List[FixLevelRule] = []
        levels = raw.get("levels")
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"{where}: 'levels' must be a non-empty list")
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
            link = lv.get("link_tag")
            if spec.is_root and link is not None:
                raise ValueError(f"{where}: {level_name} is a root level and takes no link_tag")
            if not spec.is_root and link is None:
                raise ValueError(f"{where}: {level_name} needs a link_tag (the tag naming its parent's id)")
            when = None
            if lv.get("when") is not None:
                w = lv["when"]
                if not isinstance(w, Mapping) or "tag" not in w or set(w) - {"tag", "equals"}:
                    raise ValueError(f"{where}: {level_name}.when must be {{\"tag\": T}} or {{\"tag\": T, \"equals\": V}}")
                equals = w.get("equals")
                if equals is not None and (not isinstance(equals, str) or not _VALUE_RE.match(equals)):
                    raise ValueError(f"{where}: {level_name}.when.equals must be a plain value, got {equals!r}")
                when = FixWhen(_tag(w["tag"], f"{where} {level_name}.when.tag"), equals)
            elif j != len(levels) - 1:
                raise ValueError(f"{where}: {level_name} has no 'when', so it must be the last level of {name}")
            rules.append(FixLevelRule(level_name, when, _tag(link, f"{where} {level_name}.link_tag") if link is not None else None))
        files = raw.get("files", [])
        if isinstance(files, str) or not isinstance(files, list) or not all(isinstance(f, str) for f in files):
            raise ValueError(f"{where}: 'files' must be a list of glob patterns")
        id_tags = raw.get("id_tags", ["37", "11", "41"])
        if not isinstance(id_tags, list) or not id_tags:
            raise ValueError(f"{where}: 'id_tags' must be a non-empty list of tags")
        moniker = raw.get("moniker_tag", "1")
        dest = raw.get("destination_tag", "100")
        systems.append(FixSystem(
            system=name,
            levels=tuple(rules),
            files=tuple(files),
            order_id_tag=_tag(raw.get("order_id_tag", "37"), f"{where} order_id_tag"),
            id_tags=tuple(_tag(t, f"{where} id_tags") for t in id_tags),
            moniker_tag=_tag(moniker, f"{where} moniker_tag") if moniker is not None else None,
            destination_tag=_tag(dest, f"{where} destination_tag") if dest is not None else None,
        ))
    missing = [lvl for lvl in topology.names if lvl not in mapped]
    if missing:
        raise ValueError(f"fix config: topology level(s) {missing} are not mapped by any system")
    names = [s.system for s in systems]
    if len(set(names)) != len(names):
        raise ValueError(f"fix config: a system is listed twice ({names})")
    return FixConfig(tuple(systems))


def load_fix_config(path: Optional[str], topology: Topology) -> FixConfig:
    """Load and validate a JSON mapping config; ``None`` / ``""`` -> :data:`DEFAULT_FIX_CONFIG`."""
    if not path:
        return parse_fix_config(DEFAULT_FIX_CONFIG, topology)
    with open(path, "r", encoding="utf-8-sig") as handle:
        try:
            obj = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(f"fix config {path}: not valid JSON ({exc})") from None
    return parse_fix_config(obj, topology)


# --------------------------------------------------------------------------------------
# Files
# --------------------------------------------------------------------------------------


def read_fix_lines(path: str) -> List[str]:
    """The FIX messages in one log file: every line containing ``8=FIX``, in file order."""
    with open(path, "r", encoding="utf-8-sig", errors="replace") as handle:
        return [line.rstrip("\r\n") for line in handle if "8=FIX" in line]


def read_fix_files(patterns: Sequence[str], base_dir: str) -> Tuple[List[str], List[str]]:
    """Read every file matching ``patterns`` under ``base_dir`` (sorted, de-duplicated).

    Returns:
        ``(messages, files)`` -- messages in file order, files in the order they were read.
    """
    files: List[str] = []
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(base_dir, pattern))):
            if os.path.isfile(path) and path not in files:
                files.append(path)
    messages: List[str] = []
    for path in files:
        messages.extend(read_fix_lines(path))
    return messages, files


# --------------------------------------------------------------------------------------
# The fold (python twin of fix_dag.fix_level_tables)
# --------------------------------------------------------------------------------------


def _level_of(system: FixSystem, sticky: Mapping[str, str]) -> Optional[FixLevelRule]:
    for rule in system.levels:
        if rule.when is None:
            return rule
        value = sticky.get(rule.when.tag, "")
        if (rule.when.equals is None and value) or (rule.when.equals is not None and value == rule.when.equals):
            return rule
    return None


def _num(value: str) -> Optional[float]:
    return float(value) if value else None


def fold_system(system: FixSystem, messages: Iterable[str]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """Fold one system's tape into canonical rows (with ``Level``) and its id history.

    Returns:
        ``(rows, ids)``: one canonical row per order that maps to a level (orders matching no
        level are dropped), and ``(System, Id, OrderId)`` for every id each order carried.
    """
    tags = system.tags
    latest: Dict[str, Dict[str, str]] = {}
    first: Dict[str, Dict[str, str]] = {}
    ids: List[Tuple[str, str, str]] = []
    seen_ids = set()
    for raw in messages:
        values = dict(zip(tags, pick_values(raw, tags)))
        if values["35"] != "8":
            continue
        key = values[system.order_id_tag]
        if not key:
            continue
        state = latest.setdefault(key, {})
        for tag in system.state_tags:
            if values[tag]:
                state[tag] = values[tag]
        sticky = first.setdefault(key, {})
        for tag in system.sticky_tags:
            if values[tag] and tag not in sticky:
                sticky[tag] = values[tag]
        for tag in system.id_tags:
            if values[tag] and (key, values[tag]) not in seen_ids:
                seen_ids.add((key, values[tag]))
                ids.append((system.system, values[tag], key))
    rows: List[Dict[str, Any]] = []
    for key, state in latest.items():
        sticky = first[key]
        rule = _level_of(system, sticky)
        if rule is None:
            continue
        cum = _num(state.get("14", ""))
        avg = _num(state.get("6", ""))
        rows.append({
            "Level": rule.level,
            "OrderId": key,
            "AltId": state.get("11", ""),
            "LinkId": sticky.get(rule.link_tag, "") if rule.link_tag else "",
            "Moniker": sticky.get(system.moniker_tag, "") if system.moniker_tag else "",
            "Symbol": state.get("55", ""),
            "Side": state.get("54", ""),
            "OrderQty": _num(state.get("38", "")),
            "CumQty": cum,
            "LeavesQty": _num(state.get("151", "")),
            "AvgPx": avg if (cum or 0.0) > 0 else None,
            "Status": state.get("39", ""),
            "Destination": state.get(system.destination_tag, "") if system.destination_tag else "",
        })
    return rows, ids


def fold_all(config: FixConfig, tapes: Mapping[str, Iterable[str]]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """:func:`fold_system` over every configured system (a missing tape folds to nothing)."""
    rows: List[Dict[str, Any]] = []
    ids: List[Tuple[str, str, str]] = []
    for system in config.systems:
        r, i = fold_system(system, tapes.get(system.system, ()))
        rows.extend(r)
        ids.extend(i)
    return rows, ids
