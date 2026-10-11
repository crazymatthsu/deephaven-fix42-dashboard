"""FIX order-state messages from AMPS SOW topics -> the canonical order-tree levels (doc 14 §12).

Each system (Raptor, the algo server, the order router) publishes its orders' **latest state
as FIX messages** into one or more AMPS SOW topics. Deephaven subscribes to every topic with
``sow_and_subscribe`` (:mod:`order_tree_recon.amps_sow`) and receives a stream of **events**:

=================  ======================================================================
``Action``         meaning
=================  ======================================================================
``UPSERT``         a SOW record (initial snapshot) or a publish: the order's new state
``DELETE``         ``oof`` / ``sow_delete``: the record left the SOW (deleted, expired, ...)
``SNAPSHOT_END``   ``group_end``: the snapshot of subscription number ``Epoch`` is complete
=================  ======================================================================

Two steps turn events into orders, here in pure python and in
:mod:`order_tree_recon.fixstate_dag` with Deephaven operations (the embedded-engine test
asserts they agree):

1. **The SOW mirror** -- the latest event per ``(Topic, SowKey)`` (the order id tag when an
   event has no SowKey); a ``DELETE`` removes the record; and a record whose ``Epoch`` is
   older than its topic's latest ``SNAPSHOT_END`` is dropped. That last rule is what makes a
   reconnect safe: the HA client re-runs ``sow_and_subscribe``, the new snapshot re-sends
   every live record with the new epoch, and anything deleted while disconnected -- for which
   no ``oof`` ever arrived -- disappears instead of lingering as a phantom open order.
2. **The fold** -- each current record's FIX tags become columns ``T<tag>``, and the generic
   latest-state fold (:mod:`order_tree_recon.states`) maps them onto the canonical columns.
   Every id an order carried in **any** event (OrderID, ClOrdID, OrigClOrdID by default) is
   kept as its id history, so a child that still names an amended-away ClOrdID links.

The mapping config (JSON)
-------------------------
Standard FIX tags are the defaults, so a system usually only lists its topics and levels::

    {"systems": [
      {"system": "RAPTOR", "topics": ["SOW/RAPTOR/ORDERS"], "levels": [{"level": "CLIENT"}]},
      {"system": "ALGO", "topics": ["SOW/ALGO/ORDERS"], "tags": {"Destination": "847"},
       "levels": [{"level": "ALGO_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"},
                  {"level": "ALGO_PARENT", "link_tag": "11"}]},
      ...]}

A system's parents and children can share one topic: the ``when`` rule tells them apart.
Here a child carries its parent's OrderID in tag 26009 and a parent leaves 26009 null, so
the first rule (``when`` a non-empty 26009) picks the children and the default rule the
parents -- an absent tag and an empty one (``26009=``) both read as null.

Per system: ``topics`` (AMPS SOW topics), ``files`` (globs for the file source), ``filter``
(an AMPS content filter), ``order_id_tag`` "37", ``alt_id_tag`` "11", ``id_tags``
["37", "11", "41"], ``tags`` (canonical column -> tag, over :data:`DEFAULT_TAGS`; ``null``
unmaps), ``side_codes`` / ``status_codes`` "fix" (or a ``{code: NAME}`` map, or ``null``),
``sequence_tag`` (orders one order's messages when it can arrive from several topics or out
of order), and ``levels``: ``{"level", "when": {"tag", "equals"?}, "link_tag"}`` -- first
match wins, a level without ``when`` is the default and comes last, root levels have no
``link_tag``.
"""

from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon import states
from order_tree_recon.config import Topology
from order_tree_recon.states import StateConfig, StateSystem

__all__ = [
    "SOH",
    "UPSERT",
    "DELETE",
    "SNAPSHOT_END",
    "ACTIONS",
    "EVENT_COLUMNS",
    "ARRIVAL_COLUMN",
    "DEFAULT_TAGS",
    "DEFAULT_FIX_CONFIG",
    "FixStateConfig",
    "parse_fix",
    "fix_values",
    "tag_column",
    "tag_of",
    "parse_fix_config",
    "load_fix_config",
    "event",
    "sow_current",
    "state_rows",
    "id_history",
    "fold_events",
    "match_files",
    "read_fix_messages",
    "file_events",
]

SOH = "\x01"
UPSERT = "UPSERT"
DELETE = "DELETE"
SNAPSHOT_END = "SNAPSHOT_END"
ACTIONS: Tuple[str, ...] = (UPSERT, DELETE, SNAPSHOT_END)
#: The event table every source produces (AMPS, files, a test). ``Epoch`` numbers the
#: subscription's snapshots; ``Arrival`` is a per-system counter in receive order.
EVENT_COLUMNS: Tuple[str, ...] = ("Topic", "SowKey", "Action", "Epoch", "Arrival", "RawFix")
#: The state rows' arrival column (the fold's tie-break after ``sequence_tag``).
ARRIVAL_COLUMN = "Arrival"

#: Canonical column -> FIX tag, unless a system's ``tags`` says otherwise.
DEFAULT_TAGS: Dict[str, str] = {
    "Moniker": "1",        # Account
    "Symbol": "55",
    "Side": "54",
    "OrderQty": "38",
    "CumQty": "14",
    "LeavesQty": "151",
    "AvgPx": "6",
    "Status": "39",        # OrdStatus
    "Destination": "100",  # ExDestination
}

#: The mapping fix-samples/ and the demo follow. Your topics and tags will differ.
DEFAULT_FIX_CONFIG: Dict[str, Any] = {
    "systems": [
        {"system": "RAPTOR", "topics": ["SOW/RAPTOR/ORDERS"], "files": ["SOW.RAPTOR.*.fix"],
         "levels": [{"level": "CLIENT"}]},
        {"system": "ALGO", "topics": ["SOW/ALGO/ORDERS"], "files": ["SOW.ALGO.*.fix"],
         "tags": {"Destination": "847"},
         "levels": [{"level": "ALGO_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"},
                    {"level": "ALGO_PARENT", "link_tag": "11"}]},
        {"system": "OR", "topics": ["SOW/OR/ORDERS"], "files": ["SOW.OR.*.fix"],
         "levels": [{"level": "OR_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"},
                    {"level": "OR_PARENT", "link_tag": "11"}]},
    ]
}

_TAG_RE = re.compile(r"^[0-9]+$")
_TOPIC_RE = re.compile(r"^[^\s`\\]+$")
_BODY_START = re.compile(r"(?:^|[\s:|\x01])([0-9]+=)")
_SYSTEM_KEYS = frozenset({
    "system", "topics", "files", "filter", "order_id_tag", "alt_id_tag", "id_tags", "tags", "side_codes",
    "status_codes", "sequence_tag", "levels",
})
_LEVEL_KEYS = frozenset({"level", "when", "link_tag"})


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


def parse_fix(raw: Optional[str]) -> Dict[str, str]:
    """Parse one FIX message into ``{tag: value}`` (the first occurrence of a tag wins).

    Accepts what AMPS and FIX logs deliver: a bare body (AMPS ``fix`` topics strip the
    ``8=`` / ``9=`` header and the ``10=`` trailer), or a framed message; SOH, ``|`` or
    ``^A`` delimiters; a log prefix (timestamps, session names) before the first tag.
    """
    text = (raw or "").strip()
    start = text.find("8=FIX")
    if start > 0:
        text = text[start:]
    elif start < 0:
        match = _BODY_START.search(text)
        if match is None:
            return {}
        text = text[match.start(1):]
    if SOH in text:
        parts = text.split(SOH)
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


def fix_values(raw: Optional[str], tags: Sequence[str]) -> List[str]:
    """The raw values of ``tags`` in one message (``""`` where absent). The engine's UDF."""
    fields = parse_fix(raw)
    return [fields.get(tag, "") for tag in tags]


def tag_column(tag: str) -> str:
    """The column a tag's value lands in: ``55`` -> ``T55``."""
    return f"T{tag}"


def tag_of(column: str) -> str:
    """The tag behind a ``T<tag>`` column: ``T55`` -> ``55`` (other names unchanged)."""
    return column[1:] if column[:1] == "T" and column[1:].isdigit() else column


# --------------------------------------------------------------------------------------
# The mapping config
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FixStateConfig:
    """A validated FIX mapping: the compiled field mapping plus the per-system sources."""

    state: StateConfig
    topics: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    files: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    filters: Dict[str, Optional[str]] = field(default_factory=dict)
    order_id_tags: Dict[str, str] = field(default_factory=dict)

    @property
    def systems(self) -> Tuple[StateSystem, ...]:
        return self.state.systems

    def tags(self, system: str) -> Tuple[str, ...]:
        """Every tag the mapping reads for ``system`` -- what the parser picks per message."""
        return tuple(tag_of(n) for n in self.state[system].field_names if tag_of(n) != n)


def _tag(value: Any, where: str) -> str:
    text = str(value) if isinstance(value, int) and not isinstance(value, bool) else value
    if not isinstance(text, str) or not _TAG_RE.match(text):
        raise ValueError(f"{where}: expected a FIX tag number, got {value!r}")
    return text


def _strings(value: Any, where: str, pattern: Optional["re.Pattern[str]"] = None) -> Tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
        raise ValueError(f"{where}: expected a list of non-empty strings")
    if pattern is not None:
        for v in value:
            if not pattern.match(v):
                raise ValueError(f"{where}: {v!r} is not a valid name")
    return tuple(value)


def parse_fix_config(obj: Mapping[str, Any], topology: Topology) -> FixStateConfig:
    """Validate a FIX mapping and compile it to a :class:`~order_tree_recon.states.StateConfig`.

    Raises:
        ValueError: naming the system / level and the key at fault.
    """
    if not isinstance(obj, Mapping) or not isinstance(obj.get("systems"), list) or not obj["systems"]:
        raise ValueError("fix config: expected {\"systems\": [ ... ]} with at least one system")
    compiled: List[Dict[str, Any]] = []
    topics: Dict[str, Tuple[str, ...]] = {}
    files: Dict[str, Tuple[str, ...]] = {}
    filters: Dict[str, Optional[str]] = {}
    order_id_tags: Dict[str, str] = {}
    for i, raw in enumerate(obj["systems"]):
        if not isinstance(raw, Mapping):
            raise ValueError(f"fix config systems[{i}]: expected an object")
        unknown = set(raw) - _SYSTEM_KEYS
        if unknown:
            raise ValueError(f"fix config systems[{i}]: unknown key(s) {sorted(unknown)}")
        name = raw.get("system")
        where = f"fix config [{name}]"
        oid = _tag(raw.get("order_id_tag", "37"), f"{where} order_id_tag")
        alt = raw.get("alt_id_tag", "11")
        alt = _tag(alt, f"{where} alt_id_tag") if alt is not None else None
        id_tags = raw.get("id_tags", ["37", "11", "41"])
        if not isinstance(id_tags, list) or not id_tags:
            raise ValueError(f"{where}: 'id_tags' must be a non-empty list of tags")
        tags = dict(DEFAULT_TAGS)
        overrides = raw.get("tags", {})
        if not isinstance(overrides, Mapping):
            raise ValueError(f"{where}: 'tags' must be an object {{\"CanonicalColumn\": \"tag\"}}")
        for target, tag in overrides.items():
            if target not in states.TARGETS:
                raise ValueError(f"{where}: tags.{target} is not a canonical column {list(states.TARGETS)}")
            tags[target] = _tag(tag, f"{where} tags.{target}") if tag is not None else None
        sequence = raw.get("sequence_tag")
        levels = raw.get("levels")
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"{where}: 'levels' must be a non-empty list")
        compiled_levels: List[Dict[str, Any]] = []
        for j, lv in enumerate(levels):
            if not isinstance(lv, Mapping):
                raise ValueError(f"{where} levels[{j}]: expected an object")
            unknown = set(lv) - _LEVEL_KEYS
            if unknown:
                raise ValueError(f"{where} levels[{j}]: unknown key(s) {sorted(unknown)}")
            level: Dict[str, Any] = {"level": lv.get("level")}
            if lv.get("when") is not None:
                w = lv["when"]
                if not isinstance(w, Mapping) or "tag" not in w or set(w) - {"tag", "equals"}:
                    raise ValueError(f"{where}: {lv.get('level')}.when must be {{\"tag\": T}} or {{\"tag\": T, \"equals\": V}}")
                level["when"] = {"field": tag_column(_tag(w["tag"], f"{where} {lv.get('level')}.when.tag")),
                                 "equals": w.get("equals")}
            if lv.get("link_tag") is not None:
                level["link"] = tag_column(_tag(lv["link_tag"], f"{where} {lv.get('level')}.link_tag"))
            compiled_levels.append(level)
        compiled.append({
            "system": name,
            "order_id": tag_column(oid),
            "alt_id": tag_column(alt) if alt else None,
            "id_fields": [tag_column(_tag(t, f"{where} id_tags")) for t in id_tags],
            "fields": {target: tag_column(tag) for target, tag in tags.items() if tag is not None},
            "side_codes": raw.get("side_codes", "fix"),
            "status_codes": raw.get("status_codes", "fix"),
            "sequence": tag_column(_tag(sequence, f"{where} sequence_tag")) if sequence is not None else None,
            "arrival": ARRIVAL_COLUMN,
            "avg_px_needs_fill": True,
            "levels": compiled_levels,
        })
        topics[name] = _strings(raw.get("topics", []), f"{where} topics", _TOPIC_RE)
        files[name] = _strings(raw.get("files", []), f"{where} files")
        flt = raw.get("filter")
        if flt is not None and (not isinstance(flt, str) or not flt.strip()):
            raise ValueError(f"{where}: 'filter' must be an AMPS content filter string")
        filters[name] = flt
        order_id_tags[name] = oid
    state = states.parse_state_config({"systems": compiled}, topology, label="fix config")
    for system, names in topics.items():
        others = [t for s, ts in topics.items() if s != system for t in ts]
        clash = sorted(set(names) & set(others))
        if clash:
            raise ValueError(f"fix config [{system}]: topic(s) {clash} also belong to another system")
    return FixStateConfig(state, topics, files, filters, order_id_tags)


def load_fix_config(path: Optional[str], topology: Topology) -> FixStateConfig:
    """Load and validate a JSON mapping; ``None`` / ``""`` -> :data:`DEFAULT_FIX_CONFIG`."""
    if not path:
        return parse_fix_config(DEFAULT_FIX_CONFIG, topology)
    with open(path, "r", encoding="utf-8-sig") as handle:
        try:
            obj = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(f"fix config {path}: not valid JSON ({exc})") from None
    return parse_fix_config(obj, topology)


# --------------------------------------------------------------------------------------
# Events, the SOW mirror and the fold (python twins of fixstate_dag)
# --------------------------------------------------------------------------------------

Event = Dict[str, Any]


def event(topic: str, raw: str, arrival: int, action: str = UPSERT, epoch: int = 1, sow_key: str = "") -> Event:
    """One event row (see :data:`EVENT_COLUMNS`)."""
    return {"Topic": topic, "SowKey": sow_key, "Action": action, "Epoch": int(epoch),
            "Arrival": int(arrival), "RawFix": raw}


def _record_key(ev: Mapping[str, Any], order_id_tag: str) -> str:
    sow_key = states.text(ev.get("SowKey"))
    if sow_key:
        return sow_key
    return states.text(parse_fix(ev.get("RawFix")).get(order_id_tag))


def sow_current(events: Iterable[Mapping[str, Any]], order_id_tag: str = "37") -> List[Mapping[str, Any]]:
    """The SOW mirror: the current records among ``events`` (given in arrival order).

    Latest event per ``(Topic, SowKey or order id)``; ``DELETE`` removes; a record older
    than its topic's latest ``SNAPSHOT_END`` epoch is stale. Returned by ``Arrival``.
    """
    records: Dict[Tuple[str, str], Mapping[str, Any]] = {}
    done: Dict[str, int] = {}
    for ev in events:
        topic = str(ev.get("Topic") or "")
        if ev.get("Action") == SNAPSHOT_END:
            done[topic] = int(ev.get("Epoch") or 0)
            continue
        key = _record_key(ev, order_id_tag)
        if key:
            records[(topic, key)] = ev
    current = [
        ev for (topic, _), ev in records.items()
        if ev.get("Action") == UPSERT and (topic not in done or int(ev.get("Epoch") or 0) >= done[topic])
    ]
    return sorted(current, key=lambda ev: int(ev.get("Arrival") or 0))


def state_rows(events: Iterable[Mapping[str, Any]], tags: Sequence[str]) -> List[Dict[str, str]]:
    """Events as fold rows: one ``T<tag>`` column per tag, plus ``Arrival``."""
    rows: List[Dict[str, str]] = []
    for ev in events:
        row = dict(zip((tag_column(t) for t in tags), fix_values(ev.get("RawFix"), tags)))
        row[ARRIVAL_COLUMN] = str(int(ev.get("Arrival") or 0))
        rows.append(row)
    return rows


def id_history(system: StateSystem, events: Iterable[Mapping[str, Any]], tags: Sequence[str]) -> List[Tuple[str, str, str]]:
    """``(System, Id, OrderId)`` for every id field of every ``UPSERT`` event."""
    upserts = [ev for ev in events if ev.get("Action") == UPSERT]
    _, ids = states.fold_system(system, state_rows(upserts, tags))
    return ids


def fold_events(config: FixStateConfig, events: Mapping[str, Iterable[Mapping[str, Any]]]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """Events per system -> canonical rows (with ``Level``) and the id history."""
    rows: List[Dict[str, Any]] = []
    ids: List[Tuple[str, str, str]] = []
    for system in config.systems:
        system_events = list(events.get(system.system, ()))
        tags = config.tags(system.system)
        current = sow_current(system_events, config.order_id_tags[system.system])
        folded, _ = states.fold_system(system, state_rows(current, tags))
        rows.extend(folded)
        ids.extend(id_history(system, system_events, tags))
    return rows, ids


# --------------------------------------------------------------------------------------
# Files: SOW dumps or recorded subscriptions, one FIX message per line
# --------------------------------------------------------------------------------------


def match_files(patterns: Sequence[str], base_dir: str) -> List[str]:
    """The files matching ``patterns`` under ``base_dir``: sorted per pattern, de-duplicated."""
    files: List[str] = []
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(base_dir, pattern))):
            if os.path.isfile(path) and path not in files:
                files.append(path)
    return files


def read_fix_messages(path: str) -> List[str]:
    """The FIX messages in a file: one per line; blank lines and ``#`` comments skipped."""
    with open(path, "r", encoding="utf-8-sig", errors="replace") as handle:
        lines = [line.rstrip("\r\n") for line in handle]
    return [line for line in lines if line.strip() and not line.lstrip().startswith("#") and parse_fix(line)]


def _topic_of(path: str) -> str:
    """A file's topic label: its name without the extension (``SOW.ALGO.ORDERS.fix`` ->
    ``SOW.ALGO.ORDERS``; a file name can't hold the ``/`` of ``SOW/ALGO/ORDERS``)."""
    name = os.path.basename(path)
    return name.rsplit(".", 1)[0] if "." in name else name


def file_events(config: FixStateConfig, base_dir: str) -> Tuple[Dict[str, List[Event]], Dict[str, List[str]]]:
    """Each system's ``files`` as ``UPSERT`` events; the topic is the file name without extension.

    Returns:
        ``({system: events}, {system: files read})``.
    """
    events: Dict[str, List[Event]] = {}
    files_by_system: Dict[str, List[str]] = {}
    for system in config.systems:
        files = match_files(config.files.get(system.system, ()), base_dir)
        rows: List[Event] = []
        for path in files:
            topic = _topic_of(path)
            for raw in read_fix_messages(path):
                rows.append(event(topic, raw, len(rows) + 1))
        events[system.system] = rows
        files_by_system[system.system] = files
    return events, files_by_system
