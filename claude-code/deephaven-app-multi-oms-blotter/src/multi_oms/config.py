"""Topology + tuning configuration -- doc 09 section 3.

Pure stdlib on purpose: every rule below is unit-tested on a bare host python, and
a misconfigured stack must fail at *startup* with an actionable message rather than
silently falling back to a different topology (same policy as ``FIX42_SOURCE``).

Environment (all read once, at app start):

===============================  ==============  ==========================================
Variable                         Default         Meaning
===============================  ==============  ==========================================
``MULTIOMS_HUBS``                see below       topology: JSON array of hub objects
``MULTIOMS_KAFKA_BOOTSTRAP``     ``kafka:9092``  broker for every hub topic
``MULTIOMS_QTY_TOL``             ``1e-6``        absolute tolerance for qty deltas
``MULTIOMS_NOTIONAL_TOL``        ``0.01``        absolute tolerance for notional deltas
``MULTIOMS_PAGE_SIZE``           ``200``         blotter page size (UI default)
===============================  ==============  ==========================================
"""

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__all__ = [
    "HUBS_ENV",
    "BOOTSTRAP_ENV",
    "QTY_TOL_ENV",
    "NOTIONAL_TOL_ENV",
    "PAGE_SIZE_ENV",
    "DEFAULT_HUBS_JSON",
    "DEFAULT_BOOTSTRAP",
    "DEFAULT_QTY_TOL",
    "DEFAULT_NOTIONAL_TOL",
    "DEFAULT_PAGE_SIZE",
    "HubConfig",
    "Topology",
    "parse_topology",
    "load_topology",
    "kafka_bootstrap",
    "qty_tolerance",
    "notional_tolerance",
    "page_size",
    "page_bounds",
]

#: Environment variable holding the topology JSON.
HUBS_ENV = "MULTIOMS_HUBS"
#: Environment variable holding the Kafka bootstrap servers for every hub topic.
BOOTSTRAP_ENV = "MULTIOMS_KAFKA_BOOTSTRAP"
#: Environment variable holding the absolute quantity tolerance.
QTY_TOL_ENV = "MULTIOMS_QTY_TOL"
#: Environment variable holding the absolute notional tolerance.
NOTIONAL_TOL_ENV = "MULTIOMS_NOTIONAL_TOL"
#: Environment variable holding the blotter page size.
PAGE_SIZE_ENV = "MULTIOMS_PAGE_SIZE"

#: Compose-network default -- the Kafka service name.
DEFAULT_BOOTSTRAP = "kafka:9092"
#: Absolute tolerance for ``CumQty`` / ``LeavesQty`` deltas (doc 09 section 3).
DEFAULT_QTY_TOL = 1e-6
#: Absolute tolerance for ``AvgPx * CumQty`` deltas (doc 09 section 3).
DEFAULT_NOTIONAL_TOL = 0.01
#: Blotter page size (doc 09 section 6).
DEFAULT_PAGE_SIZE = 200

#: The default topology: exactly the TODO's four hubs, topics and link tags.
DEFAULT_HUBS_JSON = """[
  {"name": "OMS-A",        "topic": "fix42.oms-a"},
  {"name": "OMS-B-parent", "topic": "fix42.oms-b-parent", "upstream": "OMS-A",        "link_tag": 16666},
  {"name": "OMS-B-child",  "topic": "fix42.oms-b-child",  "upstream": "OMS-B-parent", "link_tag": 16667},
  {"name": "OMS-C",        "topic": "fix42.oms-c",        "upstream": "OMS-B-child",  "link_tag": 16668}
]"""

#: Keys a hub object may carry; anything else is a typo worth failing on.
_HUB_KEYS = frozenset({"name", "topic", "upstream", "link_tag"})


class HubConfig:
    """One OMS hub: its tape, its upstream edge and its static depth.

    A plain class rather than a ``dataclass``: this module is imported by
    ``app.py``, which Application Mode may execute under a ``__name__`` that is not
    registered in ``sys.modules`` -- which breaks ``dataclasses``' type
    introspection at class-creation time (same reasoning as ``dh_app.app.Runtime``).
    """

    __slots__ = ("name", "topic", "upstream", "link_tag", "depth")

    def __init__(
        self,
        name: str,
        topic: str,
        upstream: str = "",
        link_tag: int = 0,
        depth: int = 0,
    ) -> None:
        """Store one validated hub definition.

        Args:
            name: Hub name, unique across the topology (the ``Oms`` column value).
            topic: Kafka topic carrying this hub's drop-copy tape.
            upstream: Name of the hub this one routes from; ``""`` for a root.
            link_tag: FIX tag whose value names the upstream order; ``0`` for a root.
            depth: Distance from this hub's root (``0`` for a root).
        """
        self.name = name
        self.topic = topic
        self.upstream = upstream
        self.link_tag = link_tag
        self.depth = depth

    @property
    def is_root(self) -> bool:
        """True when this hub has no upstream (``HubDepth == 0``)."""
        return not self.upstream

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        if self.is_root:
            return f"{self.name} <- {self.topic} (root)"
        return f"{self.name} <- {self.topic} (upstream {self.upstream} via tag {self.link_tag})"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"HubConfig(name={self.name!r}, topic={self.topic!r}, "
            f"upstream={self.upstream!r}, link_tag={self.link_tag!r}, depth={self.depth!r})"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, HubConfig):
            return NotImplemented
        return (
            self.name == other.name
            and self.topic == other.topic
            and self.upstream == other.upstream
            and self.link_tag == other.link_tag
            and self.depth == other.depth
        )


class Topology:
    """The validated hub graph: an ordered, acyclic forest with at most one parent."""

    __slots__ = ("hubs", "_by_name")

    def __init__(self, hubs: Sequence[HubConfig]) -> None:
        """Wrap an already-validated, depth-annotated hub sequence."""
        self.hubs: Tuple[HubConfig, ...] = tuple(hubs)
        self._by_name: Dict[str, HubConfig] = {hub.name: hub for hub in self.hubs}

    # -- lookups -----------------------------------------------------------------

    def __len__(self) -> int:
        """Number of configured hubs (``K`` in doc 09 section 5.3)."""
        return len(self.hubs)

    def __iter__(self):
        """Iterate the hubs in configuration order."""
        return iter(self.hubs)

    def __contains__(self, name: object) -> bool:
        """True when ``name`` is a configured hub name."""
        return name in self._by_name

    def get(self, name: str) -> Optional[HubConfig]:
        """Return the hub called ``name``, or ``None``."""
        return self._by_name.get(name)

    @property
    def names(self) -> Tuple[str, ...]:
        """Hub names in configuration order."""
        return tuple(hub.name for hub in self.hubs)

    @property
    def topics(self) -> Tuple[str, ...]:
        """Hub topics in configuration order."""
        return tuple(hub.topic for hub in self.hubs)

    @property
    def roots(self) -> Tuple[HubConfig, ...]:
        """Every hub without an upstream."""
        return tuple(hub for hub in self.hubs if hub.is_root)

    @property
    def linked_hubs(self) -> Tuple[HubConfig, ...]:
        """Every hub *with* an upstream -- i.e. one per configured edge."""
        return tuple(hub for hub in self.hubs if not hub.is_root)

    @property
    def max_depth(self) -> int:
        """Deepest ``HubDepth`` in the topology (``0`` for a flat forest)."""
        return max((hub.depth for hub in self.hubs), default=0)

    def depth_of(self, name: str) -> int:
        """``HubDepth`` of ``name``.

        Raises:
            KeyError: If ``name`` is not a configured hub.
        """
        return self._by_name[name].depth

    def children_of(self, name: str) -> Tuple[HubConfig, ...]:
        """Hubs whose ``upstream`` is ``name`` (hub-graph fan-out, usually 0 or 1)."""
        return tuple(hub for hub in self.hubs if hub.upstream == name)

    def describe(self) -> str:
        """Multi-line summary for the startup banner."""
        return "\n".join(f"    {hub.describe()}" for hub in self.hubs)


# --------------------------------------------------------------------------------------
# Parsing + validation
# --------------------------------------------------------------------------------------


def _fail(message: str) -> None:
    """Raise a :class:`ValueError` prefixed with the offending variable."""
    raise ValueError(f"{HUBS_ENV}: {message}")


def _hub_text(entry: Mapping[str, Any], key: str, index: int, required: bool) -> str:
    """Read one string field out of a hub object."""
    value = entry.get(key)
    if value is None or value == "":
        if required:
            _fail(f"hub #{index} is missing a non-empty {key!r}")
        return ""
    if not isinstance(value, str):
        _fail(f"hub #{index} has {key}={value!r}; expected a string")
    text = value.strip()
    if not text and required:
        _fail(f"hub #{index} is missing a non-empty {key!r}")
    return text


def _link_tag(entry: Mapping[str, Any], index: int, name: str) -> int:
    """Read and validate a hub's ``link_tag`` (``0`` when absent)."""
    if "link_tag" not in entry or entry["link_tag"] is None or entry["link_tag"] == "":
        return 0
    value = entry["link_tag"]
    if isinstance(value, bool):
        _fail(f"hub {name!r} has link_tag={value!r}; expected a positive integer FIX tag")
    if isinstance(value, str):
        try:
            value = int(value.strip())
        except ValueError:
            _fail(f"hub {name!r} has link_tag={entry['link_tag']!r}; expected a positive integer FIX tag")
    if not isinstance(value, int):
        _fail(f"hub {name!r} has link_tag={entry['link_tag']!r}; expected a positive integer FIX tag")
    if value <= 0:
        _fail(f"hub {name!r} has link_tag={value}; a FIX tag must be a positive integer")
    return int(value)


def _depths(by_name: Mapping[str, HubConfig]) -> Dict[str, int]:
    """Compute every hub's distance from its root, rejecting cycles.

    Args:
        by_name: Hubs keyed by name; each carries at most one ``upstream``.

    Returns:
        ``{hub name: depth}`` with roots at ``0``.

    Raises:
        ValueError: If following ``upstream`` links revisits a hub (a cycle).
    """
    cache: Dict[str, int] = {}
    for start in by_name:
        chain: List[str] = []
        seen = set()
        cur = start
        while True:
            if cur in cache:
                base = cache[cur]
                break
            if cur in seen:
                loop = " -> ".join(chain + [cur])
                _fail(
                    f"the hub graph has a cycle ({loop}); "
                    "'upstream' links must form a forest, not a loop"
                )
            seen.add(cur)
            upstream = by_name[cur].upstream
            if not upstream:
                cache[cur] = 0
                base = 0
                break
            chain.append(cur)
            cur = upstream
        depth = base
        for name in reversed(chain):
            depth += 1
            cache[name] = depth
    return cache


def parse_topology(raw: Optional[str] = None) -> Topology:
    """Parse and validate a ``MULTIOMS_HUBS`` payload.

    Args:
        raw: The JSON text; ``None``/empty uses :data:`DEFAULT_HUBS_JSON`.

    Returns:
        The validated, depth-annotated :class:`Topology`.

    Raises:
        ValueError: On any violation of doc 09 section 3 -- malformed JSON, duplicate
            names or topics, an ``upstream`` that names no hub, ``link_tag`` present
            without ``upstream`` (or vice versa), a non-positive ``link_tag``, a
            cycle, or no root. Every message names the offending hub: a topology
            typo must stop the app, never route a tape to the wrong parent.
    """
    text = (raw if raw is not None else "").strip() or DEFAULT_HUBS_JSON
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        _fail(f"is not valid JSON ({exc}); expected an array of hub objects like {DEFAULT_HUBS_JSON}")
    if not isinstance(parsed, list):
        _fail(f"must be a JSON array of hub objects, got {type(parsed).__name__}")
    if not parsed:
        _fail("must configure at least one hub")

    hubs: List[HubConfig] = []
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            _fail(f"hub #{index} must be a JSON object, got {type(entry).__name__}")
        unknown = sorted(set(entry) - _HUB_KEYS)
        if unknown:
            _fail(
                f"hub #{index} has unknown key(s) {unknown}; "
                f"expected only {sorted(_HUB_KEYS)}"
            )
        name = _hub_text(entry, "name", index, required=True)
        topic = _hub_text(entry, "topic", index, required=True)
        upstream = _hub_text(entry, "upstream", index, required=False)
        link_tag = _link_tag(entry, index, name)

        if upstream and not link_tag:
            _fail(
                f"hub {name!r} declares upstream {upstream!r} but no 'link_tag'; "
                "a downstream hub must name the FIX tag carrying its parent's id"
            )
        if link_tag and not upstream:
            _fail(
                f"hub {name!r} declares link_tag={link_tag} but no 'upstream'; "
                "a root hub has nothing to link to"
            )
        if upstream == name:
            _fail(f"hub {name!r} lists itself as its own upstream")
        hubs.append(HubConfig(name=name, topic=topic, upstream=upstream, link_tag=link_tag))

    by_name: Dict[str, HubConfig] = {}
    for hub in hubs:
        if hub.name in by_name:
            _fail(f"hub name {hub.name!r} is used more than once; hub names must be unique")
        by_name[hub.name] = hub

    seen_topics: Dict[str, str] = {}
    for hub in hubs:
        owner = seen_topics.get(hub.topic)
        if owner is not None:
            _fail(
                f"topic {hub.topic!r} is claimed by both {owner!r} and {hub.name!r}; "
                "each hub needs its own tape"
            )
        seen_topics[hub.topic] = hub.name

    for hub in hubs:
        if hub.upstream and hub.upstream not in by_name:
            _fail(
                f"hub {hub.name!r} has upstream {hub.upstream!r}, which is not a configured "
                f"hub; known hubs are {sorted(by_name)}"
            )

    depths = _depths(by_name)
    for hub in hubs:
        hub.depth = depths[hub.name]

    if not any(hub.is_root for hub in hubs):
        _fail("no root hub: exactly one hub per chain must be configured without an 'upstream'")

    return Topology(hubs)


def load_topology(env: Optional[Mapping[str, str]] = None) -> Topology:
    """Parse :data:`HUBS_ENV` from the environment.

    Args:
        env: Environment to read; defaults to :data:`os.environ`.

    Returns:
        The validated :class:`Topology` (the doc 09 section 3 default when unset).

    Raises:
        ValueError: See :func:`parse_topology`.
    """
    source = os.environ if env is None else env
    return parse_topology(source.get(HUBS_ENV))


# --------------------------------------------------------------------------------------
# Scalar tuning knobs
# --------------------------------------------------------------------------------------


def kafka_bootstrap(env: Optional[Mapping[str, str]] = None) -> str:
    """Return the Kafka bootstrap servers used for every hub topic."""
    source = os.environ if env is None else env
    return (source.get(BOOTSTRAP_ENV) or "").strip() or DEFAULT_BOOTSTRAP


def _tolerance(name: str, default: float, env: Optional[Mapping[str, str]]) -> float:
    """Read one non-negative, finite float tolerance."""
    source = os.environ if env is None else env
    raw = (source.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a number; expected e.g. {default!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"{name}={raw!r} must be finite; expected e.g. {default!r}")
    if value < 0:
        raise ValueError(f"{name}={raw!r} must be >= 0; expected e.g. {default!r}")
    return value


def qty_tolerance(env: Optional[Mapping[str, str]] = None) -> float:
    """Absolute tolerance for ``CumQty`` / ``LeavesQty`` deltas."""
    return _tolerance(QTY_TOL_ENV, DEFAULT_QTY_TOL, env)


def notional_tolerance(env: Optional[Mapping[str, str]] = None) -> float:
    """Absolute tolerance for ``AvgPx * CumQty`` deltas."""
    return _tolerance(NOTIONAL_TOL_ENV, DEFAULT_NOTIONAL_TOL, env)


def page_size(env: Optional[Mapping[str, str]] = None) -> int:
    """Blotter page size.

    Raises:
        ValueError: If :data:`PAGE_SIZE_ENV` is set to something that is not a
            positive integer -- a zero page would render an always-empty blotter.
    """
    source = os.environ if env is None else env
    raw = (source.get(PAGE_SIZE_ENV) or "").strip()
    if not raw:
        return DEFAULT_PAGE_SIZE
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(
            f"{PAGE_SIZE_ENV}={raw!r} is not an integer; expected e.g. {DEFAULT_PAGE_SIZE}"
        ) from None
    if value <= 0:
        raise ValueError(
            f"{PAGE_SIZE_ENV}={raw!r} must be a positive integer; expected e.g. {DEFAULT_PAGE_SIZE}"
        )
    return value


def page_bounds(page: int, size: int, total: Optional[int]) -> Dict[str, int]:
    """Clamp a page request against a live row count (doc 09 section 6 paging).

    Pure arithmetic, kept here rather than in :mod:`multi_oms.dashboard` so it is
    unit-testable without ``deephaven.ui``.

    Args:
        page: Zero-based page index (negative values clamp to ``0``).
        size: Rows per page; values below ``1`` clamp to ``1``.
        total: Live row count of the filtered table, or ``None`` when the count is
            not available (older ``deephaven.ui`` without ``use_cell_data``).

    Returns:
        ``{"page", "pages", "start", "end", "first_row", "last_row", "total"}`` --
        ``start``/``end`` feed ``Table.slice``; ``first_row``/``last_row`` are the
        1-based inclusive labels for the "rows X-Y of N" caption (both ``0`` when
        the page is empty). ``total``/``pages`` are ``-1``/``1`` when unknown.
    """
    size = max(1, int(size))
    page = max(0, int(page))
    if total is None or total < 0:
        start = page * size
        return {
            "page": page,
            "pages": 1,
            "start": start,
            "end": start + size,
            "first_row": start + 1,
            "last_row": start + size,
            "total": -1,
        }
    total = int(total)
    pages = max(1, -(-total // size))  # ceil without floats
    page = min(page, pages - 1)
    start = page * size
    end = min(start + size, total)
    empty = start >= end
    return {
        "page": page,
        "pages": pages,
        "start": start,
        "end": start + size,
        "first_row": 0 if empty else start + 1,
        "last_row": 0 if empty else end,
        "total": total,
    }
