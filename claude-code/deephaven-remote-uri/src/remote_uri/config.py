"""``REMOTEURI_*`` configuration -- doc 10 section 4.

Pure stdlib on purpose (``multi_oms.config`` is equally pure): every rule below is
unit-tested on a bare host python, and a misconfigured fleet must fail at *startup*
with an actionable message rather than resolve half a topology and produce silently
wrong exposure numbers. Same policy as doc 09 section 3 and ``FIX42_SOURCE``.

Environment, all read once at app start:

===================================  ==================  ===============================
Variable                             Default             Meaning
===================================  ==================  ===============================
``REMOTEURI_ROLE``                   *(required)*        ``leaf`` or ``collector``
``REMOTEURI_HUBS``                   doc 09's four hubs  the **full** topology (JSON)
``REMOTEURI_QTY_TOL``                ``1e-6``            absolute qty tolerance
``REMOTEURI_NOTIONAL_TOL``           ``0.01``            absolute notional tolerance
``REMOTEURI_LEAF_NAME``              *(required, leaf)*  e.g. ``DH1``
``REMOTEURI_LEAF_HUBS``              *(required, leaf)*  comma-separated hub names
``REMOTEURI_AMPS_URI``               ``tcp://amps:9007/amps/fix``
``REMOTEURI_AMPS_BOOKMARK``          ``epoch``           replay position
``REMOTEURI_AMPS_FILTER``            ``""``              server-side content filter
``REMOTEURI_AMPS_MAX_PENDING``       ``250000``          AMPS -> update-graph buffer
``REMOTEURI_EXEC_RING``              ``0``               exec/event ring capacity
``REMOTEURI_STATS_PERIOD_MS``        ``5000``            ``rx_leaf_stats`` refresh
``REMOTEURI_LEAVES``                 the 2-leaf demo     JSON ``[{name,uri,hubs}]``
``REMOTEURI_RESOLVER``               ``uri``             ``uri`` or ``barrage``
``REMOTEURI_CONNECT_TIMEOUT``        ``300``             seconds to keep retrying
``REMOTEURI_CONNECT_INTERVAL``       ``5``               seconds between attempts
``REMOTEURI_MD_SYMBOLS``             8 symbols           ``SYM:refpx,...``
``REMOTEURI_MD_PERIOD_MS``           ``1000``            quote refresh period
``REMOTEURI_MD_SPREAD_BPS``          ``5``               half-spread in bp
``REMOTEURI_MD_SEED``                ``42``              random-walk seed
===================================  ==================  ===============================
"""

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from multi_oms.config import DEFAULT_HUBS_JSON, HubConfig, Topology, parse_topology

from remote_uri import marketdata, uris

__all__ = [
    "ROLE_ENV",
    "HUBS_ENV",
    "QTY_TOL_ENV",
    "NOTIONAL_TOL_ENV",
    "LEAF_NAME_ENV",
    "LEAF_HUBS_ENV",
    "AMPS_URI_ENV",
    "AMPS_BOOKMARK_ENV",
    "AMPS_FILTER_ENV",
    "AMPS_MAX_PENDING_ENV",
    "EXEC_RING_ENV",
    "STATS_PERIOD_ENV",
    "LEAVES_ENV",
    "RESOLVER_ENV",
    "CONNECT_TIMEOUT_ENV",
    "CONNECT_INTERVAL_ENV",
    "MD_SYMBOLS_ENV",
    "MD_PERIOD_ENV",
    "MD_SPREAD_ENV",
    "MD_SEED_ENV",
    "ROLE_LEAF",
    "ROLE_COLLECTOR",
    "ROLES",
    "RESOLVER_URI",
    "RESOLVER_BARRAGE",
    "RESOLVERS",
    "DEFAULT_AMPS_URI",
    "DEFAULT_AMPS_BOOKMARK",
    "DEFAULT_AMPS_MAX_PENDING",
    "DEFAULT_EXEC_RING",
    "DEFAULT_STATS_PERIOD_MS",
    "DEFAULT_LEAVES_JSON",
    "DEFAULT_RESOLVER",
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_CONNECT_INTERVAL",
    "DEFAULT_MD_SYMBOLS",
    "DEFAULT_MD_PERIOD_MS",
    "DEFAULT_MD_SPREAD_BPS",
    "DEFAULT_MD_SEED",
    "DEFAULT_QTY_TOL",
    "DEFAULT_NOTIONAL_TOL",
    "LeafRef",
    "LeafSettings",
    "CollectorSettings",
    "role",
    "load_topology",
    "restrict_topology",
    "parse_leaves",
    "unassigned_hubs",
    "qty_tolerance",
    "notional_tolerance",
    "load_leaf_settings",
    "load_collector_settings",
]

# --------------------------------------------------------------------------------------
# Variable names
# --------------------------------------------------------------------------------------

ROLE_ENV = "REMOTEURI_ROLE"
HUBS_ENV = "REMOTEURI_HUBS"
QTY_TOL_ENV = "REMOTEURI_QTY_TOL"
NOTIONAL_TOL_ENV = "REMOTEURI_NOTIONAL_TOL"

LEAF_NAME_ENV = "REMOTEURI_LEAF_NAME"
LEAF_HUBS_ENV = "REMOTEURI_LEAF_HUBS"
AMPS_URI_ENV = "REMOTEURI_AMPS_URI"
AMPS_BOOKMARK_ENV = "REMOTEURI_AMPS_BOOKMARK"
AMPS_FILTER_ENV = "REMOTEURI_AMPS_FILTER"
AMPS_MAX_PENDING_ENV = "REMOTEURI_AMPS_MAX_PENDING"
EXEC_RING_ENV = "REMOTEURI_EXEC_RING"
STATS_PERIOD_ENV = "REMOTEURI_STATS_PERIOD_MS"

LEAVES_ENV = "REMOTEURI_LEAVES"
RESOLVER_ENV = "REMOTEURI_RESOLVER"
CONNECT_TIMEOUT_ENV = "REMOTEURI_CONNECT_TIMEOUT"
CONNECT_INTERVAL_ENV = "REMOTEURI_CONNECT_INTERVAL"
MD_SYMBOLS_ENV = "REMOTEURI_MD_SYMBOLS"
MD_PERIOD_ENV = "REMOTEURI_MD_PERIOD_MS"
MD_SPREAD_ENV = "REMOTEURI_MD_SPREAD_BPS"
MD_SEED_ENV = "REMOTEURI_MD_SEED"

# --------------------------------------------------------------------------------------
# Values
# --------------------------------------------------------------------------------------

#: The two roles. There is no default: an app that guessed would fold tapes on a
#: collector or resolve nothing on a leaf, both silently.
ROLE_LEAF = "leaf"
ROLE_COLLECTOR = "collector"
ROLES: Tuple[str, ...] = (ROLE_LEAF, ROLE_COLLECTOR)

#: ``deephaven.uri.resolve`` (the default) versus an explicit ``barrage_session``.
RESOLVER_URI = "uri"
RESOLVER_BARRAGE = "barrage"
RESOLVERS: Tuple[str, ...] = (RESOLVER_URI, RESOLVER_BARRAGE)

#: Compose-network default; the message type is part of the AMPS URI.
DEFAULT_AMPS_URI = "tcp://amps:9007/amps/fix"
#: Replay the whole journal -- the deterministic-rebuild contract (doc 03 section 3.3).
DEFAULT_AMPS_BOOKMARK = "epoch"
#: Bound of the AMPS reader -> update-graph hand-off, per hub.
DEFAULT_AMPS_MAX_PENDING = 250_000
#: ``0`` = append-only ``oms_executions`` / ``oms_events``; > 0 = ring capacity.
DEFAULT_EXEC_RING = 0
#: ``rx_leaf_stats`` refresh period.
DEFAULT_STATS_PERIOD_MS = 5000

#: Exactly the demo compose (doc 10 sections 4.3 and 11).
DEFAULT_LEAVES_JSON = """[
  {"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]},
  {"name": "DH2", "uri": "dh+plain://dh2:10000", "hubs": ["OMS-B-parent", "OMS-B-child", "OMS-C"]}
]"""

DEFAULT_RESOLVER = RESOLVER_URI
#: Seconds the collector keeps retrying before giving up and staying up anyway.
DEFAULT_CONNECT_TIMEOUT = 300.0
#: Seconds between resolve attempts.
DEFAULT_CONNECT_INTERVAL = 5.0

#: The generator's eight-symbol universe.
DEFAULT_MD_SYMBOLS = (
    "AAPL:190,MSFT:420,NVDA:120,AMZN:180,TSLA:250,META:500,GOOGL:170,JPM:200"
)
DEFAULT_MD_PERIOD_MS = 1000
DEFAULT_MD_SPREAD_BPS = 5.0
DEFAULT_MD_SEED = 42

#: Same tolerances as doc 09 section 3 -- the collector runs doc 09's recon.
DEFAULT_QTY_TOL = 1e-6
DEFAULT_NOTIONAL_TOL = 0.01

#: Keys a ``REMOTEURI_LEAVES`` entry may carry.
_LEAF_KEYS = frozenset({"name", "uri", "hubs"})


# --------------------------------------------------------------------------------------
# Small typed readers
# --------------------------------------------------------------------------------------


def _text(env: Optional[Mapping[str, str]], name: str, default: str = "") -> str:
    """Read one trimmed string from the environment."""
    source = os.environ if env is None else env
    return (source.get(name) or "").strip() or default


def _number(
    env: Optional[Mapping[str, str]],
    name: str,
    default: float,
    minimum: Optional[float] = None,
    integral: bool = False,
) -> float:
    """Read one finite number, failing loudly rather than falling back silently."""
    raw = _text(env, name)
    if not raw:
        return default
    try:
        value = int(raw) if integral else float(raw)
    except ValueError:
        kind = "an integer" if integral else "a number"
        raise ValueError(f"{name}={raw!r} is not {kind}; expected e.g. {default!r}") from None
    if not math.isfinite(float(value)):
        raise ValueError(f"{name}={raw!r} must be finite; expected e.g. {default!r}")
    if minimum is not None and float(value) < minimum:
        raise ValueError(f"{name}={raw!r} must be >= {minimum}; expected e.g. {default!r}")
    return value


def _tolerance(env: Optional[Mapping[str, str]], name: str, default: float) -> float:
    """Read one non-negative tolerance (doc 09's rules, doc 10's variable names)."""
    return float(_number(env, name, default, minimum=0.0))


def qty_tolerance(env: Optional[Mapping[str, str]] = None) -> float:
    """Absolute tolerance for ``CumQty`` / ``LeavesQty`` deltas."""
    return _tolerance(env, QTY_TOL_ENV, DEFAULT_QTY_TOL)


def notional_tolerance(env: Optional[Mapping[str, str]] = None) -> float:
    """Absolute tolerance for ``AvgPx * CumQty`` deltas."""
    return _tolerance(env, NOTIONAL_TOL_ENV, DEFAULT_NOTIONAL_TOL)


# --------------------------------------------------------------------------------------
# Role and topology
# --------------------------------------------------------------------------------------


def role(env: Optional[Mapping[str, str]] = None) -> str:
    """Read :data:`ROLE_ENV`.

    Returns:
        ``"leaf"`` or ``"collector"``.

    Raises:
        ValueError: When unset or unknown. There is deliberately no default app to
            fall into: a leaf that came up as a collector would resolve nothing and
            a collector that came up as a leaf would open AMPS subscriptions for
            hubs it does not own.
    """
    value = _text(env, ROLE_ENV).lower()
    if not value:
        raise ValueError(
            f"{ROLE_ENV} is not set; it must be one of {list(ROLES)} "
            "(the leaf folds AMPS hub tapes, the collector resolves the leaves' exports)"
        )
    if value not in ROLES:
        raise ValueError(f"{ROLE_ENV}={value!r} is unknown; expected one of {list(ROLES)}")
    return value


def load_topology(env: Optional[Mapping[str, str]] = None) -> Topology:
    """Parse :data:`HUBS_ENV` as the **full** fleet topology.

    Shape and validation are doc 09's (``multi_oms.config.parse_topology``); only
    the variable name differs, so one JSON document describes the fleet whether it
    runs in one server or in N.

    Raises:
        ValueError: See ``multi_oms.config.parse_topology``. The message names
            ``MULTIOMS_HUBS`` -- it is the same payload, and re-wrapping every
            message would hide the doc 09 rule that was violated.
    """
    source = os.environ if env is None else env
    return parse_topology(source.get(HUBS_ENV) or DEFAULT_HUBS_JSON)


def restrict_topology(
    full: Topology,
    names: Sequence[str],
    env_name: str = LEAF_HUBS_ENV,
) -> Topology:
    """Build a leaf's sub-topology out of the validated full one.

    Each selected hub keeps its ``link_tag``, ``upstream`` and ``depth`` from the
    full topology -- ``MultiOmsPipeline`` reads only ``name`` and ``link_tag``, but
    an honest ``depth`` keeps a leaf's own panels readable. Linking is *not*
    attempted on a leaf (a hub's upstream usually lives on another server), which is
    why an upstream that is absent from the selection is not an error here.

    Args:
        full: The validated fleet topology.
        names: Hub names to keep; order is irrelevant (the result follows the full
            topology's configuration order, so two leaves listing the same hubs in
            different orders fold them identically).
        env_name: Variable named in error messages.

    Returns:
        A :class:`~multi_oms.config.Topology` over the selected hubs.

    Raises:
        ValueError: If a name is not a configured hub, or the selection is empty --
            a leaf with no hubs would come up healthy, export four empty tables and
            silently remove its hubs from the fleet.
    """
    wanted: List[str] = []
    for entry in names:
        text = ("" if entry is None else str(entry)).strip()
        if not text:
            continue
        if full.get(text) is None:
            raise ValueError(
                f"{env_name}: hub {text!r} is not in the topology; "
                f"known hubs are {list(full.names)}"
            )
        if text not in wanted:
            wanted.append(text)
    if not wanted:
        raise ValueError(
            f"{env_name} selects no hubs; it must name at least one of {list(full.names)}"
        )
    selected: List[HubConfig] = [hub for hub in full if hub.name in wanted]
    return Topology(selected)


def _split_names(raw: str) -> List[str]:
    """Split a comma/whitespace separated name list, dropping empties."""
    return [part for part in raw.replace(",", " ").split() if part]


# --------------------------------------------------------------------------------------
# Leaves (the collector's partition of the topology)
# --------------------------------------------------------------------------------------


class LeafRef:
    """One entry of ``REMOTEURI_LEAVES``: a leaf server and the hubs it owns.

    A plain class rather than a ``dataclass``: Application Mode may exec ``app.py``
    under a ``__name__`` that is not registered in ``sys.modules``, which breaks
    ``dataclasses``' type introspection at class-creation time (same reasoning as
    ``multi_oms.config.HubConfig``).
    """

    __slots__ = ("name", "uri", "hubs")

    def __init__(self, name: str, uri: str, hubs: Sequence[str]) -> None:
        """Store one validated leaf reference."""
        self.name = name
        self.uri = uri
        self.hubs: Tuple[str, ...] = tuple(hubs)

    @property
    def suffix(self) -> str:
        """Lower-case, identifier-safe global suffix (``"DH1"`` -> ``"dh1"``)."""
        return uris.global_suffix(self.name)

    @property
    def host_port(self) -> Tuple[str, int]:
        """``(host, port)`` for ``barrage_session``."""
        return uris.host_port(self.uri, label=f"{LEAVES_ENV}: leaf {self.name!r} uri")

    def scope_uri(self, name: str) -> str:
        """``deephaven.uri.resolve`` URI of one of this leaf's exports."""
        return uris.scope_uri(self.uri, name)

    def global_name(self, base: str) -> str:
        """Collector-side global holding this leaf's copy of ``base``."""
        return uris.leaf_global_name(base, self.name)

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        return f"{self.name} <- {self.uri} hubs={list(self.hubs)}"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"LeafRef(name={self.name!r}, uri={self.uri!r}, hubs={list(self.hubs)!r})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LeafRef):
            return NotImplemented
        return self.name == other.name and self.uri == other.uri and self.hubs == other.hubs


def _fail_leaves(message: str) -> None:
    """Raise a :class:`ValueError` prefixed with :data:`LEAVES_ENV`."""
    raise ValueError(f"{LEAVES_ENV}: {message}")


def parse_leaves(raw: Optional[str], topology: Topology) -> Tuple[LeafRef, ...]:
    """Parse and validate ``REMOTEURI_LEAVES`` against the full topology.

    Doc 10 section 4.3's rules, every one of them a startup error:

    * the payload is a non-empty JSON array of ``{"name", "uri", "hubs"}`` objects;
    * leaf names are unique (they become the ``rx_*_<leaf>`` global suffixes and the
      ``Leaf`` column, so a duplicate would silently overwrite a leaf's tables);
    * ``uri`` is ``dh+plain://host[:port]`` or ``dh://...`` (:mod:`remote_uri.uris`);
    * every listed hub exists in the topology;
    * **no hub is assigned to two leaves** -- a duplicate makes ``orders_all``
      non-unique on ``GlobalKey`` and doc 09's linking ``natural_join`` fails at
      runtime with a message about the join, not about the configuration.

    A hub assigned to *no* leaf is legal and only warned about (its downstream hops
    show as ``DANGLING`` / ``NO_LINK``, which is a true statement about a fleet that
    is not folding that tape).

    Args:
        raw: The JSON text; ``None``/blank uses :data:`DEFAULT_LEAVES_JSON`.
        topology: The validated full topology.

    Returns:
        The leaves in configuration order.

    Raises:
        ValueError: On any of the rules above.
    """
    text = (raw or "").strip() or DEFAULT_LEAVES_JSON
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        _fail_leaves(
            f"is not valid JSON ({exc}); expected an array of leaf objects like "
            f"{DEFAULT_LEAVES_JSON}"
        )
    if not isinstance(parsed, list):
        _fail_leaves(f"must be a JSON array of leaf objects, got {type(parsed).__name__}")
    if not parsed:
        _fail_leaves("must configure at least one leaf")

    leaves: List[LeafRef] = []
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            _fail_leaves(f"leaf #{index} must be a JSON object, got {type(entry).__name__}")
        unknown = sorted(set(entry) - _LEAF_KEYS)
        if unknown:
            _fail_leaves(
                f"leaf #{index} has unknown key(s) {unknown}; expected only {sorted(_LEAF_KEYS)}"
            )
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            _fail_leaves(f"leaf #{index} is missing a non-empty 'name' (e.g. \"DH1\")")
        name = name.strip()
        try:
            uri = uris.normalize_leaf_uri(entry.get("uri"), label=f"leaf {name!r} 'uri'")
        except ValueError as exc:
            _fail_leaves(str(exc))
        hubs = entry.get("hubs")
        if isinstance(hubs, str):
            hubs = _split_names(hubs)
        if not isinstance(hubs, list) or not hubs:
            _fail_leaves(
                f"leaf {name!r} is missing a non-empty 'hubs' array; a leaf with no hubs "
                "would come up healthy and export four empty tables"
            )
        cleaned: List[str] = []
        for hub in hubs:
            if not isinstance(hub, str) or not hub.strip():
                _fail_leaves(f"leaf {name!r} has a non-string hub entry {hub!r}")
            hub_name = hub.strip()
            if topology.get(hub_name) is None:
                _fail_leaves(
                    f"leaf {name!r} lists hub {hub_name!r}, which is not in the topology; "
                    f"known hubs are {list(topology.names)}"
                )
            if hub_name in cleaned:
                _fail_leaves(f"leaf {name!r} lists hub {hub_name!r} more than once")
            cleaned.append(hub_name)
        leaves.append(LeafRef(name=name, uri=uri, hubs=cleaned))

    seen_names: Dict[str, int] = {}
    for index, leaf in enumerate(leaves):
        if leaf.name in seen_names:
            _fail_leaves(
                f"leaf name {leaf.name!r} is used more than once (entries "
                f"#{seen_names[leaf.name]} and #{index}); names become the "
                "rx_*_<leaf> globals and the Leaf column"
            )
        seen_names[leaf.name] = index

    seen_suffix: Dict[str, str] = {}
    for leaf in leaves:
        owner = seen_suffix.get(leaf.suffix)
        if owner is not None:
            _fail_leaves(
                f"leaves {owner!r} and {leaf.name!r} both sanitise to the global suffix "
                f"{leaf.suffix!r}; rx_orders_{leaf.suffix} would name two different tables"
            )
        seen_suffix[leaf.suffix] = leaf.name

    owner_of: Dict[str, str] = {}
    for leaf in leaves:
        for hub in leaf.hubs:
            owner = owner_of.get(hub)
            if owner is not None:
                _fail_leaves(
                    f"hub {hub!r} is assigned to both {owner!r} and {leaf.name!r}; "
                    "two leaves folding one tape make orders_all non-unique on GlobalKey "
                    "and doc 09's linking natural_join fails at runtime"
                )
            owner_of[hub] = leaf.name

    return tuple(leaves)


def unassigned_hubs(topology: Topology, leaves: Sequence[LeafRef]) -> Tuple[str, ...]:
    """Hubs no leaf folds, in topology order (a warning, never an error)."""
    assigned = {hub for leaf in leaves for hub in leaf.hubs}
    return tuple(name for name in topology.names if name not in assigned)


# --------------------------------------------------------------------------------------
# Role settings
# --------------------------------------------------------------------------------------


class LeafSettings:
    """Everything :func:`remote_uri.leaf.build_leaf` needs (doc 10 section 4.2).

    A plain class, not a ``dataclass`` -- see :class:`LeafRef`.
    """

    def __init__(
        self,
        name: str,
        topology: Topology,
        local: Topology,
        amps_uris: Sequence[str],
        bookmark: str = DEFAULT_AMPS_BOOKMARK,
        amps_filter: str = "",
        max_pending: int = DEFAULT_AMPS_MAX_PENDING,
        exec_ring: int = DEFAULT_EXEC_RING,
        stats_period_ms: int = DEFAULT_STATS_PERIOD_MS,
        qty_tol: float = DEFAULT_QTY_TOL,
        notional_tol: float = DEFAULT_NOTIONAL_TOL,
    ) -> None:
        """Store the validated leaf configuration."""
        self.role = ROLE_LEAF
        self.name = name
        self.topology = topology
        self.local = local
        self.amps_uris: Tuple[str, ...] = tuple(amps_uris)
        self.bookmark = bookmark
        self.amps_filter = amps_filter or ""
        self.max_pending = int(max_pending)
        self.exec_ring = int(exec_ring)
        self.stats_period_ms = int(stats_period_ms)
        self.qty_tol = float(qty_tol)
        self.notional_tol = float(notional_tol)

    @property
    def hub_names(self) -> Tuple[str, ...]:
        """The hubs this leaf folds, in topology order."""
        return self.local.names

    def client_name(self, hub_name: str) -> str:
        """AMPS client name for one hub subscription.

        ``dh-<leaf>-<hub>``, lower-cased. AMPS displaces the *older* connection when
        a second one logs on with the same name (doc 10 section 2.6), so the name has
        to be unique per (leaf, hub) across the whole fleet -- which it is, because
        no hub may be assigned to two leaves.
        """
        return f"dh-{str(self.name).lower()}-{str(hub_name).lower()}"

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        ring = "append-only" if self.exec_ring <= 0 else f"ring={self.exec_ring}"
        filtered = f" filter={self.amps_filter!r}" if self.amps_filter else ""
        return (
            f"{self.name} hubs={list(self.hub_names)} amps={','.join(self.amps_uris)} "
            f"bookmark={self.bookmark}{filtered} {ring}"
        )


class CollectorSettings:
    """Everything :func:`remote_uri.collector.build_collector` needs (section 4.3)."""

    def __init__(
        self,
        topology: Topology,
        leaves: Sequence[LeafRef],
        resolver: str = DEFAULT_RESOLVER,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        connect_interval: float = DEFAULT_CONNECT_INTERVAL,
        md_universe: Optional[Mapping[str, float]] = None,
        md_period_ms: int = DEFAULT_MD_PERIOD_MS,
        md_spread_bps: float = DEFAULT_MD_SPREAD_BPS,
        md_seed: int = DEFAULT_MD_SEED,
        qty_tol: float = DEFAULT_QTY_TOL,
        notional_tol: float = DEFAULT_NOTIONAL_TOL,
    ) -> None:
        """Store the validated collector configuration."""
        self.role = ROLE_COLLECTOR
        self.topology = topology
        self.leaves: Tuple[LeafRef, ...] = tuple(leaves)
        self.resolver = resolver
        self.connect_timeout = float(connect_timeout)
        self.connect_interval = float(connect_interval)
        self.md_universe: Dict[str, float] = dict(md_universe or {})
        self.md_period_ms = int(md_period_ms)
        self.md_spread_bps = float(md_spread_bps)
        self.md_seed = int(md_seed)
        self.qty_tol = float(qty_tol)
        self.notional_tol = float(notional_tol)

    @property
    def leaf_names(self) -> Tuple[str, ...]:
        """Leaf names in configuration order."""
        return tuple(leaf.name for leaf in self.leaves)

    def leaf(self, name: Any) -> Optional[LeafRef]:
        """Look a leaf up by name (case-sensitive, as configured)."""
        wanted = "" if name is None else str(name)
        for leaf in self.leaves:
            if leaf.name == wanted:
                return leaf
        return None

    def leaf_of(self, oms: Any) -> Optional[LeafRef]:
        """The leaf that folds hub ``oms``, or ``None`` when no leaf owns it."""
        wanted = "" if oms is None else str(oms)
        for leaf in self.leaves:
            if wanted in leaf.hubs:
                return leaf
        return None

    @property
    def unassigned(self) -> Tuple[str, ...]:
        """Hubs no leaf folds (warned about at startup)."""
        return unassigned_hubs(self.topology, self.leaves)

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        return (
            f"{len(self.leaves)} leaves resolver={self.resolver} "
            f"timeout={self.connect_timeout}s interval={self.connect_interval}s"
        )


def load_leaf_settings(env: Optional[Mapping[str, str]] = None) -> LeafSettings:
    """Read every leaf variable of doc 10 section 4.

    Raises:
        ValueError: On a missing name/hubs, an unknown hub, an empty AMPS URI list
            or an out-of-range numeric knob.
    """
    name = _text(env, LEAF_NAME_ENV)
    if not name:
        raise ValueError(
            f"{LEAF_NAME_ENV} is not set; a leaf must name itself (e.g. 'DH1') -- the "
            "collector attaches that name to this server's rows as the Leaf column"
        )
    topology = load_topology(env)
    hub_names = _split_names(_text(env, LEAF_HUBS_ENV))
    if not hub_names:
        raise ValueError(
            f"{LEAF_HUBS_ENV} is not set; a leaf must name the hubs it folds, "
            f"comma-separated, out of {list(topology.names)}"
        )
    local = restrict_topology(topology, hub_names)

    amps_uris = tuple(_text(env, AMPS_URI_ENV, DEFAULT_AMPS_URI).replace(",", " ").split())
    if not amps_uris:
        raise ValueError(
            f"{AMPS_URI_ENV} is empty; expected e.g. {DEFAULT_AMPS_URI!r} "
            "(comma or space separated for an HA pair)"
        )

    return LeafSettings(
        name=name,
        topology=topology,
        local=local,
        amps_uris=amps_uris,
        bookmark=_text(env, AMPS_BOOKMARK_ENV, DEFAULT_AMPS_BOOKMARK),
        amps_filter=_text(env, AMPS_FILTER_ENV),
        max_pending=int(
            _number(env, AMPS_MAX_PENDING_ENV, DEFAULT_AMPS_MAX_PENDING, minimum=1, integral=True)
        ),
        exec_ring=int(_number(env, EXEC_RING_ENV, DEFAULT_EXEC_RING, minimum=0, integral=True)),
        stats_period_ms=int(
            _number(env, STATS_PERIOD_ENV, DEFAULT_STATS_PERIOD_MS, minimum=1, integral=True)
        ),
        qty_tol=qty_tolerance(env),
        notional_tol=notional_tolerance(env),
    )


def load_collector_settings(env: Optional[Mapping[str, str]] = None) -> CollectorSettings:
    """Read every collector variable of doc 10 section 4.

    Raises:
        ValueError: On an invalid leaves partition (see :func:`parse_leaves`), an
            unknown resolver, a malformed market-data universe or an out-of-range
            numeric knob.
    """
    source = os.environ if env is None else env
    topology = load_topology(env)
    leaves = parse_leaves(source.get(LEAVES_ENV), topology)

    resolver = _text(env, RESOLVER_ENV, DEFAULT_RESOLVER).lower()
    if resolver not in RESOLVERS:
        raise ValueError(
            f"{RESOLVER_ENV}={resolver!r} is unknown; expected one of {list(RESOLVERS)} "
            "('uri' resolves dh+plain://host:port/scope/<name>, 'barrage' subscribes "
            "a barrage_session ticket)"
        )

    universe = marketdata.parse_universe(
        _text(env, MD_SYMBOLS_ENV, DEFAULT_MD_SYMBOLS), env_name=MD_SYMBOLS_ENV
    )
    if not universe:  # pragma: no cover - the default is never empty
        raise ValueError(f"{MD_SYMBOLS_ENV} parsed to an empty universe; expected 'AAPL:190,...'")

    return CollectorSettings(
        topology=topology,
        leaves=leaves,
        resolver=resolver,
        connect_timeout=_number(env, CONNECT_TIMEOUT_ENV, DEFAULT_CONNECT_TIMEOUT, minimum=0.0),
        connect_interval=_number(
            env, CONNECT_INTERVAL_ENV, DEFAULT_CONNECT_INTERVAL, minimum=0.1
        ),
        md_universe=universe,
        md_period_ms=int(
            _number(env, MD_PERIOD_ENV, DEFAULT_MD_PERIOD_MS, minimum=1, integral=True)
        ),
        md_spread_bps=_number(env, MD_SPREAD_ENV, DEFAULT_MD_SPREAD_BPS, minimum=0.0),
        md_seed=int(_number(env, MD_SEED_ENV, DEFAULT_MD_SEED, integral=True)),
        qty_tol=qty_tolerance(env),
        notional_tol=notional_tolerance(env),
    )
