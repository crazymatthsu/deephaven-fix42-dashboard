"""Level topology, break taxonomy and ``OTR_*`` settings -- doc 14 sections 3 and 5.

Pure stdlib on purpose: every rule here is unit-tested on a bare host python, and the
same constants drive both the Deephaven DAG (:mod:`order_tree_recon.dag`) and its python
reference (:mod:`order_tree_recon.reference`), so the two cannot drift apart silently.

A **level** is one kind of order in the flow -- a Raptor client order, an algo parent,
an algo child, a router parent, a router child on a venue. Several levels can live in
one **system** (one id namespace): the algo server's parents and children share the
``ALGO`` namespace, which is why an algo child's ``ParentAlgoOrderId`` and a router
parent's ``UpstreamOrderId`` can both be resolved through one ``(System, Id)`` index.

A level names the systems its parent may live in, **in preference order**. That list is
what handles flow that skips a hop: a DMA order goes Raptor -> router with no algo in
between, so ``OR_PARENT`` lists ``ALGO`` first and ``RAPTOR`` second.

Environment (all optional, read once at app start; a malformed value is a startup
error, never a silent fallback -- the same rule every app in this repo follows):

========================  ========  =================================================
Variable                  Default   Meaning
========================  ========  =================================================
``OTR_QTY_TOL``           ``1e-6``  absolute tolerance for every quantity comparison
``OTR_SEED``              ``42``    mock data seed
``OTR_MOCK_FAMILIES``     ``24``    random clean/working/DMA families on top of the
                                    fixed scenario catalog
========================  ========  =================================================
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

__all__ = [
    "QTY_TOL_ENV",
    "SEED_ENV",
    "MOCK_FAMILIES_ENV",
    "DEFAULT_QTY_TOL",
    "DEFAULT_SEED",
    "DEFAULT_MOCK_FAMILIES",
    "DEFAULT_LEVELS",
    "CANONICAL_COLUMNS",
    "STRING_COLUMNS",
    "QTY_COLUMNS",
    "LINK_STATES",
    "BREAK_KINDS",
    "RED_BREAK_KINDS",
    "AMBER_BREAK_KINDS",
    "EDGE_BREAK_KINDS",
    "LevelSpec",
    "Topology",
    "parse_topology",
    "default_topology",
    "qty_tolerance",
    "seed",
    "mock_families",
]

QTY_TOL_ENV = "OTR_QTY_TOL"
SEED_ENV = "OTR_SEED"
MOCK_FAMILIES_ENV = "OTR_MOCK_FAMILIES"

#: Shares are integral, so anything below one share is "equal"; 1e-6 matches doc 09.
DEFAULT_QTY_TOL = 1e-6
DEFAULT_SEED = 42
DEFAULT_MOCK_FAMILIES = 24

#: Raptor -> algo (parent, children) -> order router (parent, children on venues).
#: ``street`` marks the level whose orders are live on an exchange: its open quantity
#: is the market exposure, and it is never expected to have children.
DEFAULT_LEVELS: Tuple[Dict[str, Any], ...] = (
    {"name": "CLIENT", "system": "RAPTOR"},
    {"name": "ALGO_PARENT", "system": "ALGO", "parents": ["RAPTOR"]},
    {"name": "ALGO_CHILD", "system": "ALGO", "parents": ["ALGO"]},
    {"name": "OR_PARENT", "system": "OR", "parents": ["ALGO", "RAPTOR"]},
    {"name": "OR_CHILD", "system": "OR", "parents": ["OR"], "street": True},
)

#: The adapter contract: every level table handed to the DAG must carry exactly these
#: columns (extra columns are dropped). Ids are strings; ``LinkId`` is the id the order
#: carries for its parent -- the external order id for a cross-system hop, the parent
#: order id inside one system, ``""`` at a root level. ``Moniker`` only has to be
#: populated at the root level: every descendant inherits the root's.
CANONICAL_COLUMNS: Tuple[str, ...] = (
    "OrderId",
    "AltId",
    "LinkId",
    "Moniker",
    "Symbol",
    "Side",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "Status",
    "Destination",
)
#: Canonical string columns: a null is normalized to ``""``.
STRING_COLUMNS: Tuple[str, ...] = (
    "OrderId",
    "AltId",
    "LinkId",
    "Moniker",
    "Symbol",
    "Side",
    "Status",
    "Destination",
)
#: Canonical quantity columns: a null is normalized to ``0.0`` so no sum is poisoned.
QTY_COLUMNS: Tuple[str, ...] = ("OrderQty", "CumQty", "LeavesQty")

#: ``ROOT`` (a root-level order), ``LINKED``, ``ORPHAN`` (carries a link id that resolves
#: to nothing) and ``NO_LINK`` (a non-root order carrying no link id at all).
LINK_STATES: Tuple[str, ...] = ("ROOT", "LINKED", "ORPHAN", "NO_LINK")

#: The doc 14 section 5 taxonomy, in **priority order** -- a node shows the first that
#: applies, so a linking problem is never masked by the quantity break it causes.
BREAK_KINDS: Tuple[str, ...] = (
    "ORPHAN",
    "NO_LINK",
    "TOO_DEEP",
    "MISMATCH",
    "OVERFILL",
    "UNBOOKED_FILL",
    "PHANTOM_FILL",
    "CHILD_STILL_OPEN",
    "OVER_ROUTED",
    "HELD",
    "NONE",
)
#: Painted red: each one means the firm's shares or open orders disagree with the client's.
RED_BREAK_KINDS: Tuple[str, ...] = BREAK_KINDS[:9]
#: Painted amber: open here, not (yet) routed further down -- normal for an algo
#: schedule, worth a look anywhere else.
AMBER_BREAK_KINDS: Tuple[str, ...] = ("HELD",)
#: Kinds computed on a parent *against its children*: the children are flagged
#: ``OnBrokenEdge`` so both ends of the discrepancy light up.
EDGE_BREAK_KINDS: Tuple[str, ...] = (
    "UNBOOKED_FILL",
    "PHANTOM_FILL",
    "CHILD_STILL_OPEN",
    "OVER_ROUTED",
)

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_LEVEL_KEYS = frozenset({"name", "system", "parents", "street"})


@dataclass(frozen=True)
class LevelSpec:
    """One level of the order flow.

    Attributes:
        name: Level name, e.g. ``ALGO_CHILD`` (also the ``Level`` column value).
        system: The id namespace its ``OrderId`` lives in, e.g. ``ALGO``.
        parents: Systems its ``LinkId`` may resolve in, first match wins; empty for a
            root level.
        street: Orders at this level are live on a venue (the terminal level).
        level_no: Position in the topology, ``0`` for the first root.
    """

    name: str
    system: str
    parents: Tuple[str, ...] = ()
    street: bool = False
    level_no: int = 0

    @property
    def is_root(self) -> bool:
        return not self.parents


class Topology:
    """A validated, ordered tuple of :class:`LevelSpec`.

    The order is the display order (``LevelNo``); actual tree depth is data-driven, so
    a DMA tree is shallower than an algo tree without any configuration.
    """

    def __init__(self, levels: Sequence[LevelSpec], max_depth: Optional[int] = None) -> None:
        self.levels: Tuple[LevelSpec, ...] = tuple(levels)
        #: Root-walk iterations. Defaults to the number of levels, one more than the
        #: longest chain that visits each level once, so a nested algo child still
        #: resolves; anything deeper is flagged ``TOO_DEEP`` rather than mis-rooted.
        self.max_depth: int = len(self.levels) if max_depth is None else int(max_depth)

    def __iter__(self):
        return iter(self.levels)

    def __len__(self) -> int:
        return len(self.levels)

    def __getitem__(self, name: str) -> LevelSpec:
        for level in self.levels:
            if level.name == name:
                return level
        raise KeyError(name)

    @property
    def names(self) -> Tuple[str, ...]:
        return tuple(level.name for level in self.levels)

    @property
    def systems(self) -> Tuple[str, ...]:
        """Distinct systems, in first-appearance order."""
        seen: List[str] = []
        for level in self.levels:
            if level.system not in seen:
                seen.append(level.system)
        return tuple(seen)

    @property
    def max_parents(self) -> int:
        """The longest candidate list -- the number of ``natural_join`` probes needed."""
        return max((len(level.parents) for level in self.levels), default=0)

    @property
    def street_levels(self) -> Tuple[str, ...]:
        return tuple(level.name for level in self.levels if level.street)

    @property
    def root_levels(self) -> Tuple[str, ...]:
        return tuple(level.name for level in self.levels if level.is_root)

    def describe(self) -> List[str]:
        """One human-readable line per level, for the startup banner."""
        lines = []
        for level in self.levels:
            parents = " | ".join(level.parents) if level.parents else "(root)"
            street = "  [street]" if level.street else ""
            lines.append(f"L{level.level_no} {level.name:<12} system={level.system:<7} parent in: {parents}{street}")
        return lines


def parse_topology(levels: Iterable[Mapping[str, Any]], max_depth: Optional[int] = None) -> Topology:
    """Validate a level list (the shape of :data:`DEFAULT_LEVELS`).

    Rules: at least one level; names unique identifiers; every ``parents`` entry names a
    system some level declares; at least one root level; ``street`` a boolean; a root
    level cannot be a street level (the exposure identity needs a client order above the
    venue orders); ``max_depth`` at least ``len(levels) - 1``.

    Raises:
        ValueError: with a message naming the offending level and field.
    """
    raw = list(levels)
    if not raw:
        raise ValueError("topology: at least one level is required")

    specs: List[LevelSpec] = []
    names: List[str] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValueError(f"topology[{index}]: expected an object, got {type(item).__name__}")
        unknown = set(item) - _LEVEL_KEYS
        if unknown:
            raise ValueError(f"topology[{index}]: unknown key(s) {sorted(unknown)}")
        name = item.get("name")
        system = item.get("system")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise ValueError(f"topology[{index}]: 'name' must be an identifier, got {name!r}")
        if name in names:
            raise ValueError(f"topology: duplicate level name {name!r}")
        if not isinstance(system, str) or not _NAME_RE.match(system):
            raise ValueError(f"topology[{name}]: 'system' must be an identifier, got {system!r}")
        parents = item.get("parents", [])
        if isinstance(parents, str) or not isinstance(parents, (list, tuple)):
            raise ValueError(f"topology[{name}]: 'parents' must be a list of system names")
        if len(set(parents)) != len(parents):
            raise ValueError(f"topology[{name}]: 'parents' lists a system twice")
        street = item.get("street", False)
        if not isinstance(street, bool):
            raise ValueError(f"topology[{name}]: 'street' must be true/false, got {street!r}")
        if street and not parents:
            raise ValueError(f"topology[{name}]: a root level cannot be a street level")
        names.append(name)
        specs.append(LevelSpec(name, system, tuple(parents), street, index))

    systems = {spec.system for spec in specs}
    for spec in specs:
        for parent in spec.parents:
            if not isinstance(parent, str) or parent not in systems:
                raise ValueError(
                    f"topology[{spec.name}]: parent system {parent!r} is not declared by any level "
                    f"(known: {sorted(systems)})"
                )
    if not any(spec.is_root for spec in specs):
        raise ValueError("topology: at least one root level (no 'parents') is required")
    if max_depth is not None and int(max_depth) < len(specs) - 1:
        raise ValueError(f"topology: max_depth {max_depth} is shorter than {len(specs) - 1} levels")
    return Topology(specs, max_depth)


def default_topology() -> Topology:
    """The Raptor -> algo -> router topology of :data:`DEFAULT_LEVELS`."""
    return parse_topology(DEFAULT_LEVELS)


def _env(environ: Optional[Mapping[str, str]]) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def qty_tolerance(environ: Optional[Mapping[str, str]] = None) -> float:
    """``OTR_QTY_TOL`` as a finite, non-negative float."""
    raw = _env(environ).get(QTY_TOL_ENV, "").strip()
    if not raw:
        return DEFAULT_QTY_TOL
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{QTY_TOL_ENV}={raw!r}: expected a number") from None
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{QTY_TOL_ENV}={raw!r}: expected a finite number >= 0")
    return value


def _int_env(name: str, default: int, minimum: int, environ: Optional[Mapping[str, str]]) -> int:
    raw = _env(environ).get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r}: expected an integer") from None
    if value < minimum:
        raise ValueError(f"{name}={raw!r}: expected an integer >= {minimum}")
    return value


def seed(environ: Optional[Mapping[str, str]] = None) -> int:
    """``OTR_SEED`` -- any non-negative integer."""
    return _int_env(SEED_ENV, DEFAULT_SEED, 0, environ)


def mock_families(environ: Optional[Mapping[str, str]] = None) -> int:
    """``OTR_MOCK_FAMILIES`` -- random families added on top of the scenario catalog."""
    return _int_env(MOCK_FAMILIES_ENV, DEFAULT_MOCK_FAMILIES, 0, environ)
