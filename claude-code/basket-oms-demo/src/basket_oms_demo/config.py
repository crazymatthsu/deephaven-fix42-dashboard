"""``OMS_*`` environment configuration (doc 13 §6). Pure python.

Every variable has a working default, so the app runs with none of them set. A
malformed value is a **startup error** (``ValueError``), never a silent fallback -- the
same rule the other apps in this repo follow.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Mapping, Optional

__all__ = [
    "DEFAULT_VENUES",
    "Config",
    "load_config",
    "parse_bool",
    "parse_int",
    "parse_list",
]

#: Routing targets when ``OMS_VENUES`` is unset (doc 13 §6).
DEFAULT_VENUES = ("NYSE", "NASDAQ", "ARCA", "BATS", "DARK", "ALGO-VWAP", "ALGO-TWAP")

_TRUE = {"1", "true", "yes", "on", "y", "t"}
_FALSE = {"0", "false", "no", "off", "n", "f", ""}


def parse_bool(raw: Optional[str], name: str, default: bool) -> bool:
    """``"true"/"1"/"yes"/"on"`` -> True, ``"false"/"0"/"no"/"off"`` -> False, blank -> default."""
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return default if text == "" else False
    raise ValueError(f"{name}={raw!r}: expected a boolean (true/false/1/0/yes/no/on/off)")


def parse_int(raw: Optional[str], name: str, default: int, minimum: int = 0) -> int:
    """Integer with a lower bound; blank -> default."""
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r}: expected an integer") from exc
    if value < minimum:
        raise ValueError(f"{name}={raw!r}: must be >= {minimum}")
    return value


def parse_list(raw: Optional[str], name: str, default: List[str]) -> List[str]:
    """Comma-separated, upper-cased, de-duplicated; blank -> default."""
    if raw is None or raw.strip() == "":
        return list(default)
    seen: List[str] = []
    for item in raw.split(","):
        token = item.strip().upper()
        if not token:
            continue
        if token not in seen:
            seen.append(token)
    if not seen:
        raise ValueError(f"{name}={raw!r}: expected at least one name")
    return seen


@dataclass
class Config:
    """Resolved configuration; see :func:`load_config` for the variables."""

    seed: int = 42
    mock_baskets: int = 5
    sim_enabled: bool = True
    sim_interval_ms: int = 750
    trader: str = "trader1"
    venues: List[str] = field(default_factory=lambda: list(DEFAULT_VENUES))
    ticket_panel: bool = False


def load_config(env: Optional[Mapping[str, str]] = None) -> Config:
    """Read the ``OMS_*`` variables (doc 13 §6) from ``env`` (default ``os.environ``)."""
    source = os.environ if env is None else env
    get = source.get
    trader = (get("OMS_TRADER") or "trader1").strip() or "trader1"
    return Config(
        seed=parse_int(get("OMS_SEED"), "OMS_SEED", 42),
        mock_baskets=parse_int(get("OMS_MOCK_BASKETS"), "OMS_MOCK_BASKETS", 5),
        sim_enabled=parse_bool(get("OMS_SIM"), "OMS_SIM", True),
        sim_interval_ms=parse_int(get("OMS_SIM_INTERVAL_MS"), "OMS_SIM_INTERVAL_MS", 750, minimum=50),
        trader=trader,
        venues=parse_list(get("OMS_VENUES"), "OMS_VENUES", list(DEFAULT_VENUES)),
        ticket_panel=parse_bool(get("OMS_TICKET_PANEL"), "OMS_TICKET_PANEL", False),
    )
