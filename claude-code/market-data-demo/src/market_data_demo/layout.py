"""The parquet directory layout and the date helpers built on it (pure python).

Layout contract (doc 11 section 2)::

    <root>/YYYY/MM/DD/<SYMBOL>.parquet            one file per symbol per trading day
    <root>/YYYY/MM/DD/<SYMBOL>/<anything>.parquet  also accepted: a directory per symbol

``<root>`` is a local directory or an ``s3://bucket/prefix``. The mock generator writes
the first shape; the reader accepts both, so data produced by another writer (a Spark
job emitting ``part-*.parquet`` under a symbol directory, say) is picked up unchanged.

Nothing here imports deephaven or pyarrow: every function is unit-tested on a bare host
python.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Union

__all__ = [
    "PARQUET_SUFFIX",
    "ParquetRef",
    "normalize_symbol",
    "parse_symbols",
    "day_prefix",
    "relative_path",
    "parse_day_prefix",
    "parse_relative_path",
    "days_in_range",
    "trading_days",
    "is_trading_day",
    "to_date",
]

#: File extension every data file must carry (case-insensitive).
PARQUET_SUFFIX = ".parquet"

_SYMBOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]{0,31}$")
_DAY_PREFIX_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")
_LEADING_DATE_RE = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})")


@dataclass(frozen=True, order=True)
class ParquetRef:
    """One data file: which trading day and symbol it holds, and where it lives.

    ``path`` is whatever the owning store hands to the reader -- an absolute local path
    or an ``s3://bucket/key`` URI -- so the reader never has to know which store it came
    from.
    """

    day: dt.date
    symbol: str
    path: str


def normalize_symbol(raw: Any) -> str:
    """Upper-case and validate a ticker.

    Raises:
        ValueError: If the symbol is empty or carries characters that cannot appear in a
            path component of the layout (spaces, slashes, quotes, ...).
    """
    text = str(raw or "").strip().upper()
    if not _SYMBOL_RE.match(text):
        raise ValueError(f"invalid symbol {raw!r}: expected 1-32 of [A-Za-z0-9._-]")
    return text


def parse_symbols(raw: Union[None, str, Iterable[Any]]) -> List[str]:
    """Parse ``"aapl, msft;NVDA"`` (or any iterable of tickers) into a de-duplicated list.

    Order is preserved; blanks are dropped; ``None`` yields ``[]``.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        parts: Iterable[Any] = re.split(r"[,;\s]+", raw)
    else:
        parts = raw
    seen: List[str] = []
    for part in parts:
        if part is None or str(part).strip() == "":
            continue
        symbol = normalize_symbol(part)
        if symbol not in seen:
            seen.append(symbol)
    return seen


def day_prefix(day: dt.date) -> str:
    """``date(2026, 9, 4)`` -> ``"2026/09/04"``."""
    return f"{day.year:04d}/{day.month:02d}/{day.day:02d}"


def relative_path(day: dt.date, symbol: str) -> str:
    """The canonical file for one symbol-day: ``"2026/09/04/AAPL.parquet"``."""
    return f"{day_prefix(day)}/{normalize_symbol(symbol)}{PARQUET_SUFFIX}"


def parse_day_prefix(text: str) -> Optional[dt.date]:
    """``"2026/09/04"`` -> ``date(2026, 9, 4)``; ``None`` for anything else (incl. 2026/13/01)."""
    match = _DAY_PREFIX_RE.match(text.strip("/"))
    if not match:
        return None
    try:
        return dt.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def parse_relative_path(rel: str) -> Optional[ParquetRef]:
    """Recognize a layout-conforming relative path.

    Accepts ``YYYY/MM/DD/SYMBOL.parquet`` and ``YYYY/MM/DD/SYMBOL/<file>.parquet``;
    returns ``None`` for anything else (``_metadata`` sidecars, ``.crc`` files, a stray
    ``README``...), so a store can list a directory and simply skip the non-matches.
    The returned ``path`` is ``rel`` itself; stores replace it with an absolute location.
    """
    parts = [part for part in rel.replace("\\", "/").split("/") if part]
    if len(parts) not in (4, 5):
        return None
    day = parse_day_prefix("/".join(parts[:3]))
    if day is None:
        return None
    if len(parts) == 4:
        name = parts[3]
        if not name.lower().endswith(PARQUET_SUFFIX) or len(name) == len(PARQUET_SUFFIX):
            return None
        symbol_text = name[: -len(PARQUET_SUFFIX)]
    else:
        symbol_text, name = parts[3], parts[4]
        if not name.lower().endswith(PARQUET_SUFFIX) or name.startswith((".", "_")):
            return None
    try:
        symbol = normalize_symbol(symbol_text)
    except ValueError:
        return None
    return ParquetRef(day=day, symbol=symbol, path=rel)


def days_in_range(start: dt.date, end: dt.date) -> List[dt.date]:
    """Every calendar day from ``start`` to ``end`` inclusive (``[]`` if end < start)."""
    if end < start:
        return []
    return [start + dt.timedelta(days=offset) for offset in range((end - start).days + 1)]


def is_trading_day(day: dt.date) -> bool:
    """Monday-Friday. Exchange holidays are deliberately not modelled (doc 11 section 3)."""
    return day.weekday() < 5


def trading_days(start: dt.date, end: dt.date) -> List[dt.date]:
    """The weekdays from ``start`` to ``end`` inclusive."""
    return [day for day in days_in_range(start, end) if is_trading_day(day)]


def to_date(value: Any) -> Optional[dt.date]:
    """Coerce whatever a UI control or a caller hands over into a ``date``.

    Accepts ``date``/``datetime``, ISO strings (``"2026-09-04"``,
    ``"2026-09-04T13:30:00Z"``), and any object whose ``str()`` starts with an ISO date
    -- which covers the Java ``LocalDate`` / ``Instant`` / ``ZonedDateTime`` values
    ``deephaven.ui`` date pickers deliver. ``None`` and blanks yield ``None``.
    """
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value)
    match = _LEADING_DATE_RE.match(text)
    if not match:
        return None
    try:
        return dt.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def clamp_range(
    start: Optional[dt.date], end: Optional[dt.date], available: Sequence[dt.date]
) -> Optional[tuple]:
    """Fill a half-open ``(start, end)`` from the available days and order it.

    Returns ``None`` when nothing is available and neither bound was given.
    """
    if start is None and end is None:
        if not available:
            return None
        return (min(available), max(available))
    if start is None:
        start = min(available) if available else end
    if end is None:
        end = max(available) if available else start
    assert start is not None and end is not None
    if end < start:
        start, end = end, start
    return (start, end)
