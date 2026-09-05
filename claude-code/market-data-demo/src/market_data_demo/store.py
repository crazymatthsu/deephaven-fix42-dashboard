"""Where the parquet files live: local disk or S3 (doc 11 section 4).

Both stores answer the same three questions -- *which files cover this period and these
symbols*, *which days exist*, *which symbols exist* -- and hand back
:class:`~market_data_demo.layout.ParquetRef` objects whose ``path`` the reader can pass
straight to ``deephaven.parquet.read``: an absolute local path, or an ``s3://`` URI.

Listing is always **per day prefix** (``<root>/YYYY/MM/DD/``): the layout puts the date
first precisely so that a 3-month query lists ~65 small directories instead of scanning
the whole store, on disk and on S3 alike.

The S3 store talks to a boto3-shaped client (``list_objects_v2``, ``put_object``,
``head_bucket``, ``create_bucket``) that is injected, so the unit suite exercises the
pagination and key parsing against an in-memory fake without boto3 installed. The real
client is only built when :meth:`S3Store.client` is first used.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set

from market_data_demo.layout import (
    PARQUET_SUFFIX,
    ParquetRef,
    day_prefix,
    days_in_range,
    normalize_symbol,
    parse_day_prefix,
    parse_relative_path,
)

__all__ = [
    "Store",
    "LocalStore",
    "S3Store",
    "InventorySummary",
    "summarize_inventory",
    "boto3_client_factory",
]


def _symbol_set(symbols: Optional[Iterable[str]]) -> Optional[Set[str]]:
    if symbols is None:
        return None
    wanted = {normalize_symbol(symbol) for symbol in symbols if str(symbol).strip()}
    return wanted


class Store:
    """Interface both stores implement (documented here; python needs no ABC for it)."""

    kind: str = "abstract"

    def describe(self) -> str:  # pragma: no cover - trivial
        """One line for the startup banner."""
        raise NotImplementedError

    def list_files(
        self, start: dt.date, end: dt.date, symbols: Optional[Iterable[str]] = None
    ) -> List[ParquetRef]:
        """Every data file for ``[start, end]`` (and ``symbols`` if given), sorted by day, symbol."""
        raise NotImplementedError

    def available_days(self) -> List[dt.date]:
        """Every ``YYYY/MM/DD`` directory that exists, sorted (whether or not it has files)."""
        raise NotImplementedError

    def available_symbols(
        self, start: Optional[dt.date] = None, end: Optional[dt.date] = None
    ) -> List[str]:
        """Every symbol with at least one file in the period (default: all days), sorted."""
        days = self.available_days()
        if not days:
            return []
        first = start or days[0]
        last = end or days[-1]
        return sorted({ref.symbol for ref in self.list_files(first, last)})


# --------------------------------------------------------------------------------------
# Local disk
# --------------------------------------------------------------------------------------


class LocalStore(Store):
    """``<root>/YYYY/MM/DD/<SYMBOL>.parquet`` on a local (or mounted) filesystem."""

    kind = "local"

    def __init__(self, root: os.PathLike | str) -> None:
        self.root = Path(root)

    def describe(self) -> str:
        return f"local: {self.root}"

    def _day_dir(self, day: dt.date) -> Path:
        return self.root / day_prefix(day)

    def list_files(
        self, start: dt.date, end: dt.date, symbols: Optional[Iterable[str]] = None
    ) -> List[ParquetRef]:
        wanted = _symbol_set(symbols)
        refs: List[ParquetRef] = []
        for day in days_in_range(start, end):
            directory = self._day_dir(day)
            if not directory.is_dir():
                continue
            prefix = day_prefix(day)
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.is_file():
                        ref = parse_relative_path(f"{prefix}/{entry.name}")
                        if ref is not None and (wanted is None or ref.symbol in wanted):
                            refs.append(ParquetRef(ref.day, ref.symbol, str(Path(entry.path).resolve())))
                    elif entry.is_dir():
                        try:
                            symbol = normalize_symbol(entry.name)
                        except ValueError:
                            continue
                        if wanted is not None and symbol not in wanted:
                            continue
                        for part in sorted(os.listdir(entry.path)):
                            ref = parse_relative_path(f"{prefix}/{entry.name}/{part}")
                            if ref is not None:
                                refs.append(
                                    ParquetRef(ref.day, ref.symbol, str((Path(entry.path) / part).resolve()))
                                )
        refs.sort()
        return refs

    def available_days(self) -> List[dt.date]:
        days: List[dt.date] = []
        if not self.root.is_dir():
            return days
        for year in sorted(p for p in self.root.iterdir() if p.is_dir()):
            for month in sorted(p for p in year.iterdir() if p.is_dir()):
                for day_dir in sorted(p for p in month.iterdir() if p.is_dir()):
                    day = parse_day_prefix(f"{year.name}/{month.name}/{day_dir.name}")
                    if day is not None:
                        days.append(day)
        return days


# --------------------------------------------------------------------------------------
# S3 (or anything S3-compatible: MinIO, LocalStack, ...)
# --------------------------------------------------------------------------------------


def boto3_client_factory(
    region: Optional[str] = None,
    endpoint: Optional[str] = None,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    anonymous: bool = False,
    path_style: bool = False,
) -> Callable[[], Any]:
    """Return a zero-arg factory building a boto3 S3 client (imported lazily).

    Credentials are passed only when configured; otherwise boto3's default chain applies
    (env vars, shared config, instance role) -- the same chain the Deephaven side uses
    when :func:`market_data_demo.reader.s3_instructions` gets no explicit keys.
    """

    def factory() -> Any:
        import boto3  # noqa: WPS433 - optional dependency, only for MD_SOURCE=s3
        from botocore import UNSIGNED
        from botocore.config import Config

        options: Dict[str, Any] = {}
        if path_style:
            options["s3"] = {"addressing_style": "path"}
        if anonymous:
            options["signature_version"] = UNSIGNED
        kwargs: Dict[str, Any] = {"config": Config(**options)}
        if region:
            kwargs["region_name"] = region
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        if access_key_id and secret_access_key and not anonymous:
            kwargs["aws_access_key_id"] = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
        return boto3.client("s3", **kwargs)

    return factory


class S3Store(Store):
    """``s3://<bucket>/<prefix>/YYYY/MM/DD/<SYMBOL>.parquet``."""

    kind = "s3"

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        client: Any = None,
        client_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        if not bucket or "/" in bucket:
            raise ValueError(f"invalid S3 bucket name {bucket!r}")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client
        self._client_factory = client_factory

    # -- plumbing ------------------------------------------------------------------

    @property
    def client(self) -> Any:
        """The boto3(-shaped) client, built on first use."""
        if self._client is None:
            if self._client_factory is None:
                self._client_factory = boto3_client_factory()
            self._client = self._client_factory()
        return self._client

    def describe(self) -> str:
        return f"s3: {self.root_uri}"

    @property
    def root_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"

    def key_for(self, rel: str) -> str:
        """Relative layout path -> object key."""
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def uri_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def _relative(self, key: str) -> Optional[str]:
        if not self.prefix:
            return key
        head = self.prefix + "/"
        return key[len(head) :] if key.startswith(head) else None

    def _list_keys(self, prefix: str) -> Iterable[str]:
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []) or []:
                yield item["Key"]
            if not response.get("IsTruncated"):
                return
            token = response.get("NextContinuationToken")
            if not token:
                return

    def _list_common_prefixes(self, prefix: str) -> List[str]:
        found: List[str] = []
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            for item in response.get("CommonPrefixes", []) or []:
                found.append(item["Prefix"])
            if not response.get("IsTruncated"):
                return found
            token = response.get("NextContinuationToken")
            if not token:
                return found

    # -- Store API -----------------------------------------------------------------

    def list_files(
        self, start: dt.date, end: dt.date, symbols: Optional[Iterable[str]] = None
    ) -> List[ParquetRef]:
        wanted = _symbol_set(symbols)
        refs: List[ParquetRef] = []
        for day in days_in_range(start, end):
            for key in self._list_keys(self.key_for(day_prefix(day)) + "/"):
                rel = self._relative(key)
                if rel is None:
                    continue
                ref = parse_relative_path(rel)
                if ref is None or ref.day != day:
                    continue
                if wanted is not None and ref.symbol not in wanted:
                    continue
                refs.append(ParquetRef(ref.day, ref.symbol, self.uri_for(key)))
        refs.sort()
        return refs

    def available_days(self) -> List[dt.date]:
        days: List[dt.date] = []
        base = self.prefix + "/" if self.prefix else ""
        for year in self._list_common_prefixes(base):
            for month in self._list_common_prefixes(year):
                for day_p in self._list_common_prefixes(month):
                    rel = self._relative(day_p.rstrip("/"))
                    day = parse_day_prefix(rel) if rel is not None else None
                    if day is not None:
                        days.append(day)
        days.sort()
        return days

    # -- writing (used by the CLI to seed a bucket from a local tree) ----------------

    def ensure_bucket(self) -> bool:
        """Create the bucket if it does not exist. Returns True if it was created."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return False
        except Exception:  # noqa: BLE001 - any failure -> try to create
            self.client.create_bucket(Bucket=self.bucket)
            return True

    def upload_tree(
        self,
        local: LocalStore,
        start: Optional[dt.date] = None,
        end: Optional[dt.date] = None,
        symbols: Optional[Iterable[str]] = None,
        progress: Optional[Callable[[ParquetRef, str], None]] = None,
    ) -> List[ParquetRef]:
        """Copy a local layout tree into the bucket, key for key.

        Returns the refs (with ``s3://`` URIs) of every object uploaded.
        """
        days = local.available_days()
        if not days:
            return []
        first = start or days[0]
        last = end or days[-1]
        uploaded: List[ParquetRef] = []
        for ref in local.list_files(first, last, symbols):
            rel = os.path.relpath(ref.path, str(local.root.resolve())).replace(os.sep, "/")
            key = self.key_for(rel)
            with open(ref.path, "rb") as handle:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=handle.read())
            out = ParquetRef(ref.day, ref.symbol, self.uri_for(key))
            uploaded.append(out)
            if progress is not None:
                progress(out, key)
        return uploaded


# --------------------------------------------------------------------------------------
# Inventory summaries (pure)
# --------------------------------------------------------------------------------------


class InventorySummary:
    """Per-symbol and per-day roll-ups of a file listing, as plain row dicts."""

    def __init__(self, refs: Sequence[ParquetRef]) -> None:
        self.refs = list(refs)
        by_symbol: Dict[str, List[dt.date]] = {}
        by_day: Dict[dt.date, Set[str]] = {}
        for ref in self.refs:
            by_symbol.setdefault(ref.symbol, []).append(ref.day)
            by_day.setdefault(ref.day, set()).add(ref.symbol)
        self.symbol_rows: List[Dict[str, Any]] = [
            {
                "Symbol": symbol,
                "FirstDay": min(days),
                "LastDay": max(days),
                "Days": len(set(days)),
                "Files": len(days),
            }
            for symbol, days in sorted(by_symbol.items())
        ]
        self.day_rows: List[Dict[str, Any]] = [
            {"Day": day, "Symbols": len(symbols), "SymbolList": ",".join(sorted(symbols))}
            for day, symbols in sorted(by_day.items())
        ]

    @property
    def symbols(self) -> List[str]:
        return [row["Symbol"] for row in self.symbol_rows]

    @property
    def days(self) -> List[dt.date]:
        return [row["Day"] for row in self.day_rows]

    @property
    def first_day(self) -> Optional[dt.date]:
        return self.days[0] if self.days else None

    @property
    def last_day(self) -> Optional[dt.date]:
        return self.days[-1] if self.days else None


def summarize_inventory(refs: Sequence[ParquetRef]) -> InventorySummary:
    """Roll a listing up per symbol and per day."""
    return InventorySummary(refs)
