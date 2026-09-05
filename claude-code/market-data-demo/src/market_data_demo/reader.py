"""Parquet files -> Deephaven tables (doc 11 section 6). Server side only.

:class:`BarReader` turns a store listing into one static table: each file is read with
``deephaven.parquet.read`` (S3 files through ``S3Instructions``, i.e. Deephaven's own
S3 channel provider -- boto3 is used for *listing* only), the per-file tables are
``merge``d and sorted. Files are immutable, so per-file tables are cached (LRU, size
``MD_CACHE_FILES``): re-selecting a period the UI already showed costs nothing.

Every public method returns a :class:`LoadResult` and never raises -- the dashboard
renders the message instead of dying on a bad file.

**Execution context.** Deephaven table operations need the engine's ``ExecutionContext``
registered on the calling thread. The app-mode script thread and ``deephaven.ui``'s render
threads have it; the reader's own worker threads (and any plain python thread) do not. So
the reader captures the context it was constructed under and re-applies it (``with ctx:``)
around every engine call -- the same pattern ``deephaven.ui``'s hooks use for their
background threads.
"""

from __future__ import annotations

import datetime as dt
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from market_data_demo.config import Config
from market_data_demo.derived import BAR_COLUMNS, empty_bars
from market_data_demo.layout import ParquetRef, normalize_symbol
from market_data_demo.store import Store

__all__ = ["LoadResult", "BarReader", "s3_instructions"]


@dataclass
class LoadResult:
    """The outcome of one load: the table plus what went into it."""

    table: Any
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None
    symbols: List[str] = field(default_factory=list)
    files: List[ParquetRef] = field(default_factory=list)
    missing_symbols: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    cached_files: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def status(self) -> str:
        """One line for the UI: what was loaded and any problems."""
        period = f"{self.start} .. {self.end}" if self.start and self.end else "(no period)"
        parts = [f"{len(self.files)} file(s) for {period}", f"symbols: {', '.join(self.symbols) or '-'}"]
        if self.missing_symbols:
            parts.append(f"no data for: {', '.join(self.missing_symbols)}")
        if self.cached_files:
            parts.append(f"{self.cached_files} from cache")
        if self.errors:
            parts.append("ERROR: " + " | ".join(self.errors))
        return "; ".join(parts)


class _NoContext:
    """Stand-in when the engine is not importable (unit tests on the host)."""

    def __enter__(self) -> "_NoContext":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def _capture_exec_ctx() -> Any:
    """The calling thread's ``ExecutionContext`` (a no-op stand-in without deephaven)."""
    try:
        from deephaven.execution_context import get_exec_ctx

        return get_exec_ctx()
    except Exception:  # noqa: BLE001 - host python, or a thread without a context
        return _NoContext()


def s3_instructions(cfg: Config) -> Any:
    """Build ``deephaven.experimental.s3.S3Instructions`` from the configuration.

    Explicit keys are passed only when configured (otherwise the AWS default credential
    chain applies); an endpoint override switches the client to MinIO/LocalStack.
    """
    from deephaven.experimental import s3

    kwargs: Dict[str, Any] = {"region_name": cfg.s3_region}
    if cfg.s3_endpoint:
        kwargs["endpoint_override"] = cfg.s3_endpoint
    if cfg.s3_anonymous:
        kwargs["anonymous_access"] = True
    elif cfg.s3_access_key_id and cfg.s3_secret_access_key:
        kwargs["access_key_id"] = cfg.s3_access_key_id
        kwargs["secret_access_key"] = cfg.s3_secret_access_key
    return s3.S3Instructions(**kwargs)


class BarReader:
    """Reads the bars for a period and a symbol list out of a :class:`Store`."""

    def __init__(self, store: Store, cfg: Config) -> None:
        self.store = store
        self.cfg = cfg
        self._s3 = s3_instructions(cfg) if cfg.is_s3 else None
        self._cache: "OrderedDict[str, Any]" = OrderedDict()
        self._lock = threading.Lock()
        self._exec_ctx = _capture_exec_ctx()

    # -- one file ------------------------------------------------------------------

    def _read_one(self, ref: ParquetRef) -> Any:
        from deephaven import parquet

        with self._exec_ctx:
            table = parquet.read(ref.path, special_instructions=self._s3)
            return self._conform(table, ref)

    @staticmethod
    def _conform(table: Any, ref: ParquetRef) -> Any:
        """Force the canonical columns: add a path-derived ``Symbol`` and null optionals."""
        names = set(table.column_names)
        fixes: List[str] = []
        if "Symbol" not in names:
            fixes.append(f"Symbol = `{ref.symbol}`")
        if "VWAP" not in names:
            fixes.append("VWAP = NULL_DOUBLE")
        if "TradeCount" not in names:
            fixes.append("TradeCount = NULL_LONG")
        if fixes:
            table = table.update_view(fixes)
        missing = [column for column in BAR_COLUMNS if column not in set(table.column_names)]
        if missing:
            raise ValueError(f"{ref.path}: missing column(s) {', '.join(missing)}")
        return table.view(list(BAR_COLUMNS))

    def read_file(self, ref: ParquetRef) -> Any:
        """The table of one file (cached by path)."""
        with self._lock:
            cached = self._cache.get(ref.path)
            if cached is not None:
                self._cache.move_to_end(ref.path)
                return cached
        table = self._read_one(ref)
        if self.cfg.cache_files > 0:
            with self._lock:
                self._cache[ref.path] = table
                self._cache.move_to_end(ref.path)
                while len(self._cache) > self.cfg.cache_files:
                    self._cache.popitem(last=False)
        return table

    def cached_paths(self) -> List[str]:
        with self._lock:
            return list(self._cache)

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    # -- a period ------------------------------------------------------------------

    def read(
        self,
        start: dt.date,
        end: dt.date,
        symbols: Optional[Iterable[str]] = None,
    ) -> LoadResult:
        """Load every file for ``[start, end]`` and ``symbols`` into one sorted table.

        ``symbols=None`` loads every symbol present. Never raises: problems land in
        :attr:`LoadResult.errors` and the table is whatever could be read (or empty).
        """
        from deephaven import merge

        wanted: Optional[List[str]] = None
        if symbols is not None:
            wanted = []
            for symbol in symbols:
                try:
                    name = normalize_symbol(symbol)
                except ValueError:
                    continue
                if name not in wanted:
                    wanted.append(name)
        result = LoadResult(table=empty_bars(), start=start, end=end, symbols=list(wanted or []))
        if end < start:
            start, end = end, start
            result.start, result.end = start, end
        if wanted is not None and not wanted:
            return result

        try:
            refs = self.store.list_files(start, end, wanted)
        except Exception as exc:  # noqa: BLE001 - listing failure is a user-visible error
            result.errors.append(f"listing failed: {type(exc).__name__}: {exc}")
            return result
        result.files = refs
        found = {ref.symbol for ref in refs}
        if wanted is None:
            result.symbols = sorted(found)
        else:
            result.missing_symbols = [name for name in wanted if name not in found]
        if len(refs) > self.cfg.max_files:
            result.errors.append(
                f"{len(refs)} files exceed MD_MAX_FILES={self.cfg.max_files}; narrow the period or symbols"
            )
            result.files = []
            return result
        if not refs:
            return result

        before = set(self.cached_paths())
        result.cached_files = sum(1 for ref in refs if ref.path in before)
        tables: List[Any] = []
        failures: List[str] = []

        def load(ref: ParquetRef) -> Optional[Any]:
            try:
                return self.read_file(ref)
            except Exception as exc:  # noqa: BLE001 - one bad file must not kill the load
                failures.append(f"{ref.path}: {type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}")
                return None

        workers = max(1, min(self.cfg.read_threads, len(refs)))
        if workers == 1:
            loaded = [load(ref) for ref in refs]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="md-read") as pool:
                loaded = list(pool.map(load, refs))
        tables = [table for table in loaded if table is not None]
        result.errors.extend(failures[:5])
        if len(failures) > 5:
            result.errors.append(f"... and {len(failures) - 5} more file(s) failed")
        if tables:
            with self._exec_ctx:
                merged = tables[0] if len(tables) == 1 else merge(tables)
                result.table = merged.sort(["Symbol", "Timestamp"])
        return result
