"""``MD_*`` environment configuration (doc 11 section 5). Pure python.

Every variable has a working default for the local demo, so the app runs with none of
them set. ``MD_SOURCE=s3`` additionally needs ``MD_S3_BUCKET``. A malformed value is a
**startup error** (``ValueError``), never a silent fallback -- the same rule the other
apps in this repo follow.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional

from market_data_demo.layout import parse_symbols

__all__ = [
    "SOURCES",
    "Config",
    "load_config",
    "parse_bool",
    "parse_int",
]

#: Accepted values of ``MD_SOURCE``.
SOURCES = ("local", "s3")

#: Where docker-compose.market-data.yml mounts the data directory in the container.
DEFAULT_LOCAL_ROOT = "/market-data"

_TRUE = {"1", "true", "yes", "on", "y", "t"}
_FALSE = {"0", "false", "no", "off", "n", "f", ""}


def parse_bool(raw: Optional[str], name: str, default: bool) -> bool:
    """``"true"/"1"/"yes"/"on"`` -> True, ``"false"/"0"/"no"/"off"/blank`` -> False."""
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


@dataclass
class Config:
    """Resolved configuration; see :func:`load_config` for the variables."""

    source: str = "local"
    local_root: str = DEFAULT_LOCAL_ROOT
    s3_bucket: str = ""
    s3_prefix: str = ""
    s3_region: str = "us-east-1"
    s3_endpoint: str = ""
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""
    s3_anonymous: bool = False
    s3_path_style: bool = False
    default_symbols: List[str] = field(default_factory=list)
    default_days: int = 5
    default_interval: str = "1m"
    default_chart: str = "candlestick"
    hide_gaps: bool = True
    cache_files: int = 512
    max_files: int = 2000
    read_threads: int = 4

    @property
    def is_s3(self) -> bool:
        return self.source == "s3"

    def describe(self) -> str:
        """Banner text; the secret key is never printed."""
        if self.is_s3:
            creds = (
                "anonymous"
                if self.s3_anonymous
                else ("explicit keys" if self.s3_access_key_id else "default credential chain")
            )
            root = f"s3://{self.s3_bucket}/{self.s3_prefix}" if self.s3_prefix else f"s3://{self.s3_bucket}"
            endpoint = self.s3_endpoint or "(aws)"
            return (
                f"s3 {root} region={self.s3_region} endpoint={endpoint} "
                f"credentials={creds} path_style={self.s3_path_style}"
            )
        return f"local {self.local_root}"


def load_config(env: Optional[Mapping[str, str]] = None) -> Config:
    """Read the ``MD_*`` variables.

    ============================  =====================  ================================
    Variable                      Default                Meaning
    ============================  =====================  ================================
    ``MD_SOURCE``                 ``local``              ``local`` or ``s3``
    ``MD_LOCAL_ROOT``             ``/market-data``       root of the local layout
    ``MD_S3_BUCKET``              (required for s3)      bucket
    ``MD_S3_PREFIX``              ``""``                 key prefix above ``YYYY/MM/DD``
    ``MD_S3_REGION``              ``us-east-1``          region
    ``MD_S3_ENDPOINT``            ``""``                 endpoint override (MinIO, ...)
    ``MD_S3_ACCESS_KEY_ID``       ``""``                 explicit credentials; blank ->
    ``MD_S3_SECRET_ACCESS_KEY``   ``""``                 default AWS credential chain
    ``MD_S3_ANONYMOUS``           ``false``              unsigned requests (public bucket)
    ``MD_S3_PATH_STYLE``          endpoint set?          path-style addressing
    ``MD_DEFAULT_SYMBOLS``        ``""``                 initial selection (blank -> first 3)
    ``MD_DEFAULT_DAYS``           ``5``                  initial period, in available days
    ``MD_DEFAULT_INTERVAL``       ``1m``                 initial bar interval
    ``MD_DEFAULT_CHART``          ``candlestick``        initial chart type
    ``MD_HIDE_GAPS``              ``true``               hide overnight/weekend gaps on x
    ``MD_CACHE_FILES``            ``512``                per-file table cache size
    ``MD_MAX_FILES``              ``2000``               refuse larger single loads
    ``MD_READ_THREADS``           ``4``                  parallel file reads
    ============================  =====================  ================================
    """
    env = os.environ if env is None else env

    source = (env.get("MD_SOURCE") or "local").strip().lower()
    if source not in SOURCES:
        raise ValueError(f"MD_SOURCE={source!r}: expected one of {', '.join(SOURCES)}")

    endpoint = (env.get("MD_S3_ENDPOINT") or "").strip()
    cfg = Config(
        source=source,
        local_root=(env.get("MD_LOCAL_ROOT") or DEFAULT_LOCAL_ROOT).strip() or DEFAULT_LOCAL_ROOT,
        s3_bucket=(env.get("MD_S3_BUCKET") or "").strip(),
        s3_prefix=(env.get("MD_S3_PREFIX") or "").strip().strip("/"),
        s3_region=(env.get("MD_S3_REGION") or "us-east-1").strip() or "us-east-1",
        s3_endpoint=endpoint,
        s3_access_key_id=(env.get("MD_S3_ACCESS_KEY_ID") or "").strip(),
        s3_secret_access_key=(env.get("MD_S3_SECRET_ACCESS_KEY") or "").strip(),
        s3_anonymous=parse_bool(env.get("MD_S3_ANONYMOUS"), "MD_S3_ANONYMOUS", False),
        s3_path_style=parse_bool(env.get("MD_S3_PATH_STYLE"), "MD_S3_PATH_STYLE", bool(endpoint)),
        default_symbols=parse_symbols(env.get("MD_DEFAULT_SYMBOLS")),
        default_days=parse_int(env.get("MD_DEFAULT_DAYS"), "MD_DEFAULT_DAYS", 5, minimum=1),
        default_interval=(env.get("MD_DEFAULT_INTERVAL") or "1m").strip() or "1m",
        default_chart=(env.get("MD_DEFAULT_CHART") or "candlestick").strip().lower() or "candlestick",
        hide_gaps=parse_bool(env.get("MD_HIDE_GAPS"), "MD_HIDE_GAPS", True),
        cache_files=parse_int(env.get("MD_CACHE_FILES"), "MD_CACHE_FILES", 512, minimum=0),
        max_files=parse_int(env.get("MD_MAX_FILES"), "MD_MAX_FILES", 2000, minimum=1),
        read_threads=parse_int(env.get("MD_READ_THREADS"), "MD_READ_THREADS", 4, minimum=1),
    )

    if cfg.is_s3:
        if not cfg.s3_bucket:
            raise ValueError("MD_SOURCE=s3 requires MD_S3_BUCKET")
        if "/" in cfg.s3_bucket:
            raise ValueError(f"MD_S3_BUCKET={cfg.s3_bucket!r}: a bucket name, not a path")
        if bool(cfg.s3_access_key_id) != bool(cfg.s3_secret_access_key):
            raise ValueError("MD_S3_ACCESS_KEY_ID and MD_S3_SECRET_ACCESS_KEY must be set together")
        if cfg.s3_anonymous and cfg.s3_access_key_id:
            raise ValueError("MD_S3_ANONYMOUS=true cannot be combined with explicit access keys")
        if endpoint and not endpoint.lower().startswith(("http://", "https://")):
            raise ValueError(f"MD_S3_ENDPOINT={endpoint!r}: expected an http(s) URL")
    else:
        if not cfg.local_root:
            raise ValueError("MD_LOCAL_ROOT must not be blank")

    from market_data_demo.derived import INTERVALS  # local import: derived is server-side heavy? no -- pure table
    from market_data_demo.charts import CHART_TYPES

    if cfg.default_interval not in INTERVALS:
        raise ValueError(
            f"MD_DEFAULT_INTERVAL={cfg.default_interval!r}: expected one of {', '.join(INTERVALS)}"
        )
    if cfg.default_chart not in CHART_TYPES:
        raise ValueError(
            f"MD_DEFAULT_CHART={cfg.default_chart!r}: expected one of {', '.join(CHART_TYPES)}"
        )
    return cfg


def make_store(cfg: Config) -> Any:
    """Build the :class:`~market_data_demo.store.Store` the configuration names."""
    from market_data_demo.store import LocalStore, S3Store, boto3_client_factory

    if cfg.is_s3:
        return S3Store(
            cfg.s3_bucket,
            cfg.s3_prefix,
            client_factory=boto3_client_factory(
                region=cfg.s3_region,
                endpoint=cfg.s3_endpoint or None,
                access_key_id=cfg.s3_access_key_id or None,
                secret_access_key=cfg.s3_secret_access_key or None,
                anonymous=cfg.s3_anonymous,
                path_style=cfg.s3_path_style,
            ),
        )
    return LocalStore(cfg.local_root)
