"""``python -m market_data_demo`` -- generate, upload and list mock market data (doc 11 section 3).

::

    python -m market_data_demo generate --root ./data --symbols AAPL,MSFT --start 2026-08-03 --end 2026-09-04
    python -m market_data_demo upload   --root ./data --bucket market-data --prefix ohlc --endpoint http://localhost:9000
    python -m market_data_demo list     --root ./data
    python -m market_data_demo list     --bucket market-data --prefix ohlc --endpoint http://localhost:9000

``generate`` needs pyarrow; ``upload`` and an S3 ``list`` additionally need boto3
(``pip install -e ".[s3]"``). Credentials for S3 come from ``--access-key/--secret-key``,
else ``MD_S3_ACCESS_KEY_ID/MD_S3_SECRET_ACCESS_KEY``, else boto3's default chain.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence

from market_data_demo.layout import parse_symbols, to_date
from market_data_demo.store import LocalStore, S3Store, boto3_client_factory, summarize_inventory

__all__ = ["main", "build_parser", "default_root"]


def default_root() -> str:
    """``MD_LOCAL_ROOT`` if set, else ``<module>/data`` (the compose file's bind mount)."""
    configured = os.environ.get("MD_LOCAL_ROOT")
    if configured:
        return configured
    return str(Path(__file__).resolve().parents[2] / "data")


def _date(text: str) -> dt.date:
    value = to_date(text)
    if value is None:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {text!r}")
    return value


def _add_s3_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bucket", default=os.environ.get("MD_S3_BUCKET"), help="S3 bucket (env MD_S3_BUCKET)")
    parser.add_argument("--prefix", default=os.environ.get("MD_S3_PREFIX", ""), help="key prefix above YYYY/MM/DD (env MD_S3_PREFIX)")
    parser.add_argument("--region", default=os.environ.get("MD_S3_REGION", "us-east-1"))
    parser.add_argument("--endpoint", default=os.environ.get("MD_S3_ENDPOINT"), help="endpoint override, e.g. http://localhost:9000 for MinIO")
    parser.add_argument("--access-key", default=os.environ.get("MD_S3_ACCESS_KEY_ID"))
    parser.add_argument("--secret-key", default=os.environ.get("MD_S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--anonymous", action="store_true", help="unsigned requests (public bucket)")
    parser.add_argument("--virtual-host", action="store_true", help="virtual-host addressing (default: path style when --endpoint is set)")


def _s3_store(args: argparse.Namespace) -> S3Store:
    if not args.bucket:
        raise SystemExit("error: --bucket (or MD_S3_BUCKET) is required for S3")
    factory = boto3_client_factory(
        region=args.region,
        endpoint=args.endpoint,
        access_key_id=args.access_key,
        secret_access_key=args.secret_key,
        anonymous=args.anonymous,
        path_style=bool(args.endpoint) and not args.virtual_host,
    )
    return S3Store(args.bucket, args.prefix or "", client_factory=factory)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m market_data_demo", description=__doc__.split("::")[0].strip())
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write mock 1-minute OHLC parquet files under <root>/YYYY/MM/DD/<SYMBOL>.parquet")
    gen.add_argument("--root", default=default_root(), help="local data root (default: %(default)s)")
    gen.add_argument("--symbols", default="", help="comma list; default: the 8-symbol demo universe")
    gen.add_argument("--start", type=_date, default=None, help="first day (default: 30 days before --end)")
    gen.add_argument("--end", type=_date, default=None, help="last day (default: yesterday)")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--force", action="store_true", help="rewrite files that already exist")
    gen.add_argument("--quiet", action="store_true")

    up = sub.add_parser("upload", help="copy a local tree into an S3 bucket, key for key (creates the bucket if missing)")
    up.add_argument("--root", default=default_root())
    up.add_argument("--symbols", default="")
    up.add_argument("--start", type=_date, default=None)
    up.add_argument("--end", type=_date, default=None)
    up.add_argument("--quiet", action="store_true")
    _add_s3_args(up)

    ls = sub.add_parser("list", help="show the inventory of a local root or an S3 bucket")
    ls.add_argument("--root", default=None, help="local data root (default when no --bucket is given)")
    ls.add_argument("--symbols", default="")
    ls.add_argument("--start", type=_date, default=None)
    ls.add_argument("--end", type=_date, default=None)
    ls.add_argument("--files", action="store_true", help="list every file, not just the summary")
    _add_s3_args(ls)
    return parser


def _print_inventory(store, args: argparse.Namespace) -> int:
    days = store.available_days()
    if not days:
        print(f"{store.describe()}: no data")
        return 1
    first = args.start or days[0]
    last = args.end or days[-1]
    refs = store.list_files(first, last, parse_symbols(args.symbols) or None)
    inv = summarize_inventory(refs)
    print(f"{store.describe()}")
    print(f"  period  : {first} .. {last}  ({len(inv.days)} day(s) with data, {len(refs)} file(s))")
    print(f"  symbols : {len(inv.symbols)}")
    for row in inv.symbol_rows:
        print(f"    {row['Symbol']:<8} {row['FirstDay']} .. {row['LastDay']}  days={row['Days']} files={row['Files']}")
    if args.files:
        for ref in refs:
            print(f"    {ref.day} {ref.symbol:<8} {ref.path}")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "generate":
        from market_data_demo.mockgen import generate

        def progress(ref, written: bool) -> None:
            if not args.quiet:
                print(("wrote   " if written else "skipped ") + ref.path)

        report = generate(
            args.root,
            symbols=args.symbols or None,
            start=args.start,
            end=args.end,
            seed=args.seed,
            force=args.force,
            progress=progress,
        )
        print(
            f"generated {len(report.written)} file(s), skipped {len(report.skipped)} existing, "
            f"{len(report.days)} trading day(s) x {len(report.symbols)} symbol(s) under {report.root}"
        )
        if report.days:
            print(f"period {report.days[0]} .. {report.days[-1]}; symbols {', '.join(report.symbols)}")
        return 0

    if args.command == "upload":
        store = _s3_store(args)
        local = LocalStore(args.root)
        created = store.ensure_bucket()
        if created:
            print(f"created bucket s3://{store.bucket}")

        def on_upload(ref, key: str) -> None:
            if not args.quiet:
                print("uploaded " + ref.path)

        uploaded = store.upload_tree(local, args.start, args.end, parse_symbols(args.symbols) or None, progress=on_upload)
        print(f"uploaded {len(uploaded)} file(s) from {local.root} to {store.root_uri}")
        return 0 if uploaded else 1

    if args.command == "list":
        if args.bucket:
            store = _s3_store(args)
        else:
            store = LocalStore(args.root or default_root())
        return _print_inventory(store, args)

    return 2  # pragma: no cover - argparse enforces the choices


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
