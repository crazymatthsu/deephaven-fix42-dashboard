import datetime as dt

import pytest

from market_data_demo.cli import build_parser, main
from market_data_demo.store import LocalStore


def test_parser_subcommands():
    parser = build_parser()
    args = parser.parse_args(["generate", "--symbols", "AAPL", "--start", "2026-09-01", "--end", "2026-09-02", "--seed", "3"])
    assert args.command == "generate" and args.start == dt.date(2026, 9, 1) and args.seed == 3
    with pytest.raises(SystemExit):
        parser.parse_args(["generate", "--start", "yesterday"])
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_generate_and_list_roundtrip(tmp_path, capsys):
    pytest.importorskip("pyarrow")
    root = tmp_path / "data"
    rc = main(["generate", "--root", str(root), "--symbols", "aapl,msft", "--start", "2026-09-03", "--end", "2026-09-08", "--quiet"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "generated 8 file(s)" in out and "4 trading day(s) x 2 symbol(s)" in out
    assert LocalStore(root).available_days() == [dt.date(2026, 9, 3), dt.date(2026, 9, 4), dt.date(2026, 9, 7), dt.date(2026, 9, 8)]

    rc = main(["list", "--root", str(root), "--files"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "symbols : 2" in out and "AAPL" in out and "2026-09-08" in out and "AAPL.parquet" in out

    rc = main(["generate", "--root", str(root), "--symbols", "aapl,msft", "--start", "2026-09-03", "--end", "2026-09-08", "--quiet"])
    assert rc == 0
    assert "skipped 8 existing" in capsys.readouterr().out


def test_list_empty_root_is_nonzero(tmp_path, capsys):
    assert main(["list", "--root", str(tmp_path / "empty")]) == 1
    assert "no data" in capsys.readouterr().out


def test_upload_requires_bucket(tmp_path):
    with pytest.raises(SystemExit):
        main(["upload", "--root", str(tmp_path)])
