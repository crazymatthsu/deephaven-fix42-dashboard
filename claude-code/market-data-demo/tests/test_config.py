import pytest

from market_data_demo.config import Config, load_config, make_store, parse_bool, parse_int
from market_data_demo.store import LocalStore, S3Store


def test_defaults_are_local():
    cfg = load_config({})
    assert cfg.source == "local" and not cfg.is_s3
    assert cfg.local_root == "/market-data"
    assert cfg.default_symbols == []
    assert cfg.default_days == 5
    assert cfg.default_interval == "1m"
    assert cfg.default_chart == "candlestick"
    assert cfg.hide_gaps is True
    assert cfg.cache_files == 512 and cfg.max_files == 2000 and cfg.read_threads == 4
    assert cfg.describe() == "local /market-data"
    assert isinstance(make_store(cfg), LocalStore)


def test_s3_config():
    cfg = load_config(
        {
            "MD_SOURCE": "S3",
            "MD_S3_BUCKET": "market-data",
            "MD_S3_PREFIX": "/ohlc/",
            "MD_S3_ENDPOINT": "http://minio:9000",
            "MD_S3_ACCESS_KEY_ID": "minioadmin",
            "MD_S3_SECRET_ACCESS_KEY": "secret",
            "MD_DEFAULT_SYMBOLS": "aapl, msft",
            "MD_DEFAULT_DAYS": "10",
            "MD_DEFAULT_INTERVAL": "5m",
            "MD_DEFAULT_CHART": "LINE",
            "MD_HIDE_GAPS": "no",
        }
    )
    assert cfg.is_s3
    assert cfg.s3_prefix == "ohlc"
    assert cfg.s3_path_style is True  # endpoint set -> path style default
    assert cfg.default_symbols == ["AAPL", "MSFT"]
    assert cfg.default_days == 10 and cfg.default_interval == "5m" and cfg.default_chart == "line"
    assert cfg.hide_gaps is False
    text = cfg.describe()
    assert "s3://market-data/ohlc" in text and "explicit keys" in text and "secret" not in text
    store = make_store(cfg)
    assert isinstance(store, S3Store) and store.bucket == "market-data" and store.prefix == "ohlc"


def test_s3_without_endpoint_defaults_to_virtual_host_and_default_chain():
    cfg = load_config({"MD_SOURCE": "s3", "MD_S3_BUCKET": "b"})
    assert cfg.s3_path_style is False
    assert "default credential chain" in cfg.describe()
    anon = load_config({"MD_SOURCE": "s3", "MD_S3_BUCKET": "b", "MD_S3_ANONYMOUS": "true"})
    assert anon.s3_anonymous and "anonymous" in anon.describe()


@pytest.mark.parametrize(
    "env, fragment",
    [
        ({"MD_SOURCE": "gcs"}, "MD_SOURCE"),
        ({"MD_SOURCE": "s3"}, "MD_S3_BUCKET"),
        ({"MD_SOURCE": "s3", "MD_S3_BUCKET": "a/b"}, "bucket name"),
        ({"MD_SOURCE": "s3", "MD_S3_BUCKET": "b", "MD_S3_ACCESS_KEY_ID": "k"}, "together"),
        ({"MD_SOURCE": "s3", "MD_S3_BUCKET": "b", "MD_S3_ANONYMOUS": "1", "MD_S3_ACCESS_KEY_ID": "k", "MD_S3_SECRET_ACCESS_KEY": "s"}, "ANONYMOUS"),
        ({"MD_SOURCE": "s3", "MD_S3_BUCKET": "b", "MD_S3_ENDPOINT": "minio:9000"}, "MD_S3_ENDPOINT"),
        ({"MD_DEFAULT_DAYS": "0"}, "MD_DEFAULT_DAYS"),
        ({"MD_DEFAULT_DAYS": "five"}, "MD_DEFAULT_DAYS"),
        ({"MD_DEFAULT_INTERVAL": "2m"}, "MD_DEFAULT_INTERVAL"),
        ({"MD_DEFAULT_CHART": "pie"}, "MD_DEFAULT_CHART"),
        ({"MD_HIDE_GAPS": "maybe"}, "MD_HIDE_GAPS"),
        ({"MD_MAX_FILES": "0"}, "MD_MAX_FILES"),
        ({"MD_DEFAULT_SYMBOLS": "A B/C"}, "symbol"),
    ],
)
def test_misconfiguration_is_a_startup_error(env, fragment):
    with pytest.raises(ValueError) as excinfo:
        load_config(env)
    assert fragment in str(excinfo.value)


def test_parse_helpers():
    assert parse_bool(None, "X", True) is True
    assert parse_bool("", "X", True) is True
    assert parse_bool("off", "X", True) is False
    assert parse_bool("Yes", "X", False) is True
    assert parse_int(None, "X", 3) == 3
    assert parse_int(" 7 ", "X", 3) == 7
    with pytest.raises(ValueError):
        parse_int("-1", "X", 3, minimum=0)


def test_config_is_plain_dataclass():
    cfg = Config(source="s3", s3_bucket="b", s3_prefix="p", s3_endpoint="http://x:1")
    assert "endpoint=http://x:1" in cfg.describe()
