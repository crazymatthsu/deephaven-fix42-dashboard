"""Source selection, AMPS config and the AMPS/update-graph hand-off.

These run on a bare python: ``dh_app.ingest`` and ``dh_app.amps_ingest`` import both
``deephaven`` and ``AMPS`` lazily, so everything except the table construction itself
is exercised here without either installed -- the same reason ``dh_app.schemas`` is
importable bare (doc 05 section 4).
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import pytest

from dh_app import amps_ingest
from dh_app import ingest
from dh_app.amps_ingest import AmpsConfig, AmpsRawSource, RawBuffer, resolve_bookmark


# --------------------------------------------------------------------------------------
# Source selection
# --------------------------------------------------------------------------------------


def test_default_source_is_kafka():
    assert ingest.fix_source({}) == ingest.SOURCE_KAFKA
    assert ingest.DEFAULT_SOURCE == ingest.SOURCE_KAFKA


@pytest.mark.parametrize(
    "value,expected",
    [
        ("kafka", ingest.SOURCE_KAFKA),
        ("amps", ingest.SOURCE_AMPS),
        ("AMPS", ingest.SOURCE_AMPS),
        ("  Kafka  ", ingest.SOURCE_KAFKA),
        ("", ingest.SOURCE_KAFKA),
    ],
)
def test_source_is_normalized(value, expected):
    assert ingest.fix_source({ingest.SOURCE_ENV: value}) == expected


def test_unknown_source_raises_rather_than_falling_back():
    # Silently defaulting to Kafka would leave a deployment that meant to read AMPS
    # looking healthy while rebuilding a cache from the wrong journal.
    with pytest.raises(ValueError) as excinfo:
        ingest.fix_source({ingest.SOURCE_ENV: "solace"})
    assert "solace" in str(excinfo.value)


def test_source_description_names_the_active_source():
    kafka = ingest.source_description({ingest.SOURCE_ENV: "kafka"})
    assert kafka.startswith("kafka: ")
    assert "seek to beginning" in kafka

    amps = ingest.source_description(
        {
            ingest.SOURCE_ENV: "amps",
            amps_ingest.URI_ENV: "tcp://amps-a:9007/amps/fix",
            amps_ingest.TOPIC_ENV: "FixJournal",
        }
    )
    assert amps.startswith("amps: ")
    assert "tcp://amps-a:9007/amps/fix" in amps
    assert "topic=FixJournal" in amps
    assert "bookmark=epoch" in amps


def test_kafka_defaults_survive_empty_env():
    assert ingest.kafka_bootstrap({}) == ingest.DEFAULT_BOOTSTRAP
    assert ingest.kafka_topic({}) == ingest.DEFAULT_TOPIC


# --------------------------------------------------------------------------------------
# AMPS configuration
# --------------------------------------------------------------------------------------


def test_amps_default_topic_mirrors_the_kafka_one():
    # amps_ingest cannot import ingest (ingest imports it), so the default is
    # duplicated. This test is what keeps the two copies honest.
    assert amps_ingest.DEFAULT_TOPIC == ingest.DEFAULT_TOPIC


def test_config_defaults():
    config = AmpsConfig.from_env({})
    assert config.uris == (amps_ingest.DEFAULT_URI,)
    assert config.topic == amps_ingest.DEFAULT_TOPIC
    assert config.filter is None
    assert config.client_name == amps_ingest.DEFAULT_CLIENT_NAME
    assert config.bookmark == amps_ingest.DEFAULT_BOOKMARK
    assert config.max_pending == amps_ingest.DEFAULT_MAX_PENDING


def test_config_reads_every_knob():
    config = AmpsConfig.from_env(
        {
            amps_ingest.URI_ENV: "tcp://a:9007/amps/fix, tcp://b:9007/amps/fix",
            amps_ingest.TOPIC_ENV: "FixJournal",
            amps_ingest.FILTER_ENV: "/35 = 'D'",
            amps_ingest.CLIENT_NAME_ENV: "dh-1",
            amps_ingest.BOOKMARK_ENV: "now",
            amps_ingest.MAX_PENDING_ENV: "64",
        }
    )
    assert config.uris == ("tcp://a:9007/amps/fix", "tcp://b:9007/amps/fix")
    assert config.topic == "FixJournal"
    assert config.filter == "/35 = 'D'"
    assert config.client_name == "dh-1"
    assert config.bookmark == "now"
    assert config.max_pending == 64


def test_topic_falls_back_to_the_shared_topic_env():
    config = AmpsConfig.from_env({amps_ingest.TOPIC_FALLBACK_ENV: "fix42.shared"})
    assert config.topic == "fix42.shared"

    # ...but the AMPS-specific name wins when both are set.
    config = AmpsConfig.from_env(
        {
            amps_ingest.TOPIC_FALLBACK_ENV: "fix42.shared",
            amps_ingest.TOPIC_ENV: "FixJournal",
        }
    )
    assert config.topic == "FixJournal"


@pytest.mark.parametrize("value", ["", "0", "-5", "not-a-number", None])
def test_unusable_max_pending_falls_back_to_the_default(value):
    env = {} if value is None else {amps_ingest.MAX_PENDING_ENV: value}
    assert AmpsConfig.from_env(env).max_pending == amps_ingest.DEFAULT_MAX_PENDING


def test_describe_includes_the_filter_only_when_set():
    assert "filter=" not in AmpsConfig.from_env({}).describe()
    described = AmpsConfig.from_env({amps_ingest.FILTER_ENV: "/35 = 'D'"}).describe()
    assert "filter=" in described


# --------------------------------------------------------------------------------------
# Bookmark resolution
# --------------------------------------------------------------------------------------


class FakeBookmarks:
    """Stands in for ``AMPS.Client.Bookmarks`` (values as of client 5.3.5)."""

    EPOCH = "0"
    NOW = "0|1|"
    MOST_RECENT = "recent"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("epoch", FakeBookmarks.EPOCH),
        ("EPOCH", FakeBookmarks.EPOCH),
        ("beginning", FakeBookmarks.EPOCH),
        ("now", FakeBookmarks.NOW),
        ("most_recent", FakeBookmarks.MOST_RECENT),
        ("most-recent", FakeBookmarks.MOST_RECENT),
        ("recent", FakeBookmarks.MOST_RECENT),
    ],
)
def test_bookmark_aliases_resolve(value, expected):
    assert resolve_bookmark(value, FakeBookmarks) == expected


def test_default_bookmark_is_a_full_replay():
    # The whole point of the AMPS source: restart => replay => identical cache.
    assert resolve_bookmark(amps_ingest.DEFAULT_BOOKMARK, FakeBookmarks) == FakeBookmarks.EPOCH


def test_literal_bookmarks_pass_through_untouched():
    assert resolve_bookmark("3|1|", FakeBookmarks) == "3|1|"


# --------------------------------------------------------------------------------------
# RawBuffer
# --------------------------------------------------------------------------------------


def test_buffer_drain_preserves_order_and_empties():
    buffer = RawBuffer(max_pending=10)
    for i in range(5):
        assert buffer.offer((f"msg{i}", "", None)) is True
    assert buffer.pending == 5
    assert [row[0] for row in buffer.drain()] == ["msg0", "msg1", "msg2", "msg3", "msg4"]
    assert buffer.pending == 0
    assert buffer.drain() == []
    assert buffer.offered == 5


def test_buffer_blocks_the_producer_when_full_then_resumes_after_a_drain():
    # Blocking, not dropping: a dropped FIX message would silently break an amend
    # chain with nothing downstream able to notice.
    buffer = RawBuffer(max_pending=2)
    buffer.offer(("a", "", None))
    buffer.offer(("b", "", None))

    accepted = []
    producer = threading.Thread(target=lambda: accepted.append(buffer.offer(("c", "", None))))
    producer.start()

    deadline = time.monotonic() + 2.0
    while buffer.waits == 0 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert buffer.waits == 1, "producer should have blocked on a full buffer"
    assert producer.is_alive()

    drained = buffer.drain()
    producer.join(timeout=2.0)
    assert not producer.is_alive()
    assert accepted == [True]
    assert [row[0] for row in drained] == ["a", "b"]
    assert [row[0] for row in buffer.drain()] == ["c"]
    assert buffer.dropped == 0


def test_buffer_offer_times_out_rather_than_blocking_forever():
    buffer = RawBuffer(max_pending=1)
    buffer.offer(("a", "", None))
    assert buffer.offer(("b", "", None), timeout=0.05) is False
    assert buffer.dropped == 1


def test_close_releases_a_blocked_producer():
    buffer = RawBuffer(max_pending=1)
    buffer.offer(("a", "", None))

    accepted = []
    producer = threading.Thread(target=lambda: accepted.append(buffer.offer(("b", "", None))))
    producer.start()

    deadline = time.monotonic() + 2.0
    while buffer.waits == 0 and time.monotonic() < deadline:
        time.sleep(0.005)
    buffer.close()
    producer.join(timeout=2.0)

    assert not producer.is_alive()
    assert accepted == [False]
    assert buffer.closed is True
    assert buffer.offer(("c", "", None)) is False


# --------------------------------------------------------------------------------------
# AmpsRawSource message handling (no AMPS client, no Deephaven)
# --------------------------------------------------------------------------------------


class FakeMessage:
    """The three ``AMPS.Message`` accessors :meth:`AmpsRawSource._on_message` uses."""

    def __init__(self, data, bookmark="1|1|"):
        self._data = data
        self._bookmark = bookmark

    def get_data(self):
        return self._data

    def get_bookmark(self):
        return self._bookmark


class FakeClient:
    """Records what the source did to it."""

    def __init__(self):
        self.discarded = []
        self.unsubscribed = []
        self.closed = False
        self.listeners = []

    def add_connection_state_listener(self, listener):
        self.listeners.append(listener)

    def discard(self, message):
        self.discarded.append(message)

    def unsubscribe(self, sub_id):
        self.unsubscribed.append(sub_id)

    def close(self):
        self.closed = True


def _source(max_pending=10):
    """An unstarted source with a fake client already attached."""
    config = AmpsConfig.from_env({amps_ingest.MAX_PENDING_ENV: str(max_pending)})
    clock = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    source = AmpsRawSource(config=config, client_factory=lambda _c: FakeClient(), now_fn=lambda: clock)
    source._client = FakeClient()
    return source


def test_on_message_buffers_payload_and_bookmark():
    source = _source()
    source._on_message(FakeMessage("8=FIX.4.2|35=D|", bookmark="7|1|"))
    rows = source.buffer.drain()
    assert len(rows) == 1
    raw, bookmark, ingest_ts = rows[0]
    assert raw == "8=FIX.4.2|35=D|"
    assert bookmark == "7|1|"
    assert ingest_ts == datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)


def test_on_message_discards_the_bookmark_every_time():
    # Without the discard, an HA reconnect replays from the oldest undiscarded
    # bookmark -- i.e. re-delivers the entire journal on every blip.
    source = _source()
    messages = [FakeMessage("8=FIX.4.2|35=D|"), FakeMessage(""), FakeMessage(None)]
    for message in messages:
        source._on_message(message)
    assert source._client.discarded == messages


def test_on_message_skips_empty_payloads():
    source = _source()
    source._on_message(FakeMessage(""))
    source._on_message(FakeMessage(None))
    assert source.buffer.drain() == []


def test_on_message_never_raises_out_of_the_handler():
    # An exception escaping the AMPS handler kills the subscription, and the
    # dashboard would go quiet with no error anywhere downstream.
    class Exploding:
        def get_data(self):
            raise RuntimeError("boom")

    source = _source()
    message = Exploding()
    source._on_message(message)
    assert source.buffer.drain() == []
    assert source._client.discarded == [message]


def test_stop_unsubscribes_closes_and_releases_the_buffer():
    source = _source()
    client = source._client
    source._sub_id = "sub-1"
    source.stop()
    assert client.unsubscribed == ["sub-1"]
    assert client.closed is True
    assert source.buffer.closed is True
    # Idempotent: Deephaven calls it on shutdown, and start() calls it on failure.
    source.stop()


def test_stop_survives_a_client_that_throws():
    class BadClient(FakeClient):
        def unsubscribe(self, sub_id):
            raise RuntimeError("already gone")

        def close(self):
            raise RuntimeError("already closed")

    source = _source()
    source._client = BadClient()
    source._sub_id = "sub-1"
    source.stop()  # must not raise
    assert source.buffer.closed is True


def test_columns_match_the_buffered_tuple_shape():
    assert amps_ingest.COLUMN_NAMES[0] == amps_ingest.RAW_COLUMN == "RawFix"
    source = _source()
    source._on_message(FakeMessage("8=FIX.4.2|"))
    assert len(source.buffer.drain()[0]) == len(amps_ingest.COLUMN_NAMES)


# --------------------------------------------------------------------------------------
# AmpsRawSource.start() against a stubbed AMPS module
#
# This is the one place the ``bookmark_subscribe(on_message, topic, bookmark, filter)``
# call shape is pinned. It is the whole feature -- get the bookmark argument wrong and
# the pipeline quietly starts from "now" instead of replaying the journal -- and there
# is no AMPS server anywhere in this repo to catch it.
# --------------------------------------------------------------------------------------


class FakeSubscribingClient(FakeClient):
    """Captures the ``bookmark_subscribe`` call and reports connect/logon."""

    def __init__(self, fail_on=None):
        super().__init__()
        self.subscribe_calls = []
        self.connected = False
        self._fail_on = fail_on

    def connect_and_logon(self):
        if self._fail_on == "logon":
            raise RuntimeError("no route to host")
        self.connected = True

    def bookmark_subscribe(self, on_message, topic, bookmark, filter=None):
        if self._fail_on == "subscribe":
            raise RuntimeError("topic not found")
        self.subscribe_calls.append((on_message, topic, bookmark, filter))
        return "sub-42"


@pytest.fixture
def fake_amps(monkeypatch):
    """Install a stub ``AMPS`` module so ``start()``'s lazy import resolves."""
    import sys
    import types

    module = types.ModuleType("AMPS")
    module.Client = types.SimpleNamespace(Bookmarks=FakeBookmarks)
    monkeypatch.setitem(sys.modules, "AMPS", module)
    return module


def _started_source(env, client, monkeypatch):
    """Start a source against ``client``, skipping the Deephaven table build."""
    source = AmpsRawSource(config=AmpsConfig.from_env(env), client_factory=lambda _c: client)
    source.table = object()  # stands in for the blink table build_table() would make
    monkeypatch.setattr(source, "build_table", lambda: source.table)
    return source


def test_start_subscribes_from_the_epoch_bookmark(fake_amps, monkeypatch):
    client = FakeSubscribingClient()
    source = _started_source(
        {amps_ingest.TOPIC_ENV: "FixJournal"}, client, monkeypatch
    )
    source.start()

    assert client.connected is True
    assert client.listeners == [source._on_connection_state]
    assert len(client.subscribe_calls) == 1
    on_message, topic, bookmark, filter_ = client.subscribe_calls[0]
    assert on_message == source._on_message
    assert topic == "FixJournal"
    assert bookmark == FakeBookmarks.EPOCH  # replay the whole transaction log
    assert filter_ is None
    assert source._sub_id == "sub-42"


def test_start_passes_a_configured_bookmark_and_filter(fake_amps, monkeypatch):
    client = FakeSubscribingClient()
    source = _started_source(
        {
            amps_ingest.TOPIC_ENV: "FixJournal",
            amps_ingest.BOOKMARK_ENV: "most_recent",
            amps_ingest.FILTER_ENV: "/35 = 'D'",
        },
        client,
        monkeypatch,
    )
    source.start()

    _, _, bookmark, filter_ = client.subscribe_calls[0]
    assert bookmark == FakeBookmarks.MOST_RECENT
    assert filter_ == "/35 = 'D'"


def test_start_twice_raises(fake_amps, monkeypatch):
    source = _started_source({}, FakeSubscribingClient(), monkeypatch)
    source.start()
    with pytest.raises(RuntimeError):
        source.start()


@pytest.mark.parametrize("fail_on", ["logon", "subscribe"])
def test_a_failed_start_closes_the_client_and_stays_restartable(
    fake_amps, monkeypatch, fail_on
):
    client = FakeSubscribingClient(fail_on=fail_on)
    source = _started_source({}, client, monkeypatch)
    with pytest.raises(RuntimeError):
        source.start()
    assert client.closed is True
    assert source._client is None  # not left half-started, so a retry can proceed


# --------------------------------------------------------------------------------------
# The flush callback
#
# ``_on_flush`` runs on the update-graph thread once per cycle and is the only path by
# which AMPS rows reach Deephaven. The deephaven symbols it uses are looked up through
# ``_deephaven()``, so swapping that out exercises the real wiring -- drain, execution
# context, one batch per cycle, counters -- without stubbing the deephaven package.
# --------------------------------------------------------------------------------------


class RecordingPublisher:
    """Stands in for ``deephaven.stream.table_publisher.TablePublisher``."""

    def __init__(self):
        self.batches = []

    def add(self, table):
        self.batches.append(table)


def _fake_deephaven(record):
    """A ``_deephaven()`` replacement whose column factories append to ``record``."""

    def column(name, values):
        record.append((name, list(values)))
        return (name, list(values))

    return {
        "new_table": lambda cols: {"columns": cols},
        "string_col": column,
        "instant_col": column,
        "to_j_instant": lambda value: f"instant:{value.isoformat()}",
    }


def test_flush_publishes_one_batch_per_cycle_under_the_execution_context(monkeypatch):
    columns = []
    monkeypatch.setattr(amps_ingest, "_deephaven", lambda: _fake_deephaven(columns))

    source = _source()
    entered = []

    class TrackingContext:
        def __enter__(self):
            entered.append(True)
            return self

        def __exit__(self, *exc):
            return False

    source._ctx = TrackingContext()
    publisher = RecordingPublisher()

    source._on_message(FakeMessage("8=FIX.4.2|35=D|", bookmark="1|1|"))
    source._on_message(FakeMessage("8=FIX.4.2|35=8|", bookmark="2|1|"))
    source._on_flush(publisher)

    assert len(publisher.batches) == 1, "one add() per update cycle, never one per row"
    assert entered == [True]
    assert source.published == 2
    assert [name for name, _ in columns] == list(amps_ingest.COLUMN_NAMES)
    assert columns[0][1] == ["8=FIX.4.2|35=D|", "8=FIX.4.2|35=8|"]
    assert columns[1][1] == ["1|1|", "2|1|"]
    assert columns[2][1] == ["instant:2024-01-15T14:30:00+00:00"] * 2

    # The buffer is now empty, so the next cycle publishes nothing at all.
    source._on_flush(publisher)
    assert len(publisher.batches) == 1
    assert source.published == 2


def test_flush_never_raises_out_of_the_update_cycle(monkeypatch):
    import contextlib

    def exploding():
        raise RuntimeError("boom")

    monkeypatch.setattr(amps_ingest, "_deephaven", exploding)
    source = _source()
    source._ctx = contextlib.nullcontext()
    source._on_message(FakeMessage("8=FIX.4.2|"))

    source._on_flush(RecordingPublisher())  # an exception here would stall the graph

    assert source.failed_batches == 1
    assert source.published == 0
