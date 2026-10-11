"""The AMPS source's pure parts: settings, the JSON codec, the publisher's messages and
the per-topic message rules (doc 14 section 13). The broker round trip is the e2e suite's."""

from __future__ import annotations

import json
import threading

import pytest

from order_tree_recon import amps_codec, amps_publish, config
from order_tree_recon.amps_source import TopicFeed
from order_tree_recon.native import ALGO, NATIVE_SCHEMAS, RAPTOR, ROUTER


# --------------------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------------------


def test_source_defaults_to_mock():
    assert config.source({}) == "mock"
    assert config.source({"OTR_SOURCE": " AMPS "}) == "amps"
    with pytest.raises(ValueError, match="OTR_SOURCE"):
        config.source({"OTR_SOURCE": "kafka"})


def test_amps_settings():
    assert config.amps_settings({}) == config.AmpsSettings(
        ("tcp://amps:9007/amps/json",), "otr.", "otr-deephaven"
    )
    settings = config.amps_settings({
        "OTR_AMPS_URI": "tcp://a:9007/amps/json, tcps://b:9443/amps/json",
        "OTR_AMPS_TOPIC_PREFIX": "",
        "OTR_AMPS_CLIENT_NAME": "dh-1",
    })
    assert settings.uris == ("tcp://a:9007/amps/json", "tcps://b:9443/amps/json")
    assert settings.topic_prefix == ""
    assert settings.client_name == "dh-1"


@pytest.mark.parametrize(
    "env, message",
    [
        ({"OTR_AMPS_URI": "tcp://a:9007/amps/fix"}, "OTR_AMPS_URI"),  # the codec is JSON only
        ({"OTR_AMPS_URI": "a:9007"}, "OTR_AMPS_URI"),
        ({"OTR_AMPS_TOPIC_PREFIX": "otr `x"}, "OTR_AMPS_TOPIC_PREFIX"),
        ({"OTR_AMPS_CLIENT_NAME": "a b"}, "OTR_AMPS_CLIENT_NAME"),
    ],
)
def test_amps_settings_reject(env, message):
    with pytest.raises(ValueError, match=message):
        config.amps_settings(env)


# --------------------------------------------------------------------------------------
# Codec
# --------------------------------------------------------------------------------------


def test_topics():
    assert amps_codec.topics("otr.") == {
        "raptor_orders": "otr.raptor_orders",
        "algo_orders": "otr.algo_orders",
        "router_orders": "otr.router_orders",
    }


def test_every_mock_row_round_trips(book):
    for schema, rows in ((RAPTOR, book.raptor), (ALGO, book.algo), (ROUTER, book.router)):
        for row in rows:
            decoded = amps_codec.decode(schema, amps_codec.encode(schema, row))
            assert dict(zip(schema.column_names, decoded)) == {c: row[c] for c in schema.column_names}


def test_decode_is_lenient_about_shape():
    values = dict(zip(ALGO.column_names, amps_codec.decode(ALGO, '{"AlgoOrderId": "A-1", "Qty": "300", "Filled": 100.0}')))
    assert values["AlgoOrderId"] == "A-1"
    assert values["ParentAlgoOrderId"] is None  # missing -> null; the adapter treats it as a parent
    assert values["Qty"] == 300 and values["Filled"] == 100
    assert values["AvgPrice"] is None


@pytest.mark.parametrize(
    "data, message",
    [
        ("not json", "not JSON"),
        ("[1, 2]", "JSON object"),
        ('{"Qty": 1}', "no AlgoOrderId"),
        ('{"AlgoOrderId": ""}', "no AlgoOrderId"),
        ('{"AlgoOrderId": "A", "Qty": 1.5}', "whole number"),
        ('{"AlgoOrderId": "A", "Qty": "lots"}', "expected a long"),
        ('{"AlgoOrderId": "A", "Qty": true}', "expected a long"),
        ('{"AlgoOrderId": "A", "Symbol": {"x": 1}}', "expected a string"),
    ],
)
def test_decode_rejects(data, message):
    with pytest.raises(amps_codec.DecodeError, match=message):
        amps_codec.decode(ALGO, data)


def test_encode_needs_the_key_and_finite_numbers():
    with pytest.raises(KeyError):
        amps_codec.encode(ROUTER, {"Qty": 1})
    with pytest.raises(ValueError):
        amps_codec.encode(ROUTER, {"RouterOrderId": "R", "AvgPx": float("nan")})


def test_delete_payload_carries_only_the_key():
    assert json.loads(amps_codec.delete_payload(ROUTER, "OR-1.2")) == {"RouterOrderId": "OR-1.2"}


def test_book_messages_cover_the_whole_book(book):
    messages = amps_publish.book_messages(book, "otr.")
    assert len(messages) == len(book.raptor) + len(book.algo) + len(book.router)
    assert [t for t, _ in messages] == (
        ["otr.raptor_orders"] * len(book.raptor)
        + ["otr.algo_orders"] * len(book.algo)
        + ["otr.router_orders"] * len(book.router)
    )


# --------------------------------------------------------------------------------------
# TopicFeed: the per-topic message rules
# --------------------------------------------------------------------------------------


def _msg(order_id, **values):
    return json.dumps({"RouterOrderId": order_id, "Qty": 100, **values})


def _feed(**kw):
    return TopicFeed(ROUTER, "otr.router_orders", **kw)


def _rows(feed):
    """Drained rows as ``(order id, Deleted, CumQty)``."""
    key = ROUTER.column_names.index("RouterOrderId")
    cum = ROUTER.column_names.index("CumQty")
    return [(r[key], r[-1], r[cum]) for r in feed.drain()]


def test_snapshot_then_live_then_oof():
    feed = _feed()
    feed.handle("group_begin", "", "")
    feed.handle("sow", _msg("A", CumQty=0), "k1")
    feed.handle("sow", _msg("B", CumQty=0), "k2")
    feed.handle("group_end", "", "")
    feed.handle("publish", _msg("A", CumQty=50), "k1")
    feed.handle("oof", _msg("B"), "k2")
    feed.handle("ack", "", "")
    assert _rows(feed) == [("A", False, 0), ("B", False, 0), ("A", False, 50), ("B", True, None)]
    assert (feed.snapshots, feed.received, feed.upserts, feed.deletes, feed.live) == (1, 4, 3, 1, 1)
    assert feed.drain() == []


def test_oof_falls_back_to_the_data_when_the_sow_key_is_unknown():
    feed = _feed()
    feed.handle("oof", _msg("Z"), "never-seen")
    assert _rows(feed) == [("Z", True, None)]


def test_an_oof_for_a_malformed_record_still_resolves_its_key():
    feed = _feed()
    feed.handle("oof", json.dumps({"RouterOrderId": "BAD-1", "Qty": "lots"}), "unseen")
    assert _rows(feed) == [("BAD-1", True, None)] and feed.rejected == 0


def test_bad_messages_are_counted_not_raised():
    feed = _feed()
    feed.handle("publish", "{not json", "k1")
    feed.handle("publish", json.dumps({"Qty": 1}), "k2")
    feed.handle("oof", "", "unknown")
    assert feed.rejected == 3 and feed.drain() == []
    assert "oof for an unknown record" in feed.last_error


def test_resnapshot_deletes_what_vanished_while_disconnected():
    feed = _feed()
    feed.handle("group_begin", "", "")
    for order_id, key in (("A", "k1"), ("B", "k2"), ("C", "k3")):
        feed.handle("sow", _msg(order_id), key)
    feed.handle("group_end", "", "")
    feed.drain()
    # disconnect; B is sow_deleted meanwhile (its OOF is lost); the client re-subscribes
    feed.handle("group_begin", "", "")
    feed.handle("sow", _msg("A"), "k1")
    feed.handle("sow", _msg("C", CumQty=100), "k3")
    feed.handle("group_end", "", "")
    assert _rows(feed) == [("A", False, None), ("C", False, 100), ("B", True, None)]
    assert (feed.snapshots, feed.resynced, feed.live) == (2, 1, 2)
    # B's SOW key is forgotten with it: a later OOF for k2 resolves from its data, if any
    feed.handle("oof", "", "k2")
    assert feed.rejected == 1


def test_a_full_buffer_blocks_until_drained():
    feed = _feed(max_pending=2)
    feed.handle("publish", _msg("A"), "k1")
    feed.handle("publish", _msg("B"), "k2")
    done = threading.Event()

    def third():
        feed.handle("publish", _msg("C"), "k3")
        done.set()

    worker = threading.Thread(target=third, daemon=True)
    worker.start()
    assert not done.wait(0.2)  # blocked: dropping a message would leave a wrong book
    assert [r[0] for r in _rows(feed)] == ["A", "B"]
    assert done.wait(2)
    assert [r[0] for r in _rows(feed)] == ["C"]


def test_close_releases_a_blocked_receive_thread():
    feed = _feed(max_pending=1)
    feed.handle("publish", _msg("A"), "k1")
    worker = threading.Thread(target=feed.handle, args=("publish", _msg("B"), "k2"), daemon=True)
    worker.start()
    feed.close()
    worker.join(2)
    assert not worker.is_alive()


def test_status_row_has_every_counter():
    from order_tree_recon.amps_source import STATUS_COLUMNS

    status = _feed().status()
    assert set(status) | {"Connected", "State"} == set(STATUS_COLUMNS)
    assert status["Table"] == "router_orders" and status["Topic"] == "otr.router_orders"


def test_native_schemas_match_the_mock_book(book):
    for schema, rows in ((RAPTOR, book.raptor), (ALGO, book.algo), (ROUTER, book.router)):
        assert NATIVE_SCHEMAS[schema.name] is schema
        assert all(tuple(row) == schema.column_names for row in rows)
        assert len({row[schema.key] for row in rows}) == len(rows)  # the SOW key is unique


def test_start_is_idempotent_and_never_runs_two_clients_under_one_name(monkeypatch):
    from order_tree_recon import amps_source

    class FakeSource:
        def __init__(self, settings):
            self.settings = settings
            self.started = self.stopped = False

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(amps_source, "AmpsOrderSource", FakeSource)
    monkeypatch.setattr(amps_source, "_ACTIVE", [])
    first = amps_source.start(config.amps_settings({}))
    assert amps_source.start(config.amps_settings({})) is first  # a re-run reuses it
    # same client name, other broker: the old client must go, or the two would keep
    # displacing each other's logon
    second = amps_source.start(config.amps_settings({"OTR_AMPS_URI": "tcp://other:9007/amps/json"}))
    assert first.stopped and second.started and amps_source.active_sources() == [second]
    third = amps_source.start(config.amps_settings({"OTR_AMPS_CLIENT_NAME": "dh-2"}))
    assert not second.stopped and amps_source.active_sources() == [second, third]
