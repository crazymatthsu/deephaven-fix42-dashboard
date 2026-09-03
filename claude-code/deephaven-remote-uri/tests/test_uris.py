"""Scope URIs, Barrage tickets and global names -- doc 10 sections 3 and 6.

Runs on a bare host python: :mod:`remote_uri.uris` is pure stdlib, which is what lets
a typo in ``REMOTEURI_LEAVES`` be caught at startup instead of after a five-minute
resolve timeout.
"""

from __future__ import annotations

import pytest

from remote_uri import uris


# --------------------------------------------------------------------------------------
# scope_uri / scope_ticket
# --------------------------------------------------------------------------------------


def test_scope_uri_is_the_documented_form():
    assert (
        uris.scope_uri("dh+plain://dh1:10000", "rx_orders")
        == "dh+plain://dh1:10000/scope/rx_orders"
    )


def test_scope_uri_tolerates_a_trailing_slash():
    assert (
        uris.scope_uri("dh+plain://dh1:10000/", "rx_exposure")
        == "dh+plain://dh1:10000/scope/rx_exposure"
    )


def test_scope_ticket_is_bytes_with_the_s_prefix():
    ticket = uris.scope_ticket("rx_leaf_stats")
    assert ticket == b"s/rx_leaf_stats"
    # The Java client takes bytes; a str fails deep inside it with a worse message.
    assert isinstance(ticket, bytes)


def test_every_leaf_export_has_a_uri_and_a_ticket():
    assert uris.LEAF_EXPORTS == ("rx_orders", "rx_id_index", "rx_exposure", "rx_leaf_stats")
    for name in uris.LEAF_EXPORTS:
        assert uris.scope_uri("dh+plain://dh2:10000", name).endswith(f"/scope/{name}")
        assert uris.scope_ticket(name) == f"s/{name}".encode()


# --------------------------------------------------------------------------------------
# host_port
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("dh+plain://dh1:10000", ("dh1", 10000)),
        ("dh+plain://dh1", ("dh1", 10000)),
        ("dh+plain://rx-dh2:10012", ("rx-dh2", 10012)),
        ("dh://secure-leaf:443", ("secure-leaf", 443)),
        ("dh+plain://dh1:10000/", ("dh1", 10000)),
    ],
)
def test_host_port_splits_the_authority(uri, expected):
    assert uris.host_port(uri) == expected


@pytest.mark.parametrize(
    "uri",
    [
        "http://dh1:10000",
        "dh1:10000",
        "",
        "dh+plain://dh1:not-a-port",
        "dh+plain://dh1:0",
        "dh+plain://dh1:70000",
        "dh+plain://:10000",
        "dh+plain://[::1]:10000",
    ],
)
def test_host_port_rejects_what_barrage_cannot_dial(uri):
    with pytest.raises(ValueError):
        uris.host_port(uri)


# --------------------------------------------------------------------------------------
# normalize_leaf_uri
# --------------------------------------------------------------------------------------


def test_normalize_keeps_a_valid_uri_and_drops_the_trailing_slash():
    assert uris.normalize_leaf_uri("dh+plain://dh1:10000/") == "dh+plain://dh1:10000"
    assert uris.normalize_leaf_uri("  dh://leaf  ") == "dh://leaf"


def test_normalize_rejects_a_scope_path():
    # The collector appends /scope/<name> per export; a URI that already carries one
    # would resolve dh+plain://dh1:10000/scope/rx_orders/scope/rx_orders.
    with pytest.raises(ValueError) as excinfo:
        uris.normalize_leaf_uri("dh+plain://dh1:10000/scope/rx_orders")
    assert "scope" in str(excinfo.value)


@pytest.mark.parametrize("uri", ["", "   ", None, "grpc://dh1:10000", "dh+plain://"])
def test_normalize_rejects_the_rest(uri):
    with pytest.raises(ValueError):
        uris.normalize_leaf_uri(uri)


def test_normalize_names_the_offending_key():
    with pytest.raises(ValueError) as excinfo:
        uris.normalize_leaf_uri("nope", label="leaf 'DH1' 'uri'")
    assert "leaf 'DH1' 'uri'" in str(excinfo.value)


# --------------------------------------------------------------------------------------
# Global names
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "leaf,expected",
    [
        ("DH1", "dh1"),
        ("DH-2", "dh_2"),
        ("eu west", "eu_west"),
        ("3rd", "_3rd"),
        ("", "_"),
    ],
)
def test_global_suffix_is_a_legal_lowercase_identifier(leaf, expected):
    assert uris.global_suffix(leaf) == expected


def test_leaf_global_name_matches_the_frozen_shape():
    assert uris.leaf_global_name("rx_orders", "DH1") == "rx_orders_dh1"
    assert uris.leaf_global_name("rx_leaf_stats", "DH2") == "rx_leaf_stats_dh2"


def test_raw_global_name_is_doc_09s():
    assert uris.raw_global_name("OMS-A") == "oms_raw_oms_a"
    assert uris.raw_global_name("OMS-B-parent") == "oms_raw_oms_b_parent"
