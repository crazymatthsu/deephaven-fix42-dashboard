"""Publish native order rows to the AMPS SOW topics -- doc 14 section 13.

Plays the three upstream systems: each order's current state goes to its system's SOW
topic as one JSON message, and a ``sow_delete`` removes it. Used by the integration test
(``e2e/``), by ``scripts/run_demo.sh`` with ``OTR_SOURCE=amps``, and from the command line::

    python -m order_tree_recon.amps_publish --uri tcp://localhost:39007/amps/json
    python -m order_tree_recon.amps_publish --uri ... --seed 7 --families 200
    python -m order_tree_recon.amps_publish --uri ... --clear      # sow_delete everything

Topics are published upstream first (Raptor, algo, router), but nothing depends on it:
the topics are independent, and an order that arrives before its parent is an ``ORPHAN``
until the parent lands (the integration test checks exactly that).

``AMPS`` is imported only by :func:`connect` and :func:`main`; the rest is pure.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon import amps_codec, config, mockdata
from order_tree_recon.native import NATIVE_SCHEMAS, NativeSchema

__all__ = ["book_messages", "connect", "publish_rows", "delete_orders", "publish_book", "clear", "main"]


def book_messages(data: mockdata.MockData, prefix: str) -> List[Tuple[str, str]]:
    """``(topic, json)`` for every row of a mock book, Raptor first, then algo, then router."""
    tables: Dict[str, Sequence[Mapping[str, Any]]] = {
        "raptor_orders": data.raptor,
        "algo_orders": data.algo,
        "router_orders": data.router,
    }
    out: List[Tuple[str, str]] = []
    for name, schema in NATIVE_SCHEMAS.items():
        topic = amps_codec.topic(schema, prefix)
        out.extend((topic, amps_codec.encode(schema, row)) for row in tables[name])
    return out


def connect(uri: str, client_name: str) -> Any:
    """A logged-on ``AMPS.Client`` (one URI; a publisher has no subscriptions to restore)."""
    import AMPS

    client = AMPS.Client(client_name)
    client.connect(uri)
    client.logon()
    return client


def publish_rows(client: Any, schema: NativeSchema, rows: Iterable[Mapping[str, Any]], prefix: str,
                 flush: bool = True) -> int:
    """Publish rows of one native table; ``publish_flush`` waits until AMPS processed them."""
    topic = amps_codec.topic(schema, prefix)
    count = 0
    for row in rows:
        client.publish(topic, amps_codec.encode(schema, row))
        count += 1
    if flush:
        client.publish_flush()
    return count


def delete_orders(client: Any, schema: NativeSchema, order_ids: Iterable[str], prefix: str) -> int:
    """``sow_delete`` orders by id; subscribers see one OOF each. Returns the count deleted."""
    topic = amps_codec.topic(schema, prefix)
    deleted = 0
    for order_id in order_ids:
        ack = client.sow_delete_by_data(topic, amps_codec.delete_payload(schema, order_id))
        deleted += _records_deleted(ack)
    return deleted


def _records_deleted(ack: Any) -> int:
    for getter in ("get_records_deleted", "get_matches"):
        try:
            return int(getattr(ack, getter)())
        except Exception:  # noqa: BLE001 - the count is informational
            continue
    return 0


def publish_book(client: Any, data: mockdata.MockData, prefix: str) -> int:
    """Publish a whole mock book; returns the message count."""
    count = 0
    for topic, message in book_messages(data, prefix):
        client.publish(topic, message)
        count += 1
    client.publish_flush()
    return count


def clear(client: Any, prefix: str) -> int:
    """``sow_delete`` every record of the three topics."""
    deleted = 0
    for schema in NATIVE_SCHEMAS.values():
        deleted += _records_deleted(client.sow_delete(amps_codec.topic(schema, prefix), "1=1"))
    return deleted


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m order_tree_recon.amps_publish",
        description="Publish the mock Raptor / algo / router book to the AMPS SOW topics (doc 14 s13).",
    )
    parser.add_argument("--uri", default=config.DEFAULT_AMPS_URI, help="tcp://host:port/amps/json")
    parser.add_argument("--prefix", default=config.DEFAULT_AMPS_TOPIC_PREFIX, help="topic prefix (default otr.)")
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--families", type=int, default=config.DEFAULT_MOCK_FAMILIES)
    parser.add_argument("--client-name", default="otr-publisher")
    parser.add_argument("--clear", action="store_true", help="sow_delete every order first")
    parser.add_argument("--clear-only", action="store_true", help="sow_delete every order and stop")
    args = parser.parse_args(argv)

    # The same validation the Deephaven side applies to OTR_AMPS_URI / OTR_AMPS_TOPIC_PREFIX.
    settings = config.amps_settings({config.AMPS_URI_ENV: args.uri, config.AMPS_TOPIC_PREFIX_ENV: args.prefix})
    client = connect(settings.uris[0], args.client_name)
    try:
        if args.clear or args.clear_only:
            print(f"[amps_publish] cleared {clear(client, settings.topic_prefix)} records")
        if args.clear_only:
            return 0
        data = mockdata.generate(args.seed, args.families)
        count = publish_book(client, data, settings.topic_prefix)
        print(
            f"[amps_publish] seed={args.seed} families={args.families}: {count} messages "
            f"({len(data.raptor)} raptor / {len(data.algo)} algo / {len(data.router)} router) "
            f"-> {settings.uris[0]} {settings.topic_prefix}<table>"
        )
    finally:
        client.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
