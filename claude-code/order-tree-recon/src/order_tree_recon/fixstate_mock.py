"""FIX order-state messages for the mock book -- what ``fix-samples/`` holds (doc 14 section 12).

Each mock order (:mod:`order_tree_recon.mockdata`) becomes the order-state messages its system
would publish to its AMPS SOW topics over the day: a ``NEW`` state, a partial fill, the final
fill and, for every order the scenario killed, a ``CANCELED`` state. Every message is the
order's complete state, so the last one *is* the mock table row, and the FIX path can be
tested against the table path on the same book.

The messages follow :data:`order_tree_recon.fixstate.DEFAULT_FIX_CONFIG`:

* topics -- ``raptor.orders``; ``algo.parent.orders`` / ``algo.child.orders``;
  ``router.parent.orders`` / ``router.child.orders`` (several SOW topics per system);
* bodies only, as AMPS delivers a ``fix`` topic (no ``8=`` / ``9=`` / ``10=``), with ``|``
  instead of SOH for readability (the parser takes both; the AMPS loader converts);
* ClOrdID (11) is the cross-system link: an algo parent's 11 is the Raptor OrderID it was
  sent with, a router parent's 11 the algo child's id -- or, for DMA, the client ClOrdID
  Raptor passed through; an in-system child names its parent in tag 20001;
* the ``dma`` client order is cancel/replaced mid-day (``11=...-R1``, ``41=`` the original)
  while its router parent keeps the original ClOrdID: that link resolves through Raptor's
  OrigClOrdID / id history.

Regenerate the committed samples with::

    python -m order_tree_recon.fixstate_mock --out fix-samples

or publish the same messages, in send order, to an AMPS server whose SOW topics are keyed
on tag 37 (``order-tree-recon/amps/amps-config.xml``; needs ``amps-python-client``)::

    python -m order_tree_recon.fixstate_mock --publish tcp://amps:9007/amps/fix [--delay-ms 20]
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from order_tree_recon import config, fixstate, mockdata, states
from order_tree_recon.mockdata import MockData, MockOrder

__all__ = ["TOPIC_OF_LEVEL", "PARENT_TAG", "AMENDED_SCENARIO", "send_order", "messages", "events",
           "write_samples", "publish", "config_json", "main"]

#: Level -> the SOW topic its orders are published to.
TOPIC_OF_LEVEL: Dict[str, str] = {
    "CLIENT": "raptor.orders",
    "ALGO_PARENT": "algo.parent.orders",
    "ALGO_CHILD": "algo.child.orders",
    "OR_PARENT": "router.parent.orders",
    "OR_CHILD": "router.child.orders",
}
_SYSTEM_OF_LEVEL = {"CLIENT": "RAPTOR", "ALGO_PARENT": "ALGO", "ALGO_CHILD": "ALGO", "OR_PARENT": "OR", "OR_CHILD": "OR"}
#: The custom tag an in-system child carries its parent's OrderID in.
PARENT_TAG = "20001"
#: The scenario whose client order is cancel/replaced.
AMENDED_SCENARIO = "dma"
_STATUS_CODE = {name: code for code, name in states.FIX_STATUS_CODES.items()}


class _Clock:
    """One deterministic clock for every system: 13:30:00.000, +7 ms per message."""

    def __init__(self) -> None:
        self.ms = 0

    def tick(self) -> str:
        self.ms += 7
        seconds, ms = divmod(self.ms, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"20261009-{13 + hours:02d}:{30 + minutes:02d}:{seconds:02d}.{ms:03d}"


def _num(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def _cl_ord_id(order: MockOrder) -> str:
    """The ClOrdID the order's system reports: the id its *upstream* sent it with."""
    if order.level == "CLIENT":
        return order.alt_id
    if order.level in ("ALGO_PARENT", "OR_PARENT"):
        return order.link_id
    if order.level == "ALGO_CHILD":
        return order.order_id
    return "V" + order.order_id[3:]


def _state(order: MockOrder, when: str, cum: float, leaves: float, status: str,
           cl_ord_id: str, orig_cl_ord_id: str) -> str:
    """One order-state message: the order's complete state as a FIX body, ``|`` delimited."""
    avg = order.avg_px if cum > 0 else None
    fields: List[Tuple[str, str]] = [("35", "8"), ("37", order.order_id), ("11", cl_ord_id)]
    if orig_cl_ord_id:
        fields.append(("41", orig_cl_ord_id))
    if order.level == "CLIENT":
        fields.append(("1", order.moniker))
    if order.level in ("ALGO_CHILD", "OR_CHILD"):
        fields.append((PARENT_TAG, order.link_id))
    fields += [
        ("55", order.symbol), ("54", mockdata.SIDE_TO_FIX[order.side]), ("38", _num(order.qty)),
        ("14", _num(cum)), ("151", _num(leaves)), ("6", _num(avg) if avg is not None else "0"),
        ("39", _STATUS_CODE[status]), ("60", when),
    ]
    if order.level.startswith("ALGO") and order.destination:
        fields.append(("847", order.destination))  # TargetStrategy
    if order.level.startswith("OR") and order.destination:
        fields.append(("100", order.destination))  # ExDestination
    return "|".join(f"{tag}={value}" for tag, value in fields) + "|"


def _fill_split(order: MockOrder) -> Tuple[float, float]:
    """``(partial, final)`` cumulative quantities; ``partial`` is 0 when there is no partial."""
    if order.cum <= 0:
        return 0.0, 0.0
    half = float(int(order.cum / 2 / 100) * 100)
    return (half if 0 < half < order.cum else 0.0), order.cum


def send_order(data: MockData) -> List[Tuple[str, str]]:
    """Every ``(topic, message)`` in the order the systems send them (deterministic)."""
    clock = _Clock()
    out: List[Tuple[str, str]] = []
    orders = [order for root in data.roots for order in root.walk()]
    _, root_id = data.scenario_roots[AMENDED_SCENARIO]
    amended = next(o for o in orders if o.order_id == root_id)
    ids: Dict[str, Tuple[str, str]] = {o.order_id: (_cl_ord_id(o), "") for o in orders}

    def send(order: MockOrder, cum: float, leaves: float, status: str) -> None:
        cl, orig = ids[order.order_id]
        out.append((TOPIC_OF_LEVEL[order.level], _state(order, clock.tick(), cum, leaves, status, cl, orig)))

    for order in orders:  # 1. accepted
        send(order, 0.0, order.qty, "NEW")
    ids[amended.order_id] = (amended.alt_id + "-R1", amended.alt_id)  # 2. the client amends
    send(amended, 0.0, amended.qty, "NEW")
    for order in orders:  # 3. partial fills
        partial, _ = _fill_split(order)
        if partial:
            send(order, partial, max(order.qty - partial, 0.0), "PARTIALLY_FILLED")
    for order in orders:  # 4. the final fill: the order's booked CumQty / AvgPx
        _, final = _fill_split(order)
        if final:
            status = order.status if order.live else ("FILLED" if final >= order.qty else "PARTIALLY_FILLED")
            send(order, final, order.leaves if order.live else max(order.qty - final, 0.0), status)
    for order in orders:  # 5. cancels
        if not order.live:
            send(order, order.cum, 0.0, "CANCELED")
    return out


def messages(data: MockData) -> Dict[str, List[str]]:
    """``{topic: [message, ...]}`` in send order."""
    out: Dict[str, List[str]] = {topic: [] for topic in TOPIC_OF_LEVEL.values()}
    for topic, raw in send_order(data):
        out[topic].append(raw)
    return out


def events(data: MockData) -> Dict[str, List[Dict[str, Any]]]:
    """The messages as a recorded ``sow_and_subscribe``: ``UPSERT`` events per system."""
    out: Dict[str, List[Dict[str, Any]]] = {"RAPTOR": [], "ALGO": [], "OR": []}
    level_of_topic = {topic: level for level, topic in TOPIC_OF_LEVEL.items()}
    for topic, msgs in messages(data).items():
        system = _SYSTEM_OF_LEVEL[level_of_topic[topic]]
        for raw in msgs:
            out[system].append(fixstate.event(topic, raw, len(out[system]) + 1))
    return out


def config_json(obj: Dict[str, Any]) -> str:
    """A mapping config as JSON laid out for editing: one block per system, one line per level."""
    systems = obj["systems"]
    lines = ['{"systems": [']
    for i, system in enumerate(systems):
        head = {k: v for k, v in system.items() if k != "levels"}
        lines.append("  {" + json.dumps(head)[1:-1] + ",")
        lines.append('   "levels": [')
        levels = system["levels"]
        for j, level in enumerate(levels):
            lines.append("     " + json.dumps(level) + ("," if j < len(levels) - 1 else ""))
        lines.append("  ]}" + ("," if i < len(systems) - 1 else ""))
    lines.append("]}")
    return "\n".join(lines) + "\n"


def write_samples(out_dir: str, seed: int = config.DEFAULT_SEED,
                  families: int = config.DEFAULT_MOCK_FAMILIES) -> List[str]:
    """Write one ``<topic>.fix`` per SOW topic and ``fix_config.json`` into ``out_dir``."""
    os.makedirs(out_dir, exist_ok=True)
    written: List[str] = []
    contents = {f"{topic}.fix": "".join(m + "\n" for m in msgs)
                for topic, msgs in messages(mockdata.generate(seed, families)).items()}
    contents["fix_config.json"] = config_json(fixstate.DEFAULT_FIX_CONFIG)
    for name, content in contents.items():
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        written.append(path)
    return written


def publish(uri: str, data: Optional[MockData] = None, delay_ms: int = 0, client: Any = None,
            client_name: str = "otr-sample-publisher") -> int:
    """Publish every message, in send order and SOH-delimited, to its topic; returns the count."""
    if client is None:
        import AMPS

        client = AMPS.Client(client_name)
    client.connect(uri)
    client.logon()
    count = 0
    try:
        for topic, raw in send_order(data or mockdata.generate()):
            client.publish(topic, raw.replace("|", fixstate.SOH))
            count += 1
            if delay_ms:
                time.sleep(delay_ms / 1000.0)
        client.publish_flush()
    finally:
        client.close()
    return count


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Write, or publish to AMPS, FIX order-state messages for the mock book.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--out", help="output directory (e.g. fix-samples)")
    target.add_argument("--publish", metavar="URI", help="AMPS URI to publish to, e.g. tcp://amps:9007/amps/fix")
    parser.add_argument("--delay-ms", type=int, default=0, help="pause between published messages (default 0)")
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--families", type=int, default=config.DEFAULT_MOCK_FAMILIES)
    args = parser.parse_args(argv)
    if args.publish:
        count = publish(args.publish, mockdata.generate(args.seed, args.families), args.delay_ms)
        print(f"published {count} order-state messages to {args.publish}")
        return 0
    for path in write_samples(args.out, args.seed, args.families):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
