"""Order-state message files for the mock book -- what ``state-samples/`` holds (doc 14 section 12).

Each mock order (:mod:`order_tree_recon.mockdata`) becomes the order-state messages its
system would have sent over the day, in that system's own field names: a ``NEW`` state,
a partial fill, the final fill and, for every order the scenario killed, a ``CANCELED``
state. The last message of each order *is* the mock table row, so the state path can be
tested against the table path on the same book: folding the files
(:func:`order_tree_recon.states.fold_all`) and reconciling gives the table path's
``otr_recon`` row for row -- except the one amended ``AltId`` below.

The files follow :data:`order_tree_recon.states.DEFAULT_STATE_CONFIG` and on purpose show
what real feeds do:

* three formats and coding styles: ``raptor.csv`` (FIX side codes, arrival order),
  ``algo.jsonl`` (JSON Lines, a numeric ``Version`` sequence, optional fields left out),
  ``router.csv`` (an ISO-8601 ``UpdateTime`` sequence);
* the ``dma`` client order's ClOrdID is amended (``-R1``) while the router parent keeps
  the original it was sent with, so that link only resolves through Raptor's id history.

Regenerate the committed samples with::

    python -m order_tree_recon.states_mock --out state-samples
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from order_tree_recon import config, mockdata, states
from order_tree_recon.mockdata import MockData, MockOrder

__all__ = ["SAMPLE_FILES", "AMENDED_SCENARIO", "messages", "write_samples", "config_json", "main"]

#: System -> the file its messages are written to (matched by DEFAULT_STATE_CONFIG's globs).
SAMPLE_FILES: Dict[str, str] = {"RAPTOR": "raptor.csv", "ALGO": "algo.jsonl", "OR": "router.csv"}
#: The scenario whose client order gets a new ClOrdID during the day.
AMENDED_SCENARIO = "dma"

_RAPTOR_COLUMNS = ("UpdateTime", "OrderId", "ClOrdID", "Moniker", "Symbol", "Side", "OrderQty", "CumQty",
                   "LeavesQty", "AvgPx", "OrdStatus")
_ROUTER_COLUMNS = ("UpdateTime", "RouterOrderId", "ParentRouterOrderId", "UpstreamOrderId", "Venue", "Symbol",
                   "Side", "Qty", "CumQty", "LeavesQty", "AvgPx", "Status")
_SYSTEM_OF = {"CLIENT": "RAPTOR", "ALGO_PARENT": "ALGO", "ALGO_CHILD": "ALGO", "OR_PARENT": "OR", "OR_CHILD": "OR"}


class _Clock:
    """One deterministic clock for every system: 13:30:00.000, +7 ms per message."""

    def __init__(self) -> None:
        self.ms = 0

    def tick(self) -> str:
        self.ms += 7
        seconds, ms = divmod(self.ms, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"2026-10-09T{13 + hours:02d}:{30 + minutes:02d}:{seconds:02d}.{ms:03d}"


def _px(value: Optional[float]) -> str:
    return "" if value is None else repr(float(value))


def _state(order: MockOrder, when: str, version: int, cum: float, leaves: float, status: str,
           cl_ord_id: Optional[str]) -> Dict[str, Any]:
    """One order-state message in the order's own system's field names."""
    avg = order.avg_px if cum > 0 else None
    if order.level == "CLIENT":
        return {
            "UpdateTime": when, "OrderId": order.order_id, "ClOrdID": cl_ord_id or order.alt_id,
            "Moniker": order.moniker, "Symbol": order.symbol, "Side": mockdata.SIDE_TO_FIX[order.side],
            "OrderQty": int(order.qty), "CumQty": int(cum), "LeavesQty": int(leaves), "AvgPx": _px(avg),
            "OrdStatus": status,
        }
    if order.level in ("ALGO_PARENT", "ALGO_CHILD"):
        msg: Dict[str, Any] = {"Version": version, "AlgoOrderId": order.order_id}
        # JSON feeds leave fields out: a parent sends no parent id, a child no Raptor id.
        if order.level == "ALGO_PARENT":
            msg["RaptorOrderId"] = order.link_id
        else:
            msg["ParentAlgoOrderId"] = order.link_id
        msg.update({
            "Strategy": order.destination, "Symbol": order.symbol, "Side": order.side, "Qty": int(order.qty),
            "Filled": int(cum), "Open": int(leaves), "AvgPrice": avg, "State": status,
        })
        return msg
    is_parent = order.level == "OR_PARENT"
    return {
        "UpdateTime": when, "RouterOrderId": order.order_id,
        "ParentRouterOrderId": "" if is_parent else order.link_id,
        "UpstreamOrderId": order.link_id if is_parent else "",
        "Venue": order.destination, "Symbol": order.symbol, "Side": order.side, "Qty": int(order.qty),
        "CumQty": int(cum), "LeavesQty": int(leaves), "AvgPx": _px(avg), "Status": status,
    }


def _fill_split(order: MockOrder) -> Tuple[float, float]:
    """``(partial, final)`` cumulative quantities; ``partial`` is 0 when there is no partial."""
    if order.cum <= 0:
        return 0.0, 0.0
    half = float(int(order.cum / 2 / 100) * 100)
    return (half if 0 < half < order.cum else 0.0), order.cum


def messages(data: MockData) -> Dict[str, List[Dict[str, Any]]]:
    """``{system: [message, ...]}`` in send order, for every order in ``data`` (deterministic)."""
    clock = _Clock()
    versions = {system: 0 for system in SAMPLE_FILES}
    out: Dict[str, List[Dict[str, Any]]] = {system: [] for system in SAMPLE_FILES}
    orders = [order for root in data.roots for order in root.walk()]
    _, root_id = data.scenario_roots[AMENDED_SCENARIO]
    amended = next(o for o in orders if o.order_id == root_id)
    cl_ord_ids: Dict[str, str] = {}

    def send(order: MockOrder, cum: float, leaves: float, status: str) -> None:
        system = _SYSTEM_OF[order.level]
        versions[system] += 1
        out[system].append(_state(order, clock.tick(), versions[system], cum, leaves, status,
                                  cl_ord_ids.get(order.order_id)))

    for order in orders:  # 1. accepted
        send(order, 0.0, order.qty, "NEW")
    cl_ord_ids[amended.order_id] = amended.alt_id + "-R1"  # 2. the client amends; the router keeps the old id
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


def _csv(rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def config_json(obj: Dict[str, Any]) -> str:
    """A mapping config as JSON laid out for editing: one block per system, one line per level."""
    systems = obj["systems"]
    lines = ['{"systems": [']
    for i, system in enumerate(systems):
        head = {k: v for k, v in system.items() if k not in ("fields", "levels")}
        lines.append("  {" + json.dumps(head)[1:-1] + ",")
        lines.append('   "fields": ' + json.dumps(system.get("fields", {})) + ",")
        lines.append('   "levels": [')
        levels = system["levels"]
        for j, level in enumerate(levels):
            lines.append("     " + json.dumps(level) + ("," if j < len(levels) - 1 else ""))
        lines.append("  ]}" + ("," if i < len(systems) - 1 else ""))
    lines.append("]}")
    return "\n".join(lines) + "\n"


def write_samples(out_dir: str, seed: int = config.DEFAULT_SEED,
                  families: int = config.DEFAULT_MOCK_FAMILIES) -> List[str]:
    """Write ``raptor.csv``, ``algo.jsonl``, ``router.csv`` and ``state_config.json`` into ``out_dir``."""
    os.makedirs(out_dir, exist_ok=True)
    by_system = messages(mockdata.generate(seed, families))
    contents = {
        "raptor.csv": _csv(by_system["RAPTOR"], _RAPTOR_COLUMNS),
        "algo.jsonl": "".join(json.dumps(m) + "\n" for m in by_system["ALGO"]),
        "router.csv": _csv(by_system["OR"], _ROUTER_COLUMNS),
        "state_config.json": config_json(states.DEFAULT_STATE_CONFIG),
    }
    written: List[str] = []
    for name, content in contents.items():
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        written.append(path)
    return written


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Write order-state message files for the mock order book.")
    parser.add_argument("--out", required=True, help="output directory (e.g. state-samples)")
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--families", type=int, default=config.DEFAULT_MOCK_FAMILIES)
    args = parser.parse_args(argv)
    for path in write_samples(args.out, args.seed, args.families):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
