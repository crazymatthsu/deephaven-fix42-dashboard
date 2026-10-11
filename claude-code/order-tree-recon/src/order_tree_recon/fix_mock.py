"""FIX 4.2 drop-copy tapes for the mock book -- what ``fix-samples/`` holds (doc 14 section 12).

Each mock order (:mod:`order_tree_recon.mockdata`) becomes the ExecutionReports its system
would have published, so the FIX path can be tested against the table path on the very
same book: folding the tapes (:func:`order_tree_recon.fix.fold_all`) and reconciling gives
the table path's ``otr_recon`` row for row -- except ``AltId``, which a tape fills with the
latest ClOrdID for every system.

The tapes follow :data:`order_tree_recon.fix.DEFAULT_FIX_CONFIG`, and on purpose exercise
what real drop copies do:

* a QuickFIX-style log prefix (``20261009-13:30:00.007 : 8=FIX.4.2|...``), heartbeats and
  inbound NewOrderSingles (35=D) between the ExecutionReports -- all of it skipped;
* the moniker (1), the in-system parent tag (20001) and the destination only on the
  **first** report -- the fold keeps the first non-empty value;
* ClOrdID (11) as the cross-system link: an algo parent's 11 is the Raptor OrderID it
  was sent with, a router parent's 11 the algo child's id -- or, for DMA flow, the
  client's ClOrdID that Raptor passed through;
* a cancel/replace of the ``dma`` client order (new 11, 41 = the old one) that the router
  also applies, so the router parent's *original* ClOrdID only resolves through Raptor's
  id history (``extra_ids``);
* a partial and a final fill, then a cancel for every order the scenario killed;
* correct BodyLength (9) and CheckSum (10), computed on the SOH form.

Regenerate the committed samples with::

    python -m order_tree_recon.fix_mock --out fix-samples
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from order_tree_recon import config, fix, mockdata
from order_tree_recon.mockdata import MockData, MockOrder

__all__ = [
    "SOH",
    "TAPE_FILES",
    "PARENT_TAG",
    "AMENDED_SCENARIO",
    "fix_message",
    "checksum_ok",
    "tapes",
    "write_samples",
    "config_json",
    "main",
]

SOH = "\x01"
#: System -> the file its tape is written to (matched by DEFAULT_FIX_CONFIG's globs).
TAPE_FILES: Dict[str, str] = {"RAPTOR": "raptor.log", "ALGO": "algo.log", "OR": "router.log"}
#: The custom tag an in-system child carries its parent's OrderID in.
PARENT_TAG = "20001"
#: The scenario whose client order is cancel/replaced (and the router parent with it).
AMENDED_SCENARIO = "dma"

_SYSTEM_OF = {"CLIENT": "RAPTOR", "ALGO_PARENT": "ALGO", "ALGO_CHILD": "ALGO", "OR_PARENT": "OR", "OR_CHILD": "OR"}
_STATUS_CODE = {name: code for code, name in fix.ORD_STATUS_NAMES.items()}


def fix_message(fields: Sequence[Tuple[str, str]], delimiter: str = "|") -> str:
    """A FIX 4.2 message from ``(tag, value)`` pairs (MsgType first), with 8 / 9 / 10 added."""
    body = "".join(f"{tag}={value}{SOH}" for tag, value in fields)
    head = f"8=FIX.4.2{SOH}9={len(body.encode('ascii'))}{SOH}"
    total = sum((head + body).encode("ascii")) % 256
    message = f"{head}{body}10={total:03d}{SOH}"
    return message if delimiter == SOH else message.replace(SOH, delimiter)


def checksum_ok(raw: str) -> bool:
    """Whether a message (any delimiter, log prefix allowed) has a correct BodyLength and CheckSum."""
    text = raw.strip()
    text = text[text.find("8=FIX"):]
    delimiter = SOH if SOH in text else "|"
    text = text.replace(delimiter, SOH)
    if not text.endswith(SOH):
        text += SOH
    head_end = text.index(SOH, text.index(SOH) + 1) + 1  # after "9=N<SOH>"
    trailer = text.rindex(SOH + "10=") + 1
    declared_length = int(text.split(SOH)[1].partition("=")[2])
    declared_sum = int(text[trailer + 3:].rstrip(SOH))
    return declared_length == len(text[head_end:trailer].encode("ascii")) and \
        declared_sum == sum(text[:trailer].encode("ascii")) % 256


def _num(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


class _Clock:
    """One deterministic clock for every tape: 13:30:00.000, +7 ms per message."""

    def __init__(self) -> None:
        self.ms = 0

    def tick(self) -> str:
        self.ms += 7
        seconds, ms = divmod(self.ms, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"20261009-{13 + hours:02d}:{30 + minutes:02d}:{seconds:02d}.{ms:03d}"


@dataclass
class _Tape:
    system: str
    clock: _Clock
    delimiter: str
    lines: List[str] = field(default_factory=list)
    seq: int = 0
    execs: int = 0

    def emit(self, msg_type: str, fields: Sequence[Tuple[str, str]]) -> None:
        self.seq += 1
        when = self.clock.tick()
        header = [("35", msg_type), ("49", self.system), ("56", "DROPCOPY"), ("34", str(self.seq)), ("52", when)]
        self.lines.append(f"{when} : {fix_message(header + list(fields), self.delimiter)}")

    def report(self, order: MockOrder, cl_ord_id: str, exec_type: str, status: str, cum: float,
               leaves: float, avg: Optional[float], extra: Sequence[Tuple[str, str]] = ()) -> None:
        self.execs += 1
        fields = [
            ("37", order.order_id), ("11", cl_ord_id), ("17", f"{self.system}-E{self.execs:06d}"),
            ("20", "0"), ("150", exec_type), ("39", _STATUS_CODE[status]),
            ("55", order.symbol), ("54", mockdata.SIDE_TO_FIX[order.side]),
            ("38", _num(order.qty)), ("14", _num(cum)), ("151", _num(leaves)),
            ("6", _num(avg) if avg is not None else "0"),
        ]
        self.emit("8", fields + list(extra))


def _cl_ord_id(order: MockOrder) -> str:
    """The ClOrdID the order's system reports: the id its *upstream* sent it with."""
    if order.level == "CLIENT":
        return order.alt_id  # the client's own ClOrdID
    if order.level in ("ALGO_PARENT", "OR_PARENT"):
        return order.link_id  # Raptor's OrderID / the algo child's id / the DMA client ClOrdID
    if order.level == "ALGO_CHILD":
        return order.order_id  # the algo sends its child id as the ClOrdID to the router
    return "V" + order.order_id[3:]  # router -> venue: the router's own outbound ClOrdID


def _first_report_tags(order: MockOrder) -> List[Tuple[str, str]]:
    """Tags only the first ExecutionReport carries (the fold keeps first non-empty values)."""
    tags: List[Tuple[str, str]] = []
    if order.level == "CLIENT":
        tags.append(("1", order.moniker))
    if order.level in ("ALGO_CHILD", "OR_CHILD"):
        tags.append((PARENT_TAG, order.link_id))
    if order.level.startswith("ALGO") and order.destination:
        tags.append(("847", order.destination))  # TargetStrategy
    if order.level.startswith("OR") and order.destination:
        tags.append(("100", order.destination))  # ExDestination
    return tags


def _fill_split(order: MockOrder) -> Tuple[float, float]:
    """``(partial, final)`` cumulative quantities; ``partial`` is 0 when there is no partial."""
    if order.cum <= 0:
        return 0.0, 0.0
    half = float(int(order.cum / 2 / 100) * 100)
    return (half if 0 < half < order.cum else 0.0), order.cum


def tapes(data: MockData, delimiter: str = "|") -> Dict[str, List[str]]:
    """``{system: log lines}`` for every order in ``data`` (deterministic)."""
    clock = _Clock()
    out = {system: _Tape(system, clock, delimiter) for system in TAPE_FILES}
    for tape in out.values():
        tape.emit("0", [])  # a heartbeat: not an ExecutionReport, skipped by the fold
    orders = [order for root in data.roots for order in root.walk()]
    amended: Dict[str, str] = {}
    _, root_id = data.scenario_roots[AMENDED_SCENARIO]
    amended_root = next(o for o in orders if o.order_id == root_id)
    amended[root_id] = amended_root.alt_id + "-R1"
    for child in amended_root.walk():
        if child.level == "OR_PARENT" and child.link_id == amended_root.alt_id:
            amended[child.order_id] = amended[root_id]

    # Phase 1: the client NewOrderSingle (Raptor only), then the ack of every order.
    for order in orders:
        tape = out[_SYSTEM_OF[order.level]]
        if order.level == "CLIENT":
            tape.emit("D", [("11", order.alt_id), ("1", order.moniker), ("21", "1"), ("55", order.symbol),
                            ("54", mockdata.SIDE_TO_FIX[order.side]), ("38", _num(order.qty)), ("40", "1")])
        tape.report(order, _cl_ord_id(order), "0", "NEW", 0.0, order.qty, None, _first_report_tags(order))
    # Phase 2: the cancel/replace -- a new ClOrdID, OrigClOrdID = the old one.
    for order in orders:
        if order.order_id in amended:
            old = _cl_ord_id(order)
            out[_SYSTEM_OF[order.level]].report(order, amended[order.order_id], "5", "NEW", 0.0, order.qty, None,
                                                [("41", old)])
    current = {o.order_id: amended.get(o.order_id, _cl_ord_id(o)) for o in orders}
    # Phase 3: partial fills.
    for order in orders:
        partial, _ = _fill_split(order)
        if partial:
            out[_SYSTEM_OF[order.level]].report(order, current[order.order_id], "1", "PARTIALLY_FILLED", partial,
                                                max(order.qty - partial, 0.0), order.avg_px,
                                                [("32", _num(partial)), ("31", _num(order.avg_px or 0.0))])
    # Phase 4: the final fill, at the order's booked CumQty / AvgPx.
    for order in orders:
        partial, final = _fill_split(order)
        if final:
            status = "FILLED" if final >= order.qty else "PARTIALLY_FILLED"
            out[_SYSTEM_OF[order.level]].report(order, current[order.order_id], "2" if status == "FILLED" else "1",
                                                status, final, max(order.qty - final, 0.0), order.avg_px,
                                                [("32", _num(final - partial)), ("31", _num(order.avg_px or 0.0))])
    # Phase 5: cancels for every order the scenario killed.
    for order in orders:
        if not order.live:
            out[_SYSTEM_OF[order.level]].report(order, current[order.order_id], "4", "CANCELED", order.cum, 0.0,
                                                order.avg_px)
    return {system: tape.lines for system, tape in out.items()}


def write_samples(out_dir: str, seed: int = config.DEFAULT_SEED, families: int = config.DEFAULT_MOCK_FAMILIES,
                  delimiter: str = "|") -> List[str]:
    """Write ``raptor.log``, ``algo.log``, ``router.log`` and ``fix_config.json`` into ``out_dir``."""
    os.makedirs(out_dir, exist_ok=True)
    written: List[str] = []
    for system, lines in tapes(mockdata.generate(seed, families), delimiter).items():
        path = os.path.join(out_dir, TAPE_FILES[system])
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(lines) + "\n")
        written.append(path)
    path = os.path.join(out_dir, "fix_config.json")
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(config_json(fix.DEFAULT_FIX_CONFIG))
    written.append(path)
    return written


def config_json(obj: Dict[str, Any]) -> str:
    """A mapping config as JSON laid out for editing: one line per system header and per level."""
    systems = obj["systems"]
    lines = ['{"systems": [']
    for i, system in enumerate(systems):
        head = json.dumps({k: v for k, v in system.items() if k != "levels"})[1:-1]
        lines.append(f'  {{{head}, "levels": [')
        levels = system["levels"]
        for j, level in enumerate(levels):
            lines.append("    " + json.dumps(level) + ("," if j < len(levels) - 1 else ""))
        lines.append("  ]}" + ("," if i < len(systems) - 1 else ""))
    lines.append("]}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Write FIX 4.2 drop-copy tapes for the mock order book.")
    parser.add_argument("--out", required=True, help="output directory (e.g. fix-samples)")
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--families", type=int, default=config.DEFAULT_MOCK_FAMILIES)
    parser.add_argument("--delimiter", choices=("pipe", "soh"), default="pipe",
                        help="field delimiter: '|' (readable, the default) or SOH (what an engine writes)")
    args = parser.parse_args(argv)
    for path in write_samples(args.out, args.seed, args.families, "|" if args.delimiter == "pipe" else SOH):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
