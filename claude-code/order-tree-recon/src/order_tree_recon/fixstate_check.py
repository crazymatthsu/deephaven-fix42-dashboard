"""Dry-run a FIX order-state mapping without Deephaven (doc 14 section 12.3).

Against files (SOW dumps or recorded subscriptions, one FIX message per line)::

    cd order-tree-recon
    PYTHONPATH=src python -m order_tree_recon.fixstate_check --dir /path/to/files

Against your AMPS server -- a one-off ``sow`` query on every configured topic, nothing is
subscribed or changed (needs ``pip install amps-python-client``)::

    PYTHONPATH=src python -m order_tree_recon.fixstate_check --amps tcp://amps-host:9007/amps/fix \\
        --config /path/to/fix_config.json

It loads the mapping as the app does (``--config``, else ``<dir>/fix_config.json``, else the
built-in default), folds the messages with the python twin of the Deephaven fold, reconciles
them with the reference, and prints: what each topic / file delivered, mapped tags that no
message carries, orders per level, how they linked, the breaks, and the first unlinked
orders with the id they carried.

Exit status: 0 when it ran (breaks are data, not errors), 1 when the mapping is invalid or
nothing was read, 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, TextIO

from order_tree_recon import config, fixstate, reference, states
from order_tree_recon.fixstate import FixStateConfig

__all__ = ["check", "amps_sow_events", "main"]

#: The tags whose absence from every message means the mapping is wrong (not just sparse).
_ESSENTIAL = ("Symbol", "Side", "OrderQty", "CumQty", "LeavesQty")


def amps_sow_events(fix_config: FixStateConfig, uri: str, client_name: str = "otr-fixstate-check",
                    timeout_ms: int = 30_000, client_factory: Optional[Callable[[str], Any]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """One ``sow`` query per configured topic -> ``UPSERT`` events per system."""
    import AMPS

    client = client_factory(client_name) if client_factory else AMPS.Client(client_name)
    client.connect(uri)
    client.logon()
    events: Dict[str, List[Dict[str, Any]]] = {}
    try:
        for system in fix_config.systems:
            rows: List[Dict[str, Any]] = []
            for topic in fix_config.topics.get(system.system, ()):
                command = AMPS.Command("sow").set_topic(topic).set_batch_size(1000).set_timeout(timeout_ms)
                flt = fix_config.filters.get(system.system)
                if flt:
                    command = command.set_filter(flt)
                for message in client.execute(command):
                    if str(message.get_command() or "") == "sow":
                        rows.append(fixstate.event(topic, str(message.get_data() or ""), len(rows) + 1,
                                                   sow_key=str(message.get_sow_key() or "")))
            events[system.system] = rows
    finally:
        client.close()
    return events


def _report_sources(fix_config: FixStateConfig, events: Mapping[str, Sequence[Mapping[str, Any]]], out: TextIO) -> None:
    for system in fix_config.systems:
        rows = events.get(system.system, [])
        per_topic = Counter(str(e["Topic"]) for e in rows)
        listed = ", ".join(f"{t}: {n}" for t, n in per_topic.items()) or "NOTHING"
        print(f"  {system.system:<8} {listed}", file=out)
        if not rows:
            continue
        parsed = [fixstate.parse_fix(e["RawFix"]) for e in rows]
        roles: Dict[str, str] = {fixstate.tag_of(system.order_id): "order id"}
        for rule in system.levels:
            if rule.link:
                roles.setdefault(fixstate.tag_of(rule.link), f"parent link of {rule.level}")
            if rule.when:
                roles.setdefault(fixstate.tag_of(rule.when.field), f"'when' of {rule.level}")
        for target in _ESSENTIAL:
            source = system.source(target)
            if source:
                roles.setdefault(fixstate.tag_of(source), target)
        for tag, role in roles.items():
            present = sum(1 for p in parsed if p.get(tag, "").strip())
            if present == 0:
                print(f"           MISSING: tag {tag} ({role}) is in none of the {len(parsed)} messages", file=out)


def _summary(topology: config.Topology, fix_config: FixStateConfig,
             nodes: Mapping[str, Mapping[str, Any]], out: TextIO, examples: int) -> None:
    per_level = Counter(n["Level"] for n in nodes.values())
    print("orders per level: " + " | ".join(f"{lvl} {per_level.get(lvl, 0)}" for lvl in topology.names), file=out)
    links = Counter(n["LinkState"] for n in nodes.values())
    print("links: " + ", ".join(f"{state} {links.get(state, 0)}" for state in config.LINK_STATES), file=out)
    breaks = Counter((n["BreakKind"], n["Level"]) for n in nodes.values() if n["BreakKind"] != "NONE")
    print("breaks: " + (", ".join(f"{kind} x{count} ({level})" for (kind, level), count in sorted(breaks.items()))
                        or "none"), file=out)
    monikers = {n["RootMoniker"] for n in nodes.values() if n["RootMoniker"]}
    print(f"client trees: {links.get('ROOT', 0)}; monikers: {len(monikers)}", file=out)
    link_tags = {rule.level: fixstate.tag_of(rule.link) for s in fix_config.systems for rule in s.levels if rule.link}
    unlinked = [n for n in nodes.values() if n["LinkState"] in ("ORPHAN", "NO_LINK")]
    for node in unlinked[:examples]:
        parents = " / ".join(topology[str(node["Level"])].parents)
        tag = link_tags[str(node["Level"])]
        how = (f"tag {tag} = {node['LinkId']!r}, not an id of any {parents} order" if node["LinkId"]
               else f"tag {tag} is not in its latest state")
        print(f"  {node['LinkState']:<8} {node['NodeKey']} ({node['Level']}): {how}", file=out)
    if len(unlinked) > examples:
        print(f"  ... and {len(unlinked) - examples} more", file=out)
    if unlinked and links.get("LINKED", 0) == 0:
        print("hint: nothing linked -- each level's link_tag must hold an id the parent system reports "
              "in its id_tags (37 / 11 / 41 by default)", file=out)


def check(directory: Optional[str] = None, config_path: Optional[str] = None, amps_uri: Optional[str] = None,
          out: TextIO = sys.stdout, examples: int = 5,
          amps_events: Callable[..., Dict[str, List[Dict[str, Any]]]] = amps_sow_events) -> int:
    """Run the dry run on ``directory`` files or an ``amps_uri`` SOW query; returns the exit status."""
    topology = config.default_topology()
    if config_path is None and directory:
        candidate = os.path.join(directory, "fix_config.json")
        config_path = candidate if os.path.isfile(candidate) else ""
    try:
        fix_config = fixstate.load_fix_config(config_path, topology)
    except (OSError, ValueError) as exc:
        print(f"mapping: INVALID -- {exc}", file=out)
        return 1
    print(f"mapping: {config_path or '(built-in DEFAULT_FIX_CONFIG)'}", file=out)

    if amps_uri:
        print(f"source: AMPS sow query on {amps_uri}", file=out)
        try:
            events = amps_events(fix_config, amps_uri)
        except Exception as exc:  # noqa: BLE001 - report, don't trace
            print(f"AMPS: FAILED -- {type(exc).__name__}: {exc}", file=out)
            return 1
    else:
        events, files = fixstate.file_events(fix_config, directory or ".")
        print(f"source: files in {directory}", file=out)
        for system in fix_config.systems:
            names = ", ".join(os.path.basename(f) for f in files[system.system]) or "NO FILES MATCHED"
            print(f"  {system.system:<8} {list(fix_config.files[system.system])} -> {names}", file=out)
    if not any(events.values()):
        print("nothing was read: no message for any system", file=out)
        return 1
    print("messages per topic:", file=out)
    _report_sources(fix_config, events, out)

    rows, ids = fixstate.fold_events(fix_config, events)
    nodes = reference.reconcile(topology, rows, config.qty_tolerance(), extra_ids=ids)
    _summary(topology, fix_config, nodes, out, examples)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run a FIX order-state mapping (no Deephaven needed).")
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--dir", help="folder with the message files (and, by default, fix_config.json)")
    where.add_argument("--amps", metavar="URI", help="AMPS URI to run a one-off sow query on, e.g. tcp://host:9007/amps/fix")
    parser.add_argument("--config", default=None, help="mapping JSON (default: <dir>/fix_config.json, else built-in)")
    parser.add_argument("--examples", type=int, default=5, help="unlinked orders to list (default 5)")
    args = parser.parse_args(argv)
    if args.dir and not os.path.isdir(args.dir):
        parser.error(f"{args.dir} is not a directory")
    return check(args.dir, args.config, args.amps, examples=args.examples)


if __name__ == "__main__":
    raise SystemExit(main())
