"""Dry-run an order-state mapping on your files, without Deephaven (doc 14 section 12.3).

::

    cd order-tree-recon
    PYTHONPATH=src python -m order_tree_recon.states_check /path/to/files [--config state_config.json]

Loads the mapping exactly as ``OTR_SOURCE=state-files`` does (``--config``, else
``<dir>/state_config.json``, else the built-in default), reads every matching file, folds
it with the python twin of the Deephaven fold and reconciles it with the reference. It
prints what each glob matched and the columns found, mapped fields no file has, how many
orders landed on each level, how they linked, the breaks, and the first unlinked orders
with the id they carried. Pure stdlib: any Python 3.10+.

Exit status: 0 when it ran (breaks are data, not errors), 1 when the mapping is invalid or
no file matched, 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from typing import Dict, List, Mapping, Optional, Sequence, TextIO

from order_tree_recon import config, reference, states

__all__ = ["check", "main"]


def check(directory: str, config_path: Optional[str] = None, out: TextIO = sys.stdout, examples: int = 5) -> int:
    """Run the dry run; returns the exit status (see the module docstring)."""
    topology = config.default_topology()
    if config_path is None:
        candidate = os.path.join(directory, "state_config.json")
        config_path = candidate if os.path.isfile(candidate) else ""
    try:
        state_config = states.load_state_config(config_path, topology)
    except (OSError, ValueError) as exc:
        print(f"mapping: INVALID -- {exc}", file=out)
        return 1
    print(f"mapping: {config_path or '(built-in DEFAULT_STATE_CONFIG)'}", file=out)

    messages: Dict[str, List[Dict[str, str]]] = {}
    any_file = False
    for system in state_config.systems:
        try:
            rows, columns, files, bad = states.read_state_files(system.files, directory)
        except ValueError as exc:
            print(f"  {system.system:<8} ERROR -- {exc}", file=out)
            return 1
        any_file = any_file or bool(files)
        messages[system.system] = rows
        names = ", ".join(os.path.basename(f) for f in files) or "NO FILES MATCHED"
        print(f"  {system.system:<8} {list(system.files)} -> {names}: {len(rows)} messages"
              + (f", {bad} unusable lines" if bad else ""), file=out)
        if files:
            print(f"           columns: {', '.join(columns)}", file=out)
            absent = [name for name in system.field_names if name not in columns]
            if absent:
                print(f"           MISSING: mapped field(s) {absent} are in no file -- check the names", file=out)
    if not any_file:
        print(f"no file in {directory} matches any 'files' glob", file=out)
        return 1

    rows, ids = states.fold_all(state_config, messages)
    nodes = reference.reconcile(topology, rows, config.qty_tolerance(), extra_ids=ids)
    _summary(topology, state_config, nodes, out, examples)
    return 0


def _summary(topology: config.Topology, state_config: states.StateConfig,
             nodes: Mapping[str, Mapping[str, object]], out: TextIO, examples: int) -> None:
    per_level = Counter(n["Level"] for n in nodes.values())
    print("orders per level: " + " | ".join(f"{lvl} {per_level.get(lvl, 0)}" for lvl in topology.names), file=out)
    links = Counter(n["LinkState"] for n in nodes.values())
    print("links: " + ", ".join(f"{state} {links.get(state, 0)}" for state in config.LINK_STATES), file=out)
    breaks = Counter((n["BreakKind"], n["Level"]) for n in nodes.values() if n["BreakKind"] != "NONE")
    if breaks:
        print("breaks: " + ", ".join(f"{kind} x{count} ({level})" for (kind, level), count in sorted(breaks.items())),
              file=out)
    else:
        print("breaks: none", file=out)
    trees = sum(1 for n in nodes.values() if n["LinkState"] == "ROOT")
    monikers = {n["RootMoniker"] for n in nodes.values() if n["RootMoniker"]}
    print(f"client trees: {trees}; monikers: {len(monikers)}", file=out)

    link_fields = {rule.level: rule.link for system in state_config.systems for rule in system.levels}
    unlinked = [n for n in nodes.values() if n["LinkState"] in ("ORPHAN", "NO_LINK")]
    for node in unlinked[:examples]:
        parents = " / ".join(topology[str(node["Level"])].parents)
        field = link_fields[str(node["Level"])]
        if node["LinkId"]:
            how = f"{field} = {node['LinkId']!r}, not an id of any {parents} order"
        else:
            how = f"{field} is empty in its latest state"
        print(f"  {node['LinkState']:<8} {node['NodeKey']} ({node['Level']}): {how}", file=out)
    if len(unlinked) > examples:
        print(f"  ... and {len(unlinked) - examples} more", file=out)
    if unlinked and links.get("LINKED", 0) == 0:
        print("hint: nothing linked -- each level's 'link' must hold an id the parent system sends "
              "in its order_id / alt_id / id_fields", file=out)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run an order-state mapping on message files (no Deephaven).")
    parser.add_argument("directory", help="the folder with the files (what OTR_STATE_HOST_DIR will point at)")
    parser.add_argument("--config", default=None, help="mapping JSON (default: <directory>/state_config.json)")
    parser.add_argument("--examples", type=int, default=5, help="unlinked orders to list (default 5)")
    args = parser.parse_args(argv)
    if not os.path.isdir(args.directory):
        parser.error(f"{args.directory} is not a directory")
    return check(args.directory, args.config, examples=args.examples)


if __name__ == "__main__":
    raise SystemExit(main())
