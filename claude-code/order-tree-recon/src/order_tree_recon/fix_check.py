"""Dry-run a FIX mapping on your logs, without Deephaven (doc 14 section 12.3).

::

    cd order-tree-recon
    PYTHONPATH=src python -m order_tree_recon.fix_check /path/to/logs [--config fix_config.json]

Loads the mapping exactly as ``OTR_SOURCE=fix-files`` does (``--config``, else
``<dir>/fix_config.json``, else the built-in default), reads every matching file, folds it
with the python twin of the Deephaven fold, and reconciles it with the reference. It prints
what matched, how many orders landed on each level, how they linked, the breaks, and the
first orphans with the ids they were looked up by. Pure stdlib: any Python 3.10+.

Exit status: 0 when it ran (breaks are data, not errors), 1 when the mapping is invalid or
no file matched, 2 on bad arguments.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from typing import Dict, List, Optional, Sequence, TextIO

from order_tree_recon import config, fix, reference

__all__ = ["check", "main"]


def check(directory: str, config_path: Optional[str] = None, out: TextIO = sys.stdout, examples: int = 5) -> int:
    """Run the dry run; returns the exit status (see the module docstring)."""
    topology = config.default_topology()
    if config_path is None:
        candidate = os.path.join(directory, "fix_config.json")
        config_path = candidate if os.path.isfile(candidate) else ""
    try:
        fix_config = fix.load_fix_config(config_path, topology)
    except (OSError, ValueError) as exc:
        print(f"mapping: INVALID -- {exc}", file=out)
        return 1
    print(f"mapping: {config_path or '(built-in DEFAULT_FIX_CONFIG)'}", file=out)

    tapes: Dict[str, List[str]] = {}
    any_file = False
    for system in fix_config.systems:
        messages, files = fix.read_fix_files(system.files, directory)
        any_file = any_file or bool(files)
        tapes[system.system] = messages
        reports = sum(1 for m in messages if fix.pick_values(m, ["35"])[0] == "8")
        names = ", ".join(os.path.basename(f) for f in files) or "NO FILES MATCHED"
        print(f"  {system.system:<8} {list(system.files)} -> {names}: "
              f"{len(messages)} messages, {reports} ExecutionReports", file=out)
    if not any_file:
        print(f"no file in {directory} matches any 'files' glob", file=out)
        return 1

    rows, ids = fix.fold_all(fix_config, tapes)
    nodes = reference.reconcile(topology, rows, config.qty_tolerance(), extra_ids=ids)
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

    trees = sum(1 for n in nodes.values() if n["Depth"] == 0 and n["LinkState"] == "ROOT")
    print(f"client trees: {trees}; monikers: {len({n['RootMoniker'] for n in nodes.values() if n['RootMoniker']})}",
          file=out)
    link_tags = {rule.level: rule.link_tag for system in fix_config.systems for rule in system.levels}
    unlinked = [n for n in nodes.values() if n["LinkState"] in ("ORPHAN", "NO_LINK")]
    for node in unlinked[:examples]:
        parents = " / ".join(topology[node["Level"]].parents)
        if node["LinkId"]:
            how = f"link tag {link_tags[node['Level']]} = {node['LinkId']!r}, not an id of any {parents} order"
        else:
            how = f"link tag {link_tags[node['Level']]} is on none of its ExecutionReports"
        print(f"  {node['LinkState']:<8} {node['NodeKey']} ({node['Level']}): {how}", file=out)
    if len(unlinked) > examples:
        print(f"  ... and {len(unlinked) - examples} more", file=out)
    if unlinked and links.get("LINKED", 0) == 0:
        print("hint: nothing linked -- check each level's link_tag against what the upstream "
              "system reports in its id_tags (37 / 11 / 41)", file=out)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run a FIX mapping on drop-copy logs (no Deephaven needed).")
    parser.add_argument("directory", help="the folder with the logs (what OTR_FIX_HOST_DIR will point at)")
    parser.add_argument("--config", default=None, help="mapping JSON (default: <directory>/fix_config.json)")
    parser.add_argument("--examples", type=int, default=5, help="unlinked orders to list (default 5)")
    args = parser.parse_args(argv)
    if not os.path.isdir(args.directory):
        parser.error(f"{args.directory} is not a directory")
    return check(args.directory, args.config, examples=args.examples)


if __name__ == "__main__":
    raise SystemExit(main())
