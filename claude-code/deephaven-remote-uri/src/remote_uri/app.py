"""Entry point: role dispatch, memoised runtime, global export, banner -- doc 10 section 4.

Works both in **Application Mode** (``docker/apps/remote-uri-leaf`` and
``docker/apps/remote-uri-collector`` point at this file through the shared loader)
and when pasted into a Deephaven console. Everything it builds is published as a
module-level global so it appears in the web IDE's Panels menu and is reachable from
``pydeephaven`` (``session.open_table("orders_marked")``).

The role is mandatory and has no default: a collector that came up as a leaf would
open AMPS subscriptions for hubs it does not own, and a leaf that came up as a
collector would resolve nothing. ``REMOTEURI_ROLE`` decides, and an unknown value is
a startup error.

Re-running the script is safe: the wired runtime is memoized on the ``remote_uri``
package object, which lives in ``sys.modules``, so a second execution re-exports the
same tables instead of opening a second AMPS subscription per hub or a second set of
Barrage subscriptions.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------------
# sys.path bootstrap -- must run before importing remote_uri / multi_oms / fix42cache,
# because this file is executed as a loose script inside the container
# (/remote-scripts/remote_uri/app.py), not as an installed package. Three directories
# matter, in this precedence order: this module's own src dir, /moms-scripts for the
# unmodified multi_oms, and /scripts for the unmodified fix42cache.
# ---------------------------------------------------------------------------------
try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/remote-scripts/remote_uri"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/remote-scripts"

# Reversed, because insert(0, ...) puts the *last* one first.
for _candidate in ("/scripts", "/moms-scripts", "/remote-scripts", _SRC_DIR):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, List, MutableMapping  # noqa: E402

import remote_uri  # noqa: E402
from remote_uri import config  # noqa: E402

__all__ = ["Runtime", "main", "export", "banner_lines"]

#: Attribute on the ``remote_uri`` package holding the wired runtime across re-execs.
_RUNTIME_ATTR = "_REMOTEURI_RUNTIME"

#: Module-level idempotence flag (doc 04 section 9.1 -- also keeps strong refs alive).
_STARTED = False

#: The banner the e2e greps for, per role. Printed **only** after wiring succeeded.
_BANNERS = {
    config.ROLE_LEAF: "Remote-URI leaf {name} -- ready",
    config.ROLE_COLLECTOR: "Remote-URI collector -- ready",
}

#: Either a ``LeafRuntime`` or a ``CollectorRuntime``: both expose ``role``,
#: ``ready``, ``exports()``, ``banner_lines()`` and ``bind_namespace()``. A protocol
#: rather than a base class, because Application Mode's exec semantics make shared
#: base classes across separately-exec'd modules more trouble than they are worth.
Runtime = Any


def _wire() -> Runtime:
    """Read the role and build the corresponding app.

    Returns:
        A leaf or collector runtime.

    Raises:
        ValueError: If ``REMOTEURI_ROLE`` is missing or unknown, or any other
            doc 10 section 4 rule is violated -- a startup failure, never a silent
            fallback.
    """
    role = config.role()
    if role == config.ROLE_LEAF:
        from remote_uri.leaf import build_leaf

        return build_leaf(config.load_leaf_settings())

    from remote_uri.collector import wire_collector

    return wire_collector(config.load_collector_settings())


def main() -> Runtime:
    """Wire the application, or return the already-wired runtime.

    Idempotent by design: Application Mode plus a console ``exec`` of the same file
    must not double-subscribe a hub's AMPS listener or a leaf's exports.

    Returns:
        The runtime holding tables, query API and dashboard.

    Raises:
        Exception: Whatever wiring raised, after logging it. The loader catches it,
            reports it and keeps the server up so the IDE console can debug it.
    """
    global _STARTED

    existing = getattr(remote_uri, _RUNTIME_ATTR, None)
    if existing is not None:
        _STARTED = True
        print("[remote-uri] already running -- reusing the existing tables")
        return existing

    try:
        runtime = _wire()
    except Exception:  # noqa: BLE001 - surface startup failures in the server log
        print("[remote-uri] FAILED to start the remote-URI app:")
        traceback.print_exc()
        raise

    setattr(remote_uri, _RUNTIME_ATTR, runtime)
    _STARTED = True
    return runtime


def export(namespace: MutableMapping[str, Any], runtime: Runtime) -> None:
    """Publish every table, query-API function and the dashboard into ``namespace``.

    Args:
        namespace: Usually ``globals()`` of this module -- Application Mode exports
            those to the web IDE and to ``pydeephaven``.
        runtime: The wired runtime.

    Note:
        The namespace is also **remembered** by the collector runtime, so
        ``reconnect()`` can push the rebuilt globals back into it: after a rebuild
        the old ``orders_marked`` is a failed table, and a global still pointing at
        it would be worse than useless.
    """
    runtime.bind_namespace(namespace)
    namespace["remote_uri_runtime"] = runtime
    namespace.update(runtime.exports())


def banner_lines(runtime: Runtime) -> List[str]:
    """The startup banner, or the actionable failure notice."""
    role = getattr(runtime, "role", "?")
    if not getattr(runtime, "ready", False):
        return [
            "=" * 78,
            f"Remote-URI {role} -- NOT ready",
            "  the app is wired but not connected; see the lines above for what is",
            "  missing. The server is up: fix the fleet, then run reconnect() here.",
            "=" * 78,
        ]
    title = _BANNERS[role].format(name=getattr(runtime, "name", role))
    return ["=" * 78, title, *runtime.banner_lines(), "=" * 78]


def _print_banner(runtime: Runtime) -> None:
    """Print the startup summary to the server log."""
    print("\n".join(banner_lines(runtime)))


_RUNTIME = main()
export(globals(), _RUNTIME)
_print_banner(_RUNTIME)
