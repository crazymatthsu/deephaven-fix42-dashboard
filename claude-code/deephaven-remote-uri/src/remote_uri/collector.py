"""The collector app: merge the leaves, re-link, mark, aggregate -- doc 10 section 6.

``build_collector`` is a **pure function of the resolved tables**. That is the whole
design: a leaf restart, or any failure that takes a remote table down with every
dependent of it (Deephaven's semantics -- doc 10 section 2.7), is recovered by
re-resolving the fleet and calling the same function again. ``reconnect()`` is
therefore a full rebuild plus a re-export into the app namespace, not a patch.

The linking and reconciliation are doc 09's *implementation*, not a re-derivation:
``multi_oms.dag``'s builders are called with the **full** topology so the ``K-1``
iterated root walk sees ``K = 4`` even though no single server folds four hubs. An
``OMS-C`` order on ``DH2`` links to its ``OMS-A`` ancestor on ``DH1`` exactly as it
would inside one server.

The v1 limitation, documented rather than hidden: the collector's DAG depends on the
remote tables' liveness. The hardening -- a per-leaf ``TablePublisher`` bridge that
republishes added+modified rows into a local blink -- is designed in doc 10 section
2.7 and not built.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, List, MutableMapping, Optional, Tuple

from deephaven import agg, new_table
from deephaven.column import string_col

try:  # `merge` is re-exported at the package root on every server we target
    from deephaven import merge
except ImportError:  # pragma: no cover - keep working if the re-export ever moves
    from deephaven.table_operations import merge

from multi_oms.dag import (
    build_child_rollup,
    build_orders_linked,
    build_orders_recon,
    hub_config_table,
)

from remote_uri import exposure
from remote_uri.config import CollectorSettings
from remote_uri.marketdata_table import MarketDataFeed
from remote_uri.remote import RemoteResolver, ResolveReport
from remote_uri.uris import LEAF_EXPORTS

__all__ = [
    "COLLECTOR_TABLE_NAMES",
    "ROOTS_VIEW",
    "build_collector",
    "CollectorRuntime",
    "wire_collector",
]

#: Every table global the collector publishes, in dependency order (doc 10 section 6).
#: The per-leaf ``rx_*_<leaf>`` globals are added on top, one set per configured leaf.
COLLECTOR_TABLE_NAMES: Tuple[str, ...] = (
    "orders_all",
    "id_index",
    "hub_config",
    "leaf_config",
    "orders_linked",
    "child_rollup",
    "orders_recon",
    "roots",
    "market_data_latest",
    "orders_marked",
    "exposure_by_level",
    "exposure_by_source",
    "exposure_by_leaf",
    "fleet",
    "source_oms_list",
    "account_list",
    "symbol_list",
)

#: ``roots``: one row per family, carrying the root's identity under Root* names.
#: ``Depth == 0`` is the root *of the family* (the walk's fixpoint), which is not the
#: same thing as ``HubDepth == 0`` -- a family whose OMS-A leg has not arrived yet is
#: rooted at its earliest present hop and heals when the parent turns up.
ROOTS_VIEW: Tuple[str, ...] = (
    "RootKey = GlobalKey",
    "RootOms = Oms",
    "RootAccount = Account",
    "RootSymbol = Symbol",
)


def _merge_exports(resolved: Dict[str, Dict[str, Any]], name: str, order: List[str]) -> Any:
    """``merge`` one export across every leaf, in configuration order.

    Every leaf's schema is byte-identical by construction (doc 10 section 5.3: no
    ``Leaf`` column, no state filter), which is exactly what ``merge`` requires.
    """
    return merge([resolved[leaf][name] for leaf in order])


def _leaf_config_table(settings: CollectorSettings) -> Any:
    """The collector's static ``leaf_config`` (``Oms, Leaf, Uri``) -- doc 10 section 6.

    One row per (hub, leaf) pair. Unique on ``Oms`` because no hub may be assigned to
    two leaves (rejected at startup), which is the precondition for the
    ``natural_join`` that attaches ``Leaf`` to every marked row.
    """
    rows: List[Tuple[str, str, str]] = []
    for leaf in settings.leaves:
        for hub in leaf.hubs:
            rows.append((hub, leaf.name, leaf.uri))
    return new_table(
        [
            string_col("Oms", [row[0] for row in rows]),
            string_col("Leaf", [row[1] for row in rows]),
            string_col("Uri", [row[2] for row in rows]),
        ]
    )


def build_collector(
    resolved: Dict[str, Dict[str, Any]],
    settings: CollectorSettings,
    market_data_latest: Any,
) -> Dict[str, Any]:
    """Build the whole collector DAG from the resolved leaf exports.

    Pure with respect to ``resolved``: nothing is subscribed, listened to or
    executed here, which is what makes :meth:`CollectorRuntime.reconnect` a
    rebuild rather than a repair.

    Args:
        resolved: ``{leaf name: {"rx_orders", "rx_id_index", "rx_exposure",
            "rx_leaf_stats"}}`` -- every configured leaf must be present with all
            four exports.
        settings: The validated collector configuration.
        market_data_latest: The quote table (owned by :class:`MarketDataFeed`, built
            once so a reconnect does not reset the walk).

    Returns:
        ``{global name: Table}`` -- :data:`COLLECTOR_TABLE_NAMES` plus one
        ``rx_<export>_<leaf>`` per leaf per export.

    Raises:
        KeyError: If a leaf or one of its exports is missing from ``resolved`` --
            a programming error here, since the resolve loop only reports leaves
            that exposed all four.
    """
    order = [leaf.name for leaf in settings.leaves if leaf.name in resolved]
    if not order:
        raise KeyError(
            "no leaf resolved; build_collector needs at least one leaf exposing "
            f"{list(LEAF_EXPORTS)}"
        )

    tables: Dict[str, Any] = {}
    for leaf in settings.leaves:
        if leaf.name not in resolved:
            continue
        for export in LEAF_EXPORTS:
            tables[leaf.global_name(export)] = resolved[leaf.name][export]

    # -- the union: what the collector holds (doc 10 section 2.5) -----------------
    orders_all = _merge_exports(resolved, "rx_orders", order)
    # last_by keeps one row per (Oms, Id) across leaves; within a leaf the index is
    # already unique, so this only matters if a hub were ever re-homed mid-flight.
    id_index = _merge_exports(resolved, "rx_id_index", order).last_by(["Oms", "Id"])

    hub_config = hub_config_table(settings.topology)
    leaf_config = _leaf_config_table(settings)

    # -- doc 09 section 5.3/5.4, over the union ----------------------------------
    orders_linked = build_orders_linked(settings.topology, orders_all, hub_config, id_index)
    child_rollup = build_child_rollup(orders_linked)
    orders_recon = build_orders_recon(
        orders_linked, child_rollup, settings.qty_tol, settings.notional_tol
    )

    roots = orders_recon.where("Depth == 0").view(list(ROOTS_VIEW))

    # -- doc 10 section 7: mark the open quantity --------------------------------
    orders_marked = (
        orders_recon.natural_join(
            roots, on=["RootKey"], joins=["RootOms", "RootAccount", "RootSymbol"]
        )
        .natural_join(leaf_config, on=["Oms"], joins=["Leaf"])
        .natural_join(market_data_latest, on=["Symbol"], joins=["Mid", "MdTs"])
        .update_view(list(exposure.EXPOSURE_FORMULAS))
    )

    sums = [
        agg.count_("Orders"),
        agg.sum_(list(exposure.EXPOSURE_SUM_COLUMNS)),
    ]
    exposure_by_level = orders_marked.agg_by(sums, by=list(exposure.LEVEL_BY)).sort(
        list(exposure.LEVEL_SORT)
    )
    # "The" totals: root level only. Summing across hubs would count one economic
    # flow once per hop (doc 09's rule); exposure_by_level shows where it went.
    exposure_by_source = (
        orders_marked.where("Depth == 0")
        .agg_by(sums, by=list(exposure.SOURCE_BY))
        .sort(list(exposure.SOURCE_BY))
    )

    exposure_by_leaf = _merge_exports(resolved, "rx_exposure", order).natural_join(
        leaf_config, on=["Oms"], joins=["Leaf"]
    )
    fleet = _merge_exports(resolved, "rx_leaf_stats", order)

    source_oms_list = hub_config.where("UpstreamOms == ``").view(["Oms"])
    account_list = orders_all.select_distinct(["Account"]).sort(["Account"])
    symbol_list = orders_all.select_distinct(["Symbol"]).sort(["Symbol"])

    tables.update(
        {
            "orders_all": orders_all,
            "id_index": id_index,
            "hub_config": hub_config,
            "leaf_config": leaf_config,
            "orders_linked": orders_linked,
            "child_rollup": child_rollup,
            "orders_recon": orders_recon,
            "roots": roots,
            "market_data_latest": market_data_latest,
            "orders_marked": orders_marked,
            "exposure_by_level": exposure_by_level,
            "exposure_by_source": exposure_by_source,
            "exposure_by_leaf": exposure_by_leaf,
            "fleet": fleet,
            "source_oms_list": source_oms_list,
            "account_list": account_list,
            "symbol_list": symbol_list,
        }
    )
    return tables


class CollectorRuntime:
    """The wired collector, and the ``reconnect()`` that rebuilds it.

    A plain class rather than a ``dataclass`` (Application Mode breaks
    ``dataclasses``' introspection -- see ``multi_oms.app.Runtime``). It is also the
    strong reference keeping the resolver, the market-data feed and the dashboard
    alive.
    """

    def __init__(
        self,
        settings: CollectorSettings,
        resolver: RemoteResolver,
        feed: MarketDataFeed,
    ) -> None:
        """Create an unconnected runtime (call :meth:`connect` to build the DAG)."""
        self.role = "collector"
        self.settings = settings
        self.resolver = resolver
        self.feed = feed
        self.tables: Dict[str, Any] = {}
        self.api: Dict[str, Callable[..., Any]] = {}
        self.dashboard: Any = None
        self.report: Optional[ResolveReport] = None
        self.connected = False
        self._namespace: Optional[MutableMapping[str, Any]] = None

    # -- namespace ---------------------------------------------------------------

    def bind_namespace(self, namespace: MutableMapping[str, Any]) -> None:
        """Remember the app namespace so :meth:`reconnect` can re-export into it."""
        self._namespace = namespace

    @property
    def ready(self) -> bool:
        """True when the DAG is built (i.e. every leaf resolved)."""
        return self.connected

    # -- wiring ------------------------------------------------------------------

    def connect(self, wait: bool = True) -> ResolveReport:
        """Resolve the fleet and build the DAG.

        Args:
            wait: Retry until every export is resolved or ``REMOTEURI_CONNECT_TIMEOUT``
                elapses (startup). ``False`` makes a single pass (``reconnect``).

        Returns:
            The :class:`~remote_uri.remote.ResolveReport`. On an incomplete report
            the DAG is **not** built and :attr:`connected` stays ``False`` -- the
            caller logs one line per missing export and leaves the server up.
        """
        report = self.resolver.wait_for_all() if wait else self.resolver.resolve_once()
        self.report = report
        if not report.complete:
            self.connected = False
            return report
        self.tables = build_collector(report.resolved, self.settings, self.feed.table)
        self.feed.attach(self.tables["orders_all"])
        self.api = self._build_api()
        self.dashboard = self._build_dashboard()
        self.connected = True
        return report

    def reconnect(self) -> ResolveReport:
        """Re-resolve every leaf, rebuild the DAG and re-export every global.

        The doc 10 section 2.7 recovery: in Deephaven a failed remote table fails
        every dependent, so there is nothing to repair in place -- the whole DAG is
        rebuilt from fresh subscriptions.

        Sessions are closed first, in **both** resolver modes: a leaf that restarted
        has forgotten its ``rx_q_*`` globals and its old session is dead, so reusing
        it would fail the next remote call in a much less obvious way. The live
        queries are dropped first (best effort) so a leaf that is merely slow, not
        restarted, does not accumulate abandoned globals.

        Returns:
            The :class:`~remote_uri.remote.ResolveReport` of the new attempt.
        """
        print("[remote-uri] reconnect: dropping live remote queries and Barrage sessions")
        self.connected = False
        self.feed.detach()
        try:
            self.resolver.close()
        except Exception:  # noqa: BLE001 - teardown must never block a rebuild
            traceback.print_exc()
        report = self.connect(wait=False)
        if report.complete:
            self._reexport()
            print(f"[remote-uri] reconnect: {report.describe()}; globals re-exported")
        else:
            print("[remote-uri] reconnect FAILED -- the fleet is not fully exported yet:")
            for line in report.missing:
                print(line)
            print("[remote-uri] the server stays up; call reconnect() again when the leaves are ready")
        return report

    def _reexport(self) -> None:
        """Push the rebuilt globals back into the app namespace."""
        namespace = self._namespace
        if namespace is None:  # pragma: no cover - app.py always binds one
            print("[remote-uri] no app namespace bound; the rebuilt tables are on remote_uri_runtime")
            return
        namespace.update(self.exports())

    # -- the query API and the dashboard ------------------------------------------

    def _build_api(self) -> Dict[str, Callable[..., Any]]:
        """Build the doc 10 section 9 query API bound to the current tables."""
        from remote_uri.query_api import make_query_api

        return make_query_api(self.tables, self.resolver, self.settings, self.reconnect)

    def _build_dashboard(self) -> Any:
        """Build the doc 10 section 8 dashboard (``None`` without ``deephaven.ui``)."""
        from remote_uri.dashboard import build_dashboard

        try:
            return build_dashboard(self.settings, self.tables, self.api)
        except Exception:  # noqa: BLE001 - a UI failure must not cost the tables
            print("[remote-uri] the dashboard could not be built; the tables are unaffected:")
            traceback.print_exc()
            return None

    # -- exports -----------------------------------------------------------------

    def exports(self) -> Dict[str, Any]:
        """Every global this runtime publishes (doc 10 section 6).

        ``reconnect`` is published **unconditionally**, including when the resolve
        loop timed out and there is no DAG at all: it is the one thing the console
        needs in exactly that situation, and it lives on this runtime rather than on
        the (missing) tables.
        """
        published: Dict[str, Any] = {"reconnect": self.reconnect}
        published.update(self.tables)
        published.update(self.api)
        published["remote_uri_dashboard"] = self.dashboard
        return published

    @property
    def table_names(self) -> List[str]:
        """Every global table name this runtime exports, sorted."""
        return sorted(self.tables)

    def banner_lines(self) -> List[str]:
        """The startup banner body (``app.py`` adds the rule lines and the title)."""
        settings = self.settings
        lines = [
            f"  leaves          : {settings.describe()}",
        ]
        lines.extend(f"    {leaf.describe()}" for leaf in settings.leaves)
        if settings.unassigned:
            lines.append(f"  UNASSIGNED hubs : {list(settings.unassigned)} (orders will be DANGLING)")
        dashboard_status = (
            "remote_uri_dashboard"
            if self.dashboard
            else "unavailable (deephaven.ui missing) -- use the table panels"
        )
        lines.extend(
            [
                f"  hubs            : {list(settings.topology.names)}",
                f"  market data     : {self.feed.describe()}",
                f"  tolerances      : qty={settings.qty_tol} notional={settings.notional_tol}",
                f"  tables          : {', '.join(self.table_names)}",
                f"  query api       : {', '.join(sorted(self.api))}",
                f"  dashboard       : {dashboard_status}",
            ]
        )
        if self.report is not None:
            lines.append(f"  resolve         : {self.report.describe()}")
        return lines


def wire_collector(settings: CollectorSettings) -> CollectorRuntime:
    """Build the collector runtime and make the first resolve attempt.

    Never raises on a resolve timeout: the server stays up with an unconnected
    runtime so ``reconnect()`` can be called from the IDE console once the leaves
    have finished exporting (doc 10 section 6 -- the loader's contract).
    """
    resolver = RemoteResolver(settings)
    feed = MarketDataFeed(settings)
    runtime = CollectorRuntime(settings, resolver, feed)
    report = runtime.connect(wait=True)
    if not report.complete:
        print(
            "[remote-uri] the collector did NOT build its DAG: "
            f"{len(report.missing)} export(s) were still missing after "
            f"{report.elapsed:.0f}s. One line per missing export:"
        )
        for line in report.missing:
            print(line)
        print(
            "[remote-uri] the server is up and the console works. Check that every leaf "
            "printed 'Remote-URI leaf <name> -- ready', then run reconnect() here."
        )
    return runtime
