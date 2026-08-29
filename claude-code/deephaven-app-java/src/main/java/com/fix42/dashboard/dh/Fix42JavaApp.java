package com.fix42.dashboard.dh;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point: wires ingest to pipeline to DAG to query API -- the Java port of
 * {@code dh_app.app}.
 *
 * <p>Two supported routes into this class:
 *
 * <ol>
 *   <li><b>{@code type=script} plus a jpy shim</b> (the default, and what
 *       {@code docker/apps/fix42-dashboard-java/main.py} uses). The shim calls {@link #start()} and
 *       binds {@link Runtime#tables()} into the python script session's globals. Those become
 *       <em>query scope</em> variables, which is what {@code pydeephaven}'s {@code open_table} and
 *       {@code Session.tables} resolve -- so {@code integration-test/test_e2e.py} runs against this
 *       app unchanged, and the existing {@code deephaven.ui} dashboard can be pointed at these
 *       tables without modification.
 *   <li><b>{@code type=dynamic}</b>, via {@link #create(ApplicationState.Listener)}. Pure Java, no
 *       python at all. The tables appear in the web IDE's Panels menu, but as <em>application</em>
 *       fields, whose ticket ({@code a/<appId>/f/<name>}) {@code pydeephaven}'s
 *       {@code open_table} does not build -- so the e2e suite cannot see them.
 * </ol>
 *
 * <p>Re-running is safe: the wired runtime is memoized on this class, so a second call re-exports
 * the same tables instead of opening a second subscription to the source.
 */
public final class Fix42JavaApp implements ApplicationState.Factory {

    /** Application id and name for the {@code type=dynamic} route. */
    private static final String APP_ID = "fix42.dashboard.java";

    private static final String APP_NAME = "FIX42 Order State Dashboard (Java engine)";

    /**
     * Keeps the whole DAG alive.
     *
     * <p>Deephaven's application injector wraps {@code create()} in a liveness scope that releases
     * on close, so anything refreshing that is merely handed to {@code setField} dies the instant
     * {@code create()} returns. Building inside this long-lived scope -- opened with
     * {@code release=false} -- is what Deephaven's own shipped {@code GcApplication} does, and it
     * covers the shim route too, where the caller is a python script session.
     */
    private static LivenessScope scope;

    private static Runtime runtime;

    /** Public no-arg constructor: the {@code type=dynamic} loader instantiates this reflectively. */
    public Fix42JavaApp() {}

    /**
     * Wires the application, or returns the already-wired runtime.
     *
     * <p>Idempotent by design: Application Mode plus a console call of the same entry point must not
     * double-subscribe the source listener.
     *
     * @return the runtime holding tables, query API and counters
     */
    public static synchronized Runtime start() {
        if (runtime != null) {
            System.out.println("[fix42] pipeline already running -- reusing the existing tables");
            return runtime;
        }
        LivenessScope opened = new LivenessScope();
        try (SafeCloseable ignored = LivenessScopeStack.open(opened, false)) {
            Table fixRaw = Ingest.buildFixRaw();
            Fix42Pipeline pipeline = new Fix42Pipeline();
            Map<String, Table> streams = pipeline.start(fixRaw);
            Map<String, Table> derived = Fix42Dag.buildDerived(streams);

            Map<String, Table> tables = new LinkedHashMap<>();
            tables.put(Names.FIX_RAW, fixRaw);
            tables.putAll(streams);
            tables.putAll(derived);

            scope = opened;
            runtime = new Runtime(pipeline, tables, new Fix42QueryApi(derived), Ingest.sourceDescription());
            return runtime;
        } catch (RuntimeException | Error startupFailure) {
            System.out.println("[fix42] FAILED to start the FIX 4.2 dashboard application:");
            startupFailure.printStackTrace();
            scope = null;
            runtime = null;
            throw startupFailure;
        }
    }

    /** The wired runtime, or {@code null} when {@link #start()} has not run. */
    public static synchronized Runtime runtime() {
        return runtime;
    }

    /**
     * The {@code type=dynamic} route: exports every table as an {@link ApplicationState} field.
     *
     * <p>Fields appear in the Panels menu but are <b>not</b> reachable through
     * {@code pydeephaven.Session.open_table} -- see the class javadoc.
     */
    @Override
    public ApplicationState create(ApplicationState.Listener listener) {
        ApplicationState state = new ApplicationState(listener, APP_ID, APP_NAME);
        Runtime wired = start();
        wired.tables().forEach(state::setField);
        System.out.println(wired.bannerText());
        return state;
    }

    /** Everything the wiring produced; also the strong references keeping it alive. */
    public static final class Runtime {

        private final Fix42Pipeline pipeline;
        private final Map<String, Table> tables;
        private final Fix42QueryApi queryApi;
        private final String sourceDescription;

        Runtime(Fix42Pipeline pipeline, Map<String, Table> tables, Fix42QueryApi queryApi, String sourceDescription) {
            this.pipeline = pipeline;
            this.tables = Collections.unmodifiableMap(tables);
            this.queryApi = queryApi;
            this.sourceDescription = sourceDescription;
        }

        public Fix42Pipeline pipeline() {
            return pipeline;
        }

        /** Every exported table: {@code fix_raw}, the five blink streams, and the eleven derived nodes. */
        public Map<String, Table> tables() {
            return tables;
        }

        public Fix42QueryApi queryApi() {
            return queryApi;
        }

        /** One-line summary of where {@code fix_raw} is reading from. */
        public String sourceDescription() {
            return sourceDescription;
        }

        /** Every global table name this runtime exports, sorted. */
        public List<String> tableNames() {
            List<String> names = new ArrayList<>(tables.keySet());
            Collections.sort(names);
            return names;
        }

        /**
         * The concise startup summary printed to the server log, matching the python app's.
         *
         * @param dashboard what to report on the dashboard line. The Java app cannot know: the
         *     {@code deephaven.ui} dashboard is built by the python shim <em>after</em>
         *     {@link #start()} returns, because that plugin has no Java API.
         */
        public String bannerText(String dashboard) {
            String bar = "=".repeat(78);
            return String.join(
                    "\n",
                    bar,
                    "FIX 4.2 Order State Dashboard (Java engine) -- ready",
                    "  source          : " + sourceDescription,
                    "  tables          : " + String.join(", ", tableNames()),
                    "  query api       : find_by_account, find_by_symbol, get_by_clordid, "
                            + "get_by_execid, get_by_order_id, order_detail",
                    "  dashboard       : " + dashboard,
                    bar);
        }

        /** The banner for the pure-Java route, where there is no python dashboard at all. */
        public String bannerText() {
            return bannerText("unavailable (deephaven.ui is python-only) -- use the table panels");
        }
    }
}
