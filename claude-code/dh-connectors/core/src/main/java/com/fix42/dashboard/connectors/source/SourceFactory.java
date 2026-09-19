package com.fix42.dashboard.connectors.source;

import com.fix42.dashboard.connectors.config.ConnectorProperties;

/**
 * Builds the {@link RecordSource} for one transport.
 *
 * <p>The extension point a source module contributes: {@code :dh-connectors:source-amps} and
 * friends register a factory as a bean, and {@link SourceResolver} asks each in turn which
 * connectors it recognises. Core therefore holds no list of drivers -- adding a transport is
 * adding a module to the classpath, and an application carries only the ones it depends on.
 *
 * <p>{@link #supports} answers from the configuration alone (in practice: "is my block under
 * {@code source:} non-null"), so the wrong transport is never constructed just to be asked.
 */
public interface SourceFactory {

    /**
     * Whether this factory is the one that builds {@code connector}'s source.
     *
     * @param connector the connector configuration
     * @return {@code true} if {@link #create} can build a source for it
     */
    boolean supports(ConnectorProperties connector);

    /**
     * @param connector a connector this factory {@link #supports}
     * @return a fresh, unstarted source
     */
    RecordSource create(ConnectorProperties connector);
}
