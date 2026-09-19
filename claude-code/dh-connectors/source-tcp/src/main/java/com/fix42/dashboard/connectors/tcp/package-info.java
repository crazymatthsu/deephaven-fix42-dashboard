/**
 * The raw-TCP driver for {@code dh-connectors}.
 *
 * <p>{@link com.fix42.dashboard.connectors.tcp.TcpRecordSource} implements
 * {@link com.fix42.dashboard.connectors.source.RecordSource} over a framed socket,
 * {@link com.fix42.dashboard.connectors.tcp.TcpSourceFactory} claims the connectors that
 * configure {@code source.tcp}, and
 * {@link com.fix42.dashboard.connectors.tcp.TcpSourceAutoConfiguration} registers it -- the
 * shape {@code :dh-connectors:source-amps} has, minus the client library: a framed reader is
 * {@code java.net} plus the framing rules in
 * {@link com.fix42.dashboard.connectors.config.TcpSourceProperties}.
 *
 * <p>A socket has no replay and no message key, so this source is never stateful, never emits
 * a removal, and refuses {@code deephaven.source-key-column}.
 */
package com.fix42.dashboard.connectors.tcp;
