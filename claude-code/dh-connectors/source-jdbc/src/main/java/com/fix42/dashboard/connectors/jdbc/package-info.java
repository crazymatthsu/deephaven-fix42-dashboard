/**
 * The JDBC driver for {@code dh-connectors}.
 *
 * <p>{@link com.fix42.dashboard.connectors.jdbc.JdbcRecordSource} implements
 * {@link com.fix42.dashboard.connectors.source.RecordSource} over a polled query,
 * {@link com.fix42.dashboard.connectors.jdbc.JdbcSourceFactory} claims the connectors that
 * configure {@code source.jdbc}, and
 * {@link com.fix42.dashboard.connectors.jdbc.JdbcSourceAutoConfiguration} registers it --
 * the shape {@code :dh-connectors:source-kafka} has.
 *
 * <p>The configuration it binds to
 * ({@link com.fix42.dashboard.connectors.config.JdbcSourceProperties}) carries the rules that
 * govern it: a query is a snapshot rather than a stream, so {@code mode} decides whether the
 * feed is state re-read in full every poll (with a key column, one that can report what
 * vanished) or a journal read forward from a high-water mark. The rows are serialised to JSON
 * here, which is why the connector must declare {@code format: JSON}.
 */
package com.fix42.dashboard.connectors.jdbc;
