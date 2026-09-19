/**
 * The S3 driver for {@code dh-connectors}.
 *
 * <p>{@link com.fix42.dashboard.connectors.s3.S3RecordSource} implements
 * {@link com.fix42.dashboard.connectors.source.RecordSource} over a polled object listing,
 * {@link com.fix42.dashboard.connectors.s3.S3SourceFactory} claims the connectors that
 * configure {@code source.s3}, and
 * {@link com.fix42.dashboard.connectors.s3.S3SourceAutoConfiguration} registers it -- the
 * shape {@code :dh-connectors:source-kafka} has.
 *
 * <p>{@link com.fix42.dashboard.connectors.s3.S3ObjectStore} is the seam between the source's
 * rule and the AWS SDK: {@code list()} and {@code read(key)}, implemented over a real client by
 * {@code SdkS3ObjectStore} and over a map of strings by the tests.
 *
 * <p>The configuration it binds to
 * ({@link com.fix42.dashboard.connectors.config.S3SourceProperties}) carries the rules that
 * govern it: one scope (a key or a prefix, never both), and one rule for what a poll emits --
 * an unseen key or a changed ETag is read and framed. An object feed re-emits rather than
 * converging onto a key, so it is never stateful, never emits a removal, and refuses
 * {@code deephaven.source-key-column}.
 */
package com.fix42.dashboard.connectors.s3;
