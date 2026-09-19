package com.fix42.dashboard.connectors.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * The two object-store operations {@link S3RecordSource} needs, and nothing else.
 *
 * <p>The seam that keeps the source testable without a network: {@link SdkS3ObjectStore}
 * implements it over the AWS SDK, and a test supplies objects and ETags from a map. It is
 * deliberately narrow -- everything the source decides (what is new, how bytes become records,
 * when to retry) is decided above this interface, so a fake with a handful of strings in it
 * exercises all of it.
 */
public interface S3ObjectStore extends AutoCloseable {

    /**
     * One object in scope, and the version of it the store currently holds.
     *
     * @param key the object's key, as the store spells it
     * @param etag the store's entity tag for that object's current content, or {@code null}
     *     when the store does not report one -- an object with no ETag is read once and then
     *     treated as unchanged forever, because there is nothing to compare against
     */
    record ObjectRef(String key, String etag) {
    }

    /**
     * Every object currently in scope, sorted by key.
     *
     * <p>Scope is whichever of {@code source.s3.key} / {@code source.s3.prefix} is configured:
     * the one object's ref, or everything under the prefix. The sort is part of the contract,
     * because it is what makes "the records of a new day's file arrive after yesterday's"
     * true rather than incidental.
     *
     * @return the objects in scope, in key order
     * @throws IOException when the listing fails; the source treats it as a lost connection
     */
    List<ObjectRef> list() throws IOException;

    /**
     * Open one object's content.
     *
     * @param key the key to read, as {@link #list()} reported it
     * @return the object's bytes; the caller closes the stream
     * @throws IOException when the object cannot be read
     */
    InputStream read(String key) throws IOException;

    /** Release whatever the store holds. Idempotent; the default is that it holds nothing. */
    @Override
    default void close() {
    }
}
