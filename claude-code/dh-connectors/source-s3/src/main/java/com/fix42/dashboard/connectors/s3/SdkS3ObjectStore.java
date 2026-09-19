package com.fix42.dashboard.connectors.s3;

import com.fix42.dashboard.connectors.config.S3SourceProperties;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * {@link S3ObjectStore} over the AWS SDK's synchronous {@link S3Client}.
 *
 * <p>The only class in the module that knows the SDK exists; everything the source decides is
 * decided above the {@link S3ObjectStore} interface. The client is built once, from
 * {@code source.s3}, and closed with the store.
 *
 * <p>Three settings exist entirely so that a local MinIO or localstack is the same connector
 * as real S3: {@code endpoint} overrides the endpoint, {@code path-style-access} addresses
 * buckets as {@code <endpoint>/<bucket>} (virtual-host addressing needs wildcard DNS a
 * developer machine does not have), and the static credentials are used only when both halves
 * are set -- otherwise the SDK's default provider chain runs, which is what a deployed
 * connector on an instance role wants.
 */
final class SdkS3ObjectStore implements S3ObjectStore {

    private final S3Client client;
    private final String bucket;

    /** Exactly one of these is non-null; {@code ConnectorValidator} guarantees it. */
    private final String key;
    private final String prefix;

    SdkS3ObjectStore(S3SourceProperties source) {
        this(client(source), source);
    }

    /** For a caller that has already built a client -- the seam the tests do not need. */
    SdkS3ObjectStore(S3Client client, S3SourceProperties source) {
        this.client = client;
        this.bucket = source.getBucket();
        this.key = source.getKey() != null && !source.getKey().isBlank() ? source.getKey() : null;
        this.prefix = source.getPrefix();
    }

    private static S3Client client(S3SourceProperties source) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(source.getRegion()))
                // Explicit, so the SDK never service-loads a default client: this module
                // excludes apache-client and netty-nio-client precisely so it cannot.
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(source.isPathStyleAccess());
        if (source.getEndpoint() != null) {
            builder.endpointOverride(URI.create(source.getEndpoint()));
        }
        if (source.hasStaticCredentials()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(source.getAccessKey(), source.getSecretKey())));
        }
        return builder.build();
    }

    /**
     * The single key's ref, or every object under the prefix.
     *
     * <p>Single-key mode is a {@code HeadObject} rather than a listing: it is one request that
     * answers exactly the question the source asks (does this object still have the ETag I
     * read?), and a key that does not exist yet raises, which the source treats the same way
     * it treats a bucket it cannot reach.
     */
    @Override
    public List<ObjectRef> list() {
        if (key != null) {
            HeadObjectResponse head = client.headObject(request ->
                    request.bucket(bucket).key(key));
            return List.of(new ObjectRef(key, head.eTag()));
        }
        List<ObjectRef> refs = new ArrayList<>();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        // Paginated: a prefix with more than 1000 objects under it is ordinary, and the
        // source's "new or changed" rule is only correct if it sees all of them.
        for (ListObjectsV2Response page : client.listObjectsV2Paginator(request)) {
            for (S3Object object : page.contents()) {
                refs.add(new ObjectRef(object.key(), object.eTag()));
            }
        }
        // S3 lists in lexicographic key order already; sorting makes that a property of this
        // class rather than of the service, so the contract holds for any implementation.
        refs.sort(Comparator.comparing(ObjectRef::key));
        return refs;
    }

    @Override
    public InputStream read(String objectKey) {
        return client.getObject(request -> request.bucket(bucket).key(objectKey));
    }

    @Override
    public void close() {
        client.close();
    }
}
