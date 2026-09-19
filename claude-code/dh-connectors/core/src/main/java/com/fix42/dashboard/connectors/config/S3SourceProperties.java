package com.fix42.dashboard.connectors.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/**
 * The S3 side of one connector: a bucket, the object or the prefix to read, and how the bytes
 * of each object are split into records.
 *
 * <p>Lives under {@code source.s3}; its presence is what selects the S3 source
 * ({@link SourceProperties}).
 *
 * <p>An object store has no subscription, so this transport <em>polls</em> -- and unlike the
 * JDBC block there is no mode to choose, because there is only one rule: an object whose key
 * has not been seen, or whose ETag has changed since it was, is read and emitted. A single
 * {@link #getKey()} re-emits whenever that object is rewritten; a {@link #getPrefix()} emits
 * each new object under it once. Both fall out of the same rule.
 *
 * <p>That rule also says what this feed is <em>not</em>: an object re-read is a re-emission,
 * not a convergence onto a key, so an S3 connector is never
 * {@link SourceProperties#stateful()}. {@code APPEND_ONLY} and {@code RING} are its natural
 * targets; a {@code KEYED} table is legal but is the operator's explicit call, keyed on payload
 * fields, because a frame carries no key of its own and this source never emits a removal.
 */
public class S3SourceProperties {

    /** How one record is delimited from the next inside an object. */
    public enum Framing {
        /** Records are separated by {@link #getDelimiter()} -- one line of an NDJSON object. */
        DELIMITED,
        /** The whole object is one record -- one document per key. */
        WHOLE
    }

    /** Bucket to read from. */
    @NotBlank
    private String bucket;

    /**
     * A single object to read. Exactly one of {@code key} / {@link #getPrefix()} is configured
     * ({@link ConnectorValidator} enforces it, rather than an annotation, because "exactly one"
     * is a rule about the pair): a bucket alone does not say what to read, and naming both
     * would name the same scope twice.
     */
    private String key;

    /** Every object under this prefix, in key order. The other half of the pair above. */
    private String prefix;

    /** AWS region the bucket lives in; also what signs the requests. */
    @NotBlank
    private String region = "us-east-1";

    /**
     * An S3-compatible endpoint to dial instead of AWS -- MinIO, localstack, a vendor gateway.
     * Blank is unset (the getter normalises it), so a deployment can leave
     * {@code "${S3_ENDPOINT:}"} in the file and get real S3 when nothing sets the variable.
     */
    private String endpoint;

    /**
     * Address buckets as {@code <endpoint>/<bucket>} rather than {@code <bucket>.<endpoint>}.
     * What a local MinIO needs, because virtual-host addressing requires wildcard DNS that a
     * developer machine does not have.
     */
    private boolean pathStyleAccess = false;

    /**
     * Static access key. Blank is unset (the getter normalises it); set together with
     * {@link #getSecretKey()} it is what the client signs with, and leaving both unset hands
     * the job to the SDK's default provider chain -- environment, profile, container or
     * instance credentials, which is what a deployed connector should normally use.
     *
     * <p>Values arrive as {@code ${S3_ACCESS_KEY:}}-style placeholders, never as literals: the
     * config tree is plaintext in git, and {@code ConfigTreeTest} allows a credential-keyed
     * line to carry a placeholder and nothing else.
     */
    private String accessKey;

    /** Static secret key; the other half of {@link #getAccessKey()}, same placeholder rule. */
    private String secretKey;

    /** How an object's bytes are split into records. */
    @NotNull
    private Framing framing = Framing.DELIMITED;

    /**
     * Record separator for {@link Framing#DELIMITED}, matched as a byte sequence in
     * {@link #getCharset()}, so a multi-byte separator such as CRLF works. An object that does
     * not end with one still yields its tail as a record -- an object is finite, unlike a
     * socket, so the bytes after the last separator are a whole record rather than a fragment
     * waiting for more.
     *
     * <p>{@code @NotEmpty}, deliberately not {@code @NotBlank}: the commonest delimiter of
     * all is a newline, and a newline <em>is</em> blank. The rule that matters is that the
     * separator exists -- an empty one would frame every byte as its own record.
     */
    @NotEmpty
    private String delimiter = "\n";

    /** Charset the object bytes are decoded with. */
    @NotBlank
    private String charset = "UTF-8";

    /**
     * How long to wait between listings. Slower than the other transports' defaults on purpose:
     * every poll is a billed {@code ListObjectsV2}, and an object store is a place files land,
     * not a place ticks arrive.
     */
    @NotNull
    private Duration pollInterval = Duration.ofSeconds(30);

    /** How long to wait before retrying after a failed listing or a failed object read. */
    @NotNull
    private Duration reconnectDelay = Duration.ofSeconds(5);

    /**
     * Whatever names the objects in scope -- the single key, or the prefix -- for logs and
     * error messages.
     *
     * @return the configured {@link #getKey()} or {@link #getPrefix()}, whichever is set
     */
    public String keyOrPrefix() {
        return key != null && !key.isBlank() ? key : prefix;
    }

    /**
     * Whether the connector supplies its own credentials rather than leaning on the SDK's
     * default provider chain. Both halves or neither: one alone cannot sign anything.
     *
     * @return {@code true} when both the access key and the secret key are set
     */
    public boolean hasStaticCredentials() {
        return getAccessKey() != null && getSecretKey() != null;
    }

    private static String unsetWhenBlank(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    /** @return the endpoint override, or {@code null} when none is configured. */
    public String getEndpoint() {
        return unsetWhenBlank(endpoint);
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    public void setPathStyleAccess(boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }

    /** @return the static access key, or {@code null} when the placeholder resolved to blank. */
    public String getAccessKey() {
        return unsetWhenBlank(accessKey);
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    /** @return the static secret key, or {@code null} when the placeholder resolved to blank. */
    public String getSecretKey() {
        return unsetWhenBlank(secretKey);
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public Framing getFraming() {
        return framing;
    }

    public void setFraming(Framing framing) {
        this.framing = framing;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(Duration reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }
}
