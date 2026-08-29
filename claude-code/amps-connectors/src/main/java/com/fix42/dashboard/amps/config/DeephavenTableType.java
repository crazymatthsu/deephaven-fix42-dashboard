package com.fix42.dashboard.amps.config;

/**
 * The kind of Deephaven table a connector publishes into.
 *
 * <p>Deephaven offers exactly one remotely writable table -- the {@code input_table}, in its
 * append-only and keyed forms -- so every other shape has to be built on the server out of
 * something that can be fed from off-box. That splits the four types into two families:
 *
 * <table border="1">
 *   <caption>How each type is created and fed</caption>
 *   <tr><th>type</th><th>created as</th><th>rows arrive via</th><th>retains</th></tr>
 *   <tr><td>{@link #KEYED}</td><td>{@code input_table(key_cols=…)}</td>
 *       <td>{@code addToInputTable}</td><td>one row per key</td></tr>
 *   <tr><td>{@link #APPEND_ONLY}</td><td>{@code input_table()}</td>
 *       <td>{@code addToInputTable}</td><td>everything</td></tr>
 *   <tr><td>{@link #BLINK}</td><td>{@code table_publisher()}</td>
 *       <td>{@code TablePublisher.add}</td><td>one update cycle</td></tr>
 *   <tr><td>{@link #RING}</td><td>{@code ring_table(blink, capacity)}</td>
 *       <td>{@code TablePublisher.add}</td><td>the last {@code capacity} rows</td></tr>
 * </table>
 *
 * <p>{@link #BLINK} and {@link #RING} are the bounded-memory answers, and they are bounded for
 * real: nothing upstream of them holds the rows. Deriving a blink table from an append-only
 * {@code input_table} instead ({@code add_only_to_blink}) would give the same <em>semantics</em>
 * while the input table went on retaining every row, which is why this module does not do that
 * -- Deephaven's own documentation warns the combination increases memory rather than saving it.
 *
 * <p>Only {@link #KEYED} supports removal, so it is the only type an out-of-focus message can be
 * applied to, and the only one {@code publish-mode: DELTA} can merge into.
 */
public enum DeephavenTableType {

    /** {@code input_table(col_defs=…, key_cols=…)}: an add replaces that key's row. */
    KEYED(true, false),

    /** {@code input_table(col_defs=…)}: every row is appended and kept. */
    APPEND_ONLY(false, false),

    /** A blink table: rows are visible for one update graph cycle and then gone. */
    BLINK(false, true),

    /** {@code ring_table} over a blink table: the latest {@code ring-capacity} rows. */
    RING(false, true);

    private final boolean keyed;
    private final boolean publisherBacked;

    DeephavenTableType(boolean keyed, boolean publisherBacked) {
        this.keyed = keyed;
        this.publisherBacked = publisherBacked;
    }

    /**
     * Whether rows are addressed by key: an add replaces the row for that key, and a removal is
     * meaningful.
     *
     * @return {@code true} for {@link #KEYED}
     */
    public boolean keyed() {
        return keyed;
    }

    /**
     * Whether the target is fed through a server-side {@code TablePublisher} rather than by
     * writing into an input table.
     *
     * @return {@code true} for {@link #BLINK} and {@link #RING}
     */
    public boolean publisherBacked() {
        return publisherBacked;
    }

    /**
     * Whether the type is sized by {@code ring-capacity}.
     *
     * @return {@code true} for {@link #RING}
     */
    public boolean bounded() {
        return this == RING;
    }

    /**
     * Resolve the configured type, falling back to the shape the topic implies.
     *
     * <p>Leaving {@code table-type} unset reproduces the behaviour this module had before the
     * setting existed: a SOW topic is a keyed store, a journal topic is a log. Naming a type
     * overrides that, which is the whole point of the setting -- a SOW topic rendered as a
     * {@link #BLINK} table is a live view of updates rather than of state, and a journal topic
     * rendered as a {@link #RING} table is a bounded tail of it.
     *
     * @param configured the value from {@code deephaven.table-type}, or {@code null}
     * @param sow whether the source is a SOW topic
     * @return the type to create
     */
    public static DeephavenTableType resolve(DeephavenTableType configured, boolean sow) {
        if (configured != null) {
            return configured;
        }
        return sow ? KEYED : APPEND_ONLY;
    }
}
