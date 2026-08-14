package com.fix42.dashboard.gen;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An ordered FIX 4.2 tag&rarr;value map.
 *
 * <p>Insertion order is the wire order of the body; re-setting an existing tag overwrites the value
 * <em>in place</em>, which is what lets {@link ScenarioEngine} reserve header slots for
 * {@code 34 MsgSeqNum}, {@code 52 SendingTime} and {@code 60 TransactTime} at build time and stamp
 * them once the interleaved stream order is known.
 *
 * <p>The framing tags {@code 9 BodyLength} and {@code 10 CheckSum} are owned by
 * {@link FixSerializer} and may not be set here.
 */
public final class FixMessage {

    private final LinkedHashMap<Integer, String> fields = new LinkedHashMap<>();

    private FixMessage() {}

    /** Creates a {@code FIX.4.2} message of the given {@code 35 MsgType}. */
    public static FixMessage create(String msgType) {
        FixMessage msg = new FixMessage();
        msg.fields.put(FixTags.BEGIN_STRING, FixTags.BEGIN_STRING_FIX42);
        msg.fields.put(FixTags.MSG_TYPE, Objects.requireNonNull(msgType, "msgType"));
        return msg;
    }

    public FixMessage set(int tag, String value) {
        if (tag == FixTags.BODY_LENGTH || tag == FixTags.CHECK_SUM) {
            throw new IllegalArgumentException("tag " + tag + " is computed by FixSerializer");
        }
        if (tag <= 0) {
            throw new IllegalArgumentException("tag must be positive: " + tag);
        }
        Objects.requireNonNull(value, "value for tag " + tag);
        if (value.indexOf(FixTags.SOH) >= 0) {
            throw new IllegalArgumentException("value for tag " + tag + " contains SOH");
        }
        fields.put(tag, value);
        return this;
    }

    public FixMessage set(int tag, long value) {
        return set(tag, Long.toString(value));
    }

    /** Sets a price-like field using FIX-friendly decimal formatting (no exponent, min 2 decimals). */
    public FixMessage setPrice(int tag, double value) {
        return set(tag, price(value));
    }

    /** Sets a quantity field; whole quantities render without a decimal point. */
    public FixMessage setQty(int tag, double value) {
        return set(tag, qty(value));
    }

    public String get(int tag) {
        return fields.get(tag);
    }

    public boolean has(int tag) {
        return fields.containsKey(tag);
    }

    public String msgType() {
        return fields.get(FixTags.MSG_TYPE);
    }

    /** Unmodifiable view of the fields in wire order. */
    public Map<Integer, String> fields() {
        return Collections.unmodifiableMap(fields);
    }

    /** Formats a price: fixed-point, trailing zeros trimmed, at least two decimals. */
    public static String price(double value) {
        BigDecimal b = BigDecimal.valueOf(value).setScale(6, RoundingMode.HALF_UP).stripTrailingZeros();
        if (b.scale() < 2) {
            b = b.setScale(2, RoundingMode.UNNECESSARY);
        }
        return b.toPlainString();
    }

    /** Formats a quantity: integral values render as integers, otherwise fixed-point. */
    public static String qty(double value) {
        if (value == Math.rint(value) && !Double.isInfinite(value)) {
            return Long.toString((long) value);
        }
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();
    }

    @Override
    public String toString() {
        return FixSerializer.renderPipe(FixSerializer.serialize(this));
    }
}
