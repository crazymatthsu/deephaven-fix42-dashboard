package com.fix42.dashboard.amps.config;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * Deephaven column type of a mapped field, plus the string -> value coercion for it.
 *
 * <p>Every AMPS payload is text, so each configured field arrives as a {@code String} and
 * has to be coerced to the java type Deephaven expects for that column. The
 * {@link #deephavenDType()} rendering is the matching {@code deephaven.dtypes} expression
 * used when the table is created server-side.
 */
public enum ColumnType {

    STRING(String.class, "dht.string"),
    BOOLEAN(Boolean.class, "dht.bool_"),
    BYTE(Byte.class, "dht.byte"),
    SHORT(Short.class, "dht.int16"),
    INT(Integer.class, "dht.int32"),
    LONG(Long.class, "dht.int64"),
    FLOAT(Float.class, "dht.float32"),
    DOUBLE(Double.class, "dht.double"),
    CHAR(Character.class, "dht.char"),
    INSTANT(Instant.class, "dht.Instant");

    /** FIX UTCTimestamp (tags 52/60): {@code yyyyMMdd-HH:mm:ss} with optional fraction. */
    private static final DateTimeFormatter FIX_UTC_TIMESTAMP = new DateTimeFormatterBuilder()
            .appendPattern("yyyyMMdd-HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter(Locale.ROOT);

    private final Class<?> javaType;
    private final String deephavenDType;

    ColumnType(Class<?> javaType, String deephavenDType) {
        this.javaType = javaType;
        this.deephavenDType = deephavenDType;
    }

    /** The java type Deephaven's Arrow column for this type is built from. */
    public Class<?> javaType() {
        return javaType;
    }

    /** The {@code deephaven.dtypes} expression naming this type in generated python. */
    public String deephavenDType() {
        return deephavenDType;
    }

    /**
     * Resolve a configured type name, tolerating the common aliases.
     *
     * @param text the configured value, e.g. {@code "int"}, {@code "Integer"}, {@code "timestamp"}
     * @return the matching constant
     * @throws IllegalArgumentException if {@code text} names no known type
     */
    public static ColumnType parse(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("column type must not be blank");
        }
        String key = text.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return switch (key) {
            case "STR", "STRING", "TEXT", "VARCHAR" -> STRING;
            case "BOOL", "BOOLEAN" -> BOOLEAN;
            case "BYTE", "INT8" -> BYTE;
            case "SHORT", "INT16" -> SHORT;
            case "INT", "INT32", "INTEGER" -> INT;
            case "LONG", "INT64", "BIGINT" -> LONG;
            case "FLOAT", "FLOAT32", "SINGLE" -> FLOAT;
            case "DOUBLE", "FLOAT64", "DECIMAL" -> DOUBLE;
            case "CHAR", "CHARACTER" -> CHAR;
            case "INSTANT", "TIMESTAMP", "DATETIME", "TIME" -> INSTANT;
            default -> throw new IllegalArgumentException("unknown column type: " + text);
        };
    }

    /**
     * Coerce one raw AMPS field value to this column's java type.
     *
     * @param raw the field value as it appeared in the payload; may be {@code null}
     * @return the coerced value, or {@code null} when {@code raw} is null or blank -- a blank
     *     field is an absent field, which Deephaven stores as its null value for the column
     * @throws IllegalArgumentException if {@code raw} is not a valid value for this type
     */
    public Object coerce(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }
        try {
            return switch (this) {
                case STRING -> value;
                case BOOLEAN -> parseBoolean(value);
                case BYTE -> Byte.valueOf(value);
                case SHORT -> Short.valueOf(value);
                case INT -> Integer.valueOf(value);
                case LONG -> Long.valueOf(value);
                case FLOAT -> Float.valueOf(value);
                case DOUBLE -> Double.valueOf(value);
                case CHAR -> value.charAt(0);
                case INSTANT -> parseInstant(value);
            };
        } catch (IllegalArgumentException | java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "value '" + raw + "' is not a valid " + name() + ": " + e.getMessage(), e);
        }
    }

    private static Boolean parseBoolean(String value) {
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "TRUE", "T", "Y", "YES", "1" -> Boolean.TRUE;
            case "FALSE", "F", "N", "NO", "0" -> Boolean.FALSE;
            default -> throw new IllegalArgumentException("not a boolean");
        };
    }

    /**
     * Parse the timestamp encodings that actually turn up on AMPS topics: FIX UTCTimestamp,
     * ISO-8601, and bare epoch numbers (unit inferred from digit count).
     */
    private static Instant parseInstant(String value) {
        if (value.chars().allMatch(Character::isDigit) && value.length() != 8) {
            long number = Long.parseLong(value);
            return switch (value.length()) {
                case 19 -> Instant.ofEpochSecond(number / 1_000_000_000L, number % 1_000_000_000L);
                case 16 -> Instant.ofEpochSecond(number / 1_000_000L, (number % 1_000_000L) * 1_000L);
                case 13 -> Instant.ofEpochMilli(number);
                case 10 -> Instant.ofEpochSecond(number);
                default -> throw new IllegalArgumentException(
                        "ambiguous epoch timestamp (expected 10, 13, 16 or 19 digits)");
            };
        }
        if (value.length() > 8 && value.charAt(8) == '-') {
            return LocalDateTime.parse(value, FIX_UTC_TIMESTAMP).toInstant(ZoneOffset.UTC);
        }
        return Instant.parse(value);
    }
}
