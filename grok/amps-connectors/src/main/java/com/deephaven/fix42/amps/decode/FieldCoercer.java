package com.deephaven.fix42.amps.decode;

import com.deephaven.fix42.amps.config.ColumnType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class FieldCoercer {
    private static final DateTimeFormatter FIX_UTC =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss[.SSS]");

    private FieldCoercer() {}

    public static Object coerce(String raw, ColumnType type) {
        if (raw == null) {
            return null;
        }
        if (type != ColumnType.STRING && raw.isBlank()) {
            return null;
        }
        try {
            return switch (type) {
                case STRING -> raw;
                case BYTE -> Byte.parseByte(raw.trim());
                case SHORT -> Short.parseShort(raw.trim());
                case INT -> Integer.parseInt(raw.trim());
                case LONG -> Long.parseLong(raw.trim());
                case FLOAT -> Float.parseFloat(raw.trim());
                case DOUBLE -> Double.parseDouble(raw.trim());
                case BOOLEAN -> parseBoolean(raw.trim());
                case CHAR -> {
                    if (raw.isEmpty()) {
                        yield null;
                    }
                    yield raw.charAt(0);
                }
                case INSTANT -> parseInstant(raw.trim());
            };
        } catch (RuntimeException e) {
            throw new DecodeException("cannot coerce '" + raw + "' to " + type, e);
        }
    }

    private static boolean parseBoolean(String raw) {
        if (raw.equalsIgnoreCase("true") || raw.equalsIgnoreCase("y") || raw.equals("1")) {
            return true;
        }
        if (raw.equalsIgnoreCase("false") || raw.equalsIgnoreCase("n") || raw.equals("0")) {
            return false;
        }
        throw new DecodeException("cannot coerce '" + raw + "' to BOOLEAN");
    }

    private static Instant parseInstant(String raw) {
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        if (raw.matches("-?\\d+")) {
            long n = Long.parseLong(raw);
            if (raw.length() > 12) {
                return Instant.ofEpochMilli(n);
            }
            return Instant.ofEpochSecond(n);
        }
        try {
            return LocalDateTime.parse(raw, FIX_UTC).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw new DecodeException("cannot coerce '" + raw + "' to INSTANT", e);
        }
    }
}
