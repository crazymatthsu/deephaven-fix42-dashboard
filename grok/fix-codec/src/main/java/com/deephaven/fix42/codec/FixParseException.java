package com.deephaven.fix42.codec;

public final class FixParseException extends RuntimeException {
    public FixParseException(String message) {
        super(message);
    }

    public FixParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
