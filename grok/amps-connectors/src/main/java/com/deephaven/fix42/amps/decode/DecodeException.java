package com.deephaven.fix42.amps.decode;

public final class DecodeException extends RuntimeException {
    public DecodeException(String message) {
        super(message);
    }

    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
