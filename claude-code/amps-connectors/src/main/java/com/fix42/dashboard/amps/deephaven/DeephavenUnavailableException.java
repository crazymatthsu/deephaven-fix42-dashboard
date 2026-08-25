package com.fix42.dashboard.amps.deephaven;

/** Thrown when the Deephaven server cannot be reached or rejected an operation. */
public class DeephavenUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DeephavenUnavailableException(String message) {
        super(message);
    }

    public DeephavenUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
