package com.fix42.dashboard.fixcache;

/**
 * A Java stand-in for a python built-in exception, so that error text produced by the port matches
 * the python original character for character.
 *
 * <p>{@code OrderStateMachine} renders a caught failure as
 * {@code "internal error: <type>: <message>"}, where python's {@code <type>} is
 * {@code type(exc).__name__}. Carrying the python type name here lets pathological input (a FIX
 * message whose {@code 38 OrderQty} is {@code nan}, say) produce the identical {@code Result.error}
 * in both implementations.
 */
public final class PyException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String pyType;

    PyException(String pyType, String message) {
        super(message);
        this.pyType = pyType;
    }

    /** python's {@code type(exc).__name__}, e.g. {@code ValueError}. */
    public String pyType() {
        return pyType;
    }

    /** python's {@code int(float('nan'))}. */
    static PyException valueErrorNanToInteger() {
        return new PyException("ValueError", "cannot convert float NaN to integer");
    }

    /** python's {@code int(float('inf'))} (the message is sign-independent). */
    static PyException overflowErrorInfinityToInteger() {
        return new PyException("OverflowError", "cannot convert float infinity to integer");
    }
}
