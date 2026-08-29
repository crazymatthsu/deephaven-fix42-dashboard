package com.fix42.dashboard.fixcache;

import java.math.BigInteger;
import java.util.regex.Pattern;

/**
 * {@code int(str)} with python's acceptance rules, for the {@code 34 MsgSeqNum} tag.
 *
 * <p>Two differences from {@link Long#parseLong} matter on a wire protocol: python accepts
 * digit-group underscores ({@code int("1_0") == 10}) and python's integers are unbounded.
 */
final class PyInt {

    /** python's {@code int()} grammar for a base-10 string (already stripped of whitespace). */
    private static final Pattern PY_INT = Pattern.compile("[+-]?\\d(?:_?\\d)*");

    private PyInt() {}

    /**
     * Parses {@code text} the way python's {@code int()} does.
     *
     * @param text already-stripped text
     * @return the value, unbounded
     * @throws NumberFormatException if python's {@code int()} would raise {@code ValueError}
     */
    static BigInteger parse(String text) {
        if (!PY_INT.matcher(text).matches()) {
            throw new NumberFormatException(text);
        }
        return new BigInteger(text.replace("_", ""));
    }
}
