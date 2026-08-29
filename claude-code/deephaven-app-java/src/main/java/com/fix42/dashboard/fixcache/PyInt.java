package com.fix42.dashboard.fixcache;

import java.math.BigInteger;

/**
 * {@code int(str)} with python's acceptance rules, for the {@code 34 MsgSeqNum} tag.
 *
 * <p>Two differences from {@link Long#parseLong} matter on a wire protocol: python accepts
 * digit-group underscores ({@code int("1_0") == 10}) and python's integers are unbounded.
 *
 * <p>Scanned rather than matched with a regex, for the stack-safety reason in {@link PyDigits}.
 */
final class PyInt {

    private PyInt() {}

    /**
     * Parses {@code text} the way python's {@code int()} does.
     *
     * @param text already-stripped text
     * @return the value, unbounded
     * @throws NumberFormatException if python's {@code int()} would raise {@code ValueError}
     */
    static BigInteger parse(String text) {
        int n = text.length();
        int at = 0;
        if (at < n && (text.charAt(at) == '+' || text.charAt(at) == '-')) {
            at++;
        }
        int end = PyDigits.scanRun(text, at);
        if (end != n) {
            throw new NumberFormatException(text);
        }
        return new BigInteger(PyDigits.toAsciiDigits(text));
    }
}
