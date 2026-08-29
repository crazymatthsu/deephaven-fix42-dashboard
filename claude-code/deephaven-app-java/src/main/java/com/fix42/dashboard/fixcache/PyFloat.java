package com.fix42.dashboard.fixcache;

/**
 * {@code float(str)} with python's acceptance rules, for FIX numeric tag values.
 *
 * <p>Java's {@link Double#parseDouble} is <em>more</em> permissive than python's {@code float()} in
 * exactly one way that matters on a wire protocol: it accepts a trailing type suffix and hex
 * literals, so {@code "10d"}, {@code "5f"} and {@code "0x1p3"} parse to numbers instead of raising.
 * A FIX field like {@code 38=100d} must be rejected the way the python original rejects it, or the
 * Java cache would silently adopt a quantity the python cache treated as absent. It is also
 * <em>less</em> permissive in two: python accepts {@code inf}/{@code nan} spellings and digit-group
 * underscores.
 *
 * <p>The grammar below is python's, scanned rather than matched with a regex -- see {@link PyDigits}
 * for why that distinction is load-bearing.
 *
 * <pre>
 * [+-]? ( inf | infinity | nan | number )
 * number: run ['.' [run]] [exponent] | '.' run [exponent]
 * exponent: [eE] [+-]? run
 * run: digit (['_'] digit)*
 * </pre>
 */
final class PyFloat {

    private PyFloat() {}

    /**
     * Parses {@code text} the way python's {@code float()} does.
     *
     * @param text already-stripped text
     * @return the parsed value
     * @throws NumberFormatException if python's {@code float()} would raise {@code ValueError}
     */
    static double parse(String text) {
        int n = text.length();
        int i = 0;
        boolean negative = false;
        if (i < n && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
            negative = text.charAt(i) == '-';
            i++;
        }

        String magnitude = text.substring(i);
        if (PyDigits.equalsIgnoreCaseAscii(magnitude, "inf")
                || PyDigits.equalsIgnoreCaseAscii(magnitude, "infinity")) {
            return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        if (PyDigits.equalsIgnoreCaseAscii(magnitude, "nan")) {
            return Double.NaN;
        }

        int at = i;
        int integerEnd = PyDigits.scanRun(text, at);
        boolean hasInteger = integerEnd > 0;
        if (hasInteger) {
            at = integerEnd;
        }
        if (at < n && text.charAt(at) == '.') {
            at++;
            int fractionEnd = PyDigits.scanRun(text, at);
            if (fractionEnd > 0) {
                at = fractionEnd;
            } else if (!hasInteger) {
                throw new NumberFormatException(text); // "." with digits on neither side
            }
        } else if (!hasInteger) {
            throw new NumberFormatException(text);
        }
        if (at < n && (text.charAt(at) == 'e' || text.charAt(at) == 'E')) {
            int exponentAt = at + 1;
            if (exponentAt < n && (text.charAt(exponentAt) == '+' || text.charAt(exponentAt) == '-')) {
                exponentAt++;
            }
            int exponentEnd = PyDigits.scanRun(text, exponentAt);
            if (exponentEnd < 0) {
                throw new NumberFormatException(text); // an exponent marker with no digits
            }
            at = exponentEnd;
        }
        if (at != n) {
            throw new NumberFormatException(text);
        }
        return Double.parseDouble(PyDigits.toAsciiDigits(text));
    }
}
