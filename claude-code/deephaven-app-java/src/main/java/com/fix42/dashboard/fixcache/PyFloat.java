package com.fix42.dashboard.fixcache;

import java.util.regex.Pattern;

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
 * <p>The pattern below is python's float grammar, so both implementations accept exactly the same
 * strings.
 */
final class PyFloat {

    private static final Pattern PY_FLOAT = Pattern.compile(
            "[+-]?(?:(?:inf(?:inity)?|nan)"
                    + "|(?:\\d(?:_?\\d)*(?:\\.(?:\\d(?:_?\\d)*)?)?|\\.\\d(?:_?\\d)*)"
                    + "(?:[eE][+-]?\\d(?:_?\\d)*)?)",
            Pattern.CASE_INSENSITIVE);

    private PyFloat() {}

    /**
     * Parses {@code text} the way python's {@code float()} does.
     *
     * @param text already-stripped text
     * @return the parsed value
     * @throws NumberFormatException if python's {@code float()} would raise {@code ValueError}
     */
    static double parse(String text) {
        if (!PY_FLOAT.matcher(text).matches()) {
            throw new NumberFormatException(text);
        }
        String normalized = text.replace("_", "");
        String bare = normalized.startsWith("+") || normalized.startsWith("-")
                ? normalized.substring(1)
                : normalized;
        double magnitude;
        if (bare.equalsIgnoreCase("inf") || bare.equalsIgnoreCase("infinity")) {
            magnitude = Double.POSITIVE_INFINITY;
        } else if (bare.equalsIgnoreCase("nan")) {
            magnitude = Double.NaN;
        } else {
            magnitude = Double.parseDouble(bare);
        }
        return normalized.startsWith("-") ? -magnitude : magnitude;
    }
}
