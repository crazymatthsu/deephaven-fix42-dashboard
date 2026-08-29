package com.fix42.dashboard.fixcache;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Python-compatible number rendering for the human-readable {@code Detail} strings.
 *
 * <p>The python original is three lines:
 *
 * <pre>
 * def _num(value: float) -&gt; str:
 *     if value == int(value):
 *         return str(int(value))
 *     return f"{value:.6g}"
 * </pre>
 *
 * <p>Both branches need care in Java, because the unit tests assert on these strings verbatim:
 *
 * <ul>
 *   <li>{@code str(int(v))} truncates the <em>exact binary</em> value of the double toward zero with
 *       unbounded precision -- {@code str(int(1.2345678901234567e19))} is
 *       {@code "12345678901234567168"}, not the decimal literal. {@link BigDecimal#BigDecimal(double)}
 *       plus {@link BigDecimal#toBigInteger()} reproduces that exactly; a {@code (long)} cast would
 *       silently saturate.
 *   <li>{@code "%.6g"} differs between the two languages: Java's {@code String.format("%.6g", 185.5)}
 *       yields {@code "185.500"} where python yields {@code "185.5"}, because python strips trailing
 *       zeros from a {@code g} conversion and Java does not. {@link #format6g(double)} reimplements
 *       python's rule: round to 6 significant digits (half-even), choose scientific notation when the
 *       decimal exponent is {@code < -4} or {@code >= 6}, then strip trailing zeros.
 * </ul>
 */
public final class PyNum {

    /** Significant digits of python's default {@code g} presentation used by {@code _num}. */
    private static final int PRECISION = 6;

    private static final MathContext MC = new MathContext(PRECISION, RoundingMode.HALF_EVEN);

    private PyNum() {}

    /**
     * Render a double the way {@code fix42cache.state_machine._num} does.
     *
     * <p>python's {@code value == int(value)} test <em>raises</em> on a non-finite value, and the
     * state machine lets that propagate into {@code Result.error}. Wire data can reach here: a FIX
     * message carrying {@code 38=nan} or {@code 38=inf} parses fine (python's {@code float()}
     * accepts both spellings) and only fails when the {@code Detail} string is rendered. Throwing
     * the matching {@link PyException} keeps the two implementations' error strings identical --
     * and the chain mutations made before this point survive in both, exactly as they do in python.
     *
     * @param value the value
     * @return the rendered value
     * @throws PyException if python's {@code int(value)} would raise
     */
    public static String num(double value) {
        if (Double.isNaN(value)) {
            throw PyException.valueErrorNanToInteger();
        }
        if (Double.isInfinite(value)) {
            throw PyException.overflowErrorInfinityToInteger();
        }
        if (value == Math.floor(value)) {
            // python: str(int(value)) -- exact, unbounded, truncating toward zero.
            return new BigDecimal(value).toBigInteger().toString();
        }
        return format6g(value);
    }

    /**
     * Python's {@code format(value, '.6g')}.
     *
     * @param value a finite double
     * @return the value at 6 significant digits, trailing zeros stripped, in fixed or scientific
     *     notation per python's rule
     */
    public static String format6g(double value) {
        if (value == 0.0) {
            return "0";
        }
        BigDecimal rounded = new BigDecimal(value).round(MC);
        // Decimal exponent of the rounded value: floor(log10(|rounded|)), computed exactly.
        int exponent = rounded.precision() - rounded.scale() - 1;
        if (exponent < -4 || exponent >= PRECISION) {
            BigDecimal mantissa = rounded.movePointLeft(exponent).stripTrailingZeros();
            String digits = mantissa.toPlainString();
            int magnitude = Math.abs(exponent);
            String exponentDigits = magnitude < 10 ? "0" + magnitude : Integer.toString(magnitude);
            return digits + "e" + (exponent < 0 ? "-" : "+") + exponentDigits;
        }
        BigDecimal plain = rounded.stripTrailingZeros();
        if (plain.scale() < 0) {
            plain = plain.setScale(0);
        }
        return plain.toPlainString();
    }
}
