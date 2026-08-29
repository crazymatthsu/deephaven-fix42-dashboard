package com.fix42.dashboard.fixcache;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * python's {@code datetime.strptime} for the two timestamp layouts this project accepts.
 *
 * <p>{@link java.time.format.DateTimeFormatter} is <b>not</b> a drop-in substitute: python's
 * {@code %m}, {@code %d}, {@code %H}, {@code %M} and {@code %S} each accept <em>one or two</em>
 * digits, so python reads {@code "2024011-14:30:00"} as 2024-01-01T14:30 while a
 * {@code uuuuMMdd} formatter rejects it outright. On an audit feed that difference is a silently
 * divergent {@code TransactTime} column, so the patterns below are CPython's own
 * {@code Lib/_strptime.py} regex fragments, in CPython's alternation order:
 *
 * <pre>
 * 'Y': (?P&lt;Y&gt;\d\d\d\d)                  'H': (?P&lt;H&gt;2[0-3]|[0-1]\d|\d)
 * 'm': (?P&lt;m&gt;1[0-2]|0[1-9]|[1-9])       'M': (?P&lt;M&gt;[0-5]\d|\d)
 * 'd': (?P&lt;d&gt;3[01]|[12]\d|0[1-9]|[1-9]) 'S': (?P&lt;S&gt;6[0-1]|[0-5]\d|\d)
 * 'f': (?P&lt;f&gt;[0-9]{1,6})
 * </pre>
 *
 * <p>Out-of-range combinations the regexes still admit -- 30 February, second 60 -- are rejected by
 * construction, which is what python's {@code datetime(...)} does too.
 *
 * <p>One deliberate, documented difference: python matches without anchoring the end and then
 * rejects any leftover text, whereas {@link Matcher#matches()} anchors the end and so may backtrack
 * to a longer parse. The two disagree only if a string has both a short first match and a distinct
 * full-length match; the separators in these layouts make that unreachable, and the fuzz check in
 * {@code PyStrptimeFuzzTest} exercises the claim.
 */
final class PyStrptime {

    private static final String Y = "(\\d\\d\\d\\d)";
    private static final String M = "(1[0-2]|0[1-9]|[1-9])";
    private static final String D = "(3[01]|[12]\\d|0[1-9]|[1-9])";
    private static final String HH = "(2[0-3]|[0-1]\\d|\\d)";
    private static final String MI = "([0-5]\\d|\\d)";
    private static final String SS = "(6[0-1]|[0-5]\\d|\\d)";
    private static final String FRACTION = "([0-9]{1,6})";

    /** python's {@code datetime.MINYEAR}; {@code java.time} would happily accept year 0. */
    private static final int MIN_YEAR = 1;

    /** python's {@code "%Y%m%d-%H:%M:%S.%f"}. */
    private static final Pattern WITH_FRACTION =
            Pattern.compile(Y + M + D + "-" + HH + ":" + MI + ":" + SS + "\\." + FRACTION);

    /** python's {@code "%Y%m%d-%H:%M:%S"}. */
    private static final Pattern WITHOUT_FRACTION =
            Pattern.compile(Y + M + D + "-" + HH + ":" + MI + ":" + SS);

    private PyStrptime() {}

    /**
     * Parses {@code text} as UTC, trying the fractional layout first (python's loop order).
     *
     * @param text the already-stripped tag value
     * @return the instant, or {@code null} when neither layout matches or the fields are out of range
     */
    static Instant parseUtc(String text) {
        Instant withFraction = attempt(WITH_FRACTION, text, true);
        return withFraction != null ? withFraction : attempt(WITHOUT_FRACTION, text, false);
    }

    private static Instant attempt(Pattern pattern, String text, boolean hasFraction) {
        Matcher m = pattern.matcher(text);
        if (!m.matches()) {
            return null;
        }
        int nanos = 0;
        if (hasFraction) {
            // python: microseconds are right-padded to 6 digits ("1" means 100000us).
            String digits = (m.group(7) + "000000").substring(0, 6);
            nanos = Integer.parseInt(digits) * 1000;
        }
        int year = Integer.parseInt(m.group(1));
        if (year < MIN_YEAR) {
            // java.time accepts the proleptic year 0 (1 BC); python's datetime.MINYEAR is 1, so
            // "00000101-00:00:00" must be rejected here to keep the two in step.
            return null;
        }
        try {
            LocalDate date =
                    LocalDate.of(year, Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)));
            LocalTime time = LocalTime.of(
                    Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)), Integer.parseInt(m.group(6)), nanos);
            return LocalDateTime.of(date, time).toInstant(ZoneOffset.UTC);
        } catch (DateTimeException outOfRange) {
            // 30 February, second 60: python's datetime(...) raises here too.
            return null;
        }
    }
}
