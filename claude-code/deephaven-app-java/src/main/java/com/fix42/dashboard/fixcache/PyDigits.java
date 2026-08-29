package com.fix42.dashboard.fixcache;

/**
 * Shared scanning helpers for python's numeric-literal grammar.
 *
 * <p>Deliberately hand-written rather than a regex. The obvious pattern for a python digit run is
 * {@code \d(?:_?\d)*}, and {@link java.util.regex.Pattern} implements a starred <em>group</em> by
 * recursion -- one JVM frame per repetition -- so a FIX field carrying a few thousand digits
 * overflows the stack. That matters here far more than it looks: {@code StackOverflowError} is an
 * {@link Error}, not a {@link RuntimeException}, so it would sail past
 * {@code OrderStateMachine.processFields}' catch and out of the Deephaven listener, killing the
 * stream for good. python just raises {@code ValueError} and carries on. These loops are O(n) with
 * a constant stack.
 */
final class PyDigits {

    private PyDigits() {}

    /**
     * True for any Unicode decimal digit, which is what python's {@code float()} and {@code int()}
     * accept -- {@code float("\u0661\u0662")} is 12.0, not a {@code ValueError}.
     *
     * <p>{@link Character#digit(char, int)} with radix 10 is exactly the Nd category (it returns -1
     * for {@code 'a'}-{@code 'f'} at this radix), so it is the right test rather than a
     * {@code '0' <= c <= '9'} range.
     */
    static boolean isDigit(char c) {
        return Character.digit(c, 10) >= 0;
    }

    /**
     * Rewrites Unicode decimal digits as ASCII so the JDK parsers can read them, and drops the
     * PEP 515 underscores.
     *
     * <p>Returns {@code text} itself when nothing needs changing, which is every real FIX message.
     */
    static String toAsciiDigits(String text) {
        boolean needsWork = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '_' || (isDigit(c) && (c < '0' || c > '9'))) {
                needsWork = true;
                break;
            }
        }
        if (!needsWork) {
            return text;
        }
        StringBuilder out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '_') {
                continue;
            }
            int digit = Character.digit(c, 10);
            out.append(digit >= 0 ? (char) ('0' + digit) : c);
        }
        return out.toString();
    }

    /**
     * python's {@code str.strip()}.
     *
     * <p>{@link String#strip()} is <em>not</em> the same function: it strips
     * {@link Character#isWhitespace}, which by definition excludes the non-breaking spaces
     * (U+00A0, U+2007, U+202F) and does not know about U+0085 NEL -- all four of which python's
     * {@code str.isspace()} does strip. A tag value padded with a non-breaking space would
     * otherwise parse in python and be treated as absent here.
     */
    static String strip(String text) {
        int start = 0;
        int end = text.length();
        while (start < end && isPySpace(text.charAt(start))) {
            start++;
        }
        while (end > start && isPySpace(text.charAt(end - 1))) {
            end--;
        }
        return (start == 0 && end == text.length()) ? text : text.substring(start, end);
    }

    /** python's {@code str.isspace()} for a single char. */
    private static boolean isPySpace(char c) {
        // isWhitespace covers the ASCII controls and the separators that may break;
        // isSpaceChar adds the non-breaking separators; U+0085 NEL is in neither.
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == 0x0085;
    }

    /**
     * Scans a python digit run -- digits with single underscores <em>between</em> them (PEP 515).
     *
     * @param text the string being parsed
     * @param from the index to start at
     * @return the index one past the run, or {@code -1} when {@code from} is not a digit. An
     *     underscore not followed by a digit ends the run, which leaves trailing text for the
     *     caller's full-match check to reject -- exactly as python rejects {@code "1__0"}.
     */
    static int scanRun(String text, int from) {
        int n = text.length();
        if (from >= n || !isDigit(text.charAt(from))) {
            return -1;
        }
        int i = from + 1;
        while (i < n) {
            char c = text.charAt(i);
            if (isDigit(c)) {
                i++;
            } else if (c == '_' && i + 1 < n && isDigit(text.charAt(i + 1))) {
                i += 2;
            } else {
                break;
            }
        }
        return i;
    }

    /** ASCII case-insensitive equality, for the {@code inf}/{@code infinity}/{@code nan} spellings. */
    static boolean equalsIgnoreCaseAscii(String text, String lowercase) {
        if (text.length() != lowercase.length()) {
            return false;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                c += 32;
            }
            if (c != lowercase.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
