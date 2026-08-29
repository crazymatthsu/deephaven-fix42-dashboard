package com.fix42.dashboard.fixcache;

/**
 * python's {@code repr()} for strings, used by the {@code unsupported MsgType: 'X'} error text.
 *
 * <p>The python original interpolates {@code {msg_type!r}}; reproducing the quoting rule keeps the
 * two implementations' error strings identical.
 */
final class PyRepr {

    private PyRepr() {}

    /** Renders {@code value} the way python's {@code repr()} renders a {@code str}. */
    static String of(String value) {
        // python prefers single quotes, switching to double quotes when the value contains a single
        // quote but no double quote.
        char quote = (value.indexOf('\'') >= 0 && value.indexOf('"') < 0) ? '"' : '\'';
        StringBuilder out = new StringBuilder(value.length() + 2).append(quote);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c == quote) {
                        out.append('\\').append(c);
                    } else if (c < 0x20 || c == 0x7f) {
                        out.append(String.format(java.util.Locale.ROOT, "\\x%02x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append(quote).toString();
    }
}
