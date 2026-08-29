package com.fix42.dashboard.fixcache;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lenient FIX 4.2 wire-format helpers (doc 01 section 1) -- port of {@code fix42cache.parser}.
 *
 * <p>The parser never rejects a message: framing on audit/drop-copy feeds is routinely stripped or
 * rewritten, so validation results are <em>recorded</em> ({@code ChecksumOk}) rather than enforced.
 */
public final class FixParser {

    /** The real FIX field delimiter (ASCII SOH, 0x01). */
    public static final char SOH = 0x01;

    /** Display/fixture delimiter accepted by the parser. */
    public static final char PIPE = '|';

    private static final String CHECKSUM_MARKER = "10=";

    private FixParser() {}

    /**
     * Parse a raw FIX 4.2 message into {@code {tag: value}}.
     *
     * <ul>
     *   <li>Accepts either the SOH delimiter or {@code |} (fixtures/tests). If any SOH is present the
     *       message is split on SOH only, so a {@code |} inside a free-text field is preserved.
     *   <li>Each field is split on its <b>first</b> {@code =} only (values may contain {@code =}).
     *   <li>Empty segments and segments whose tag is not numeric are skipped.
     *   <li>Repeated tags: the last occurrence wins. The returned map keeps <em>insertion</em> order
     *       (a repeat updates in place), matching python dict semantics, because
     *       {@link #renderFields(Map)} publishes that order.
     * </ul>
     *
     * @param raw the wire string, possibly {@code null}
     * @return an insertion-ordered map of tag to value; never {@code null}
     */
    public static Map<Integer, String> parseFix(String raw) {
        Map<Integer, String> fields = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return fields;
        }
        String text = rstripNewlines(raw);
        char delimiter = text.indexOf(SOH) >= 0 ? SOH : PIPE;
        for (String segment : split(text, delimiter)) {
            if (segment.isEmpty()) {
                continue;
            }
            int equals = segment.indexOf('=');
            if (equals < 0) {
                continue;
            }
            Integer tag = asTag(PyDigits.strip(segment.substring(0, equals)));
            if (tag == null) {
                continue;
            }
            fields.put(tag, segment.substring(equals + 1));
        }
        return fields;
    }

    /** Render a raw message for display: SOH delimiters become {@code |}. */
    public static String renderPipe(String raw) {
        return raw == null ? "" : raw.replace(SOH, PIPE);
    }

    /**
     * Render {@code {tag: value}} back to a pipe-delimited string (audit display).
     *
     * <p>Used when the state machine is fed pre-parsed fields and therefore has no original wire
     * string to publish. Field order follows the map order; no {@code 9}/{@code 10} framing is
     * recomputed.
     */
    public static String renderFields(Map<Integer, String> fields) {
        StringBuilder out = new StringBuilder();
        for (Map.Entry<Integer, String> entry : fields.entrySet()) {
            if (out.length() > 0) {
                out.append(PIPE);
            }
            out.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return out.toString();
    }

    /**
     * Validate tag 10 CheckSum.
     *
     * <p>The checksum is {@code sum(bytes) % 256} over everything up to <b>and including</b> the SOH
     * preceding {@code 10=}. Pipe-delimited fixtures are normalised back to SOH before summing, so a
     * message checksums identically in either delimiter form.
     *
     * @param raw the wire string
     * @return {@code TRUE}/{@code FALSE}, or {@code null} when the message carries no tag 10
     */
    public static Boolean checksumOk(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        String text = rstripNewlines(raw);
        if (text.indexOf(SOH) < 0 && text.indexOf(PIPE) >= 0) {
            text = text.replace(PIPE, SOH);
        }

        int bodyEnd;
        int valueAt;
        int markerAt = text.lastIndexOf(SOH + CHECKSUM_MARKER);
        if (markerAt >= 0) {
            bodyEnd = markerAt + 1; // include the SOH itself
            valueAt = markerAt + 1 + CHECKSUM_MARKER.length();
        } else if (text.startsWith(CHECKSUM_MARKER)) {
            bodyEnd = 0;
            valueAt = CHECKSUM_MARKER.length();
        } else {
            return null;
        }

        int valueEnd = text.indexOf(SOH, valueAt);
        String declared = PyDigits.strip(valueEnd < 0 ? text.substring(valueAt) : text.substring(valueAt, valueEnd));
        if (!isDigits(declared)) {
            return Boolean.FALSE;
        }
        int computed = 0;
        for (byte b : text.substring(0, bodyEnd).getBytes(StandardCharsets.UTF_8)) {
            computed += (b & 0xFF);
        }
        java.math.BigInteger declaredValue = PyInt.parse(declared);
        return declaredValue.equals(java.math.BigInteger.valueOf(computed % 256));
    }

    /**
     * Parse {@code YYYYMMDD-HH:MM:SS} with optional {@code .sss} into a UTC instant.
     *
     * <p>Used for both tag 60 TransactTime and tag 52 SendingTime.
     *
     * @param value the raw tag value, possibly {@code null}
     * @return the instant, or {@code null} for missing/empty/unparseable values
     */
    public static Instant parseTransactTime(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return PyStrptime.parseUtc(PyDigits.strip(value));
    }

    // ------------------------------------------------------------------ helpers

    /** python's {@code raw.rstrip("\r\n")}. */
    private static String rstripNewlines(String text) {
        int end = text.length();
        while (end > 0) {
            char c = text.charAt(end - 1);
            if (c != '\r' && c != '\n') {
                break;
            }
            end--;
        }
        return end == text.length() ? text : text.substring(0, end);
    }

    /** python's {@code str.split(sep)}: no regex, no trailing-empty removal. */
    private static List<String> split(String text, char delimiter) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == delimiter) {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start));
        return parts;
    }

    /**
     * python's {@code tag_text.isdigit()} then {@code int(tag_text)}.
     *
     * <p>The length test is applied to the tag's <em>value</em>, not to the raw text: a FIX dialect
     * that zero-pads its tags sends {@code 0000000035=D}, which python reads as tag 35, and a test
     * on the raw length would silently drop the field -- taking the whole message's state effect
     * with it when the dropped field is tag 35.
     *
     * <p>Two documented limits, both about a tag python <em>raises</em> on rather than one it reads
     * differently. python's {@code parse_fix} calls {@code int(tag_text)} unguarded, so a
     * {@code ValueError} there propagates out and makes the <em>whole message</em> unparseable;
     * this parser skips the field and reads the rest. That happens for:
     *
     * <ol>
     *   <li>a digit run genuinely longer than an {@code int}, which cannot key the
     *       {@code Map<Integer, String>} this parser returns at all;
     *   <li>a character where python's {@code str.isdigit()} is true but {@code int()} still
     *       raises -- {@code Numeric_Type=Digit} but not {@code Decimal}, such as the superscripts.
     *       {@code java.lang.Character} exposes no {@code Numeric_Type} accessor, so reproducing
     *       that set means shipping a Unicode table for the 95 BMP code points involved; on an
     *       ASCII wire protocol that is not a trade worth making.
     * </ol>
     *
     * <p>Both are asserted in {@code FixParserTest} so the deviation is pinned, not merely
     * described. Every tag either implementation actually <em>reads</em> is read identically,
     * including zero-padded and non-ASCII decimal digits.
     */
    private static Integer asTag(String tagText) {
        if (!isDigits(tagText)) {
            return null;
        }
        String digits = PyDigits.toAsciiDigits(tagText);
        int firstSignificant = 0;
        while (firstSignificant < digits.length() - 1 && digits.charAt(firstSignificant) == '0') {
            firstSignificant++;
        }
        String significant = digits.substring(firstSignificant);
        if (significant.length() > 9) {
            return null;
        }
        return Integer.valueOf(significant);
    }

    /** Every character is a Unicode decimal digit, as python's {@code str.isdigit()} requires. */
    private static boolean isDigits(String text) {
        if (text.isEmpty()) {
            return false;
        }
        for (int i = 0; i < text.length(); i++) {
            if (!PyDigits.isDigit(text.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
