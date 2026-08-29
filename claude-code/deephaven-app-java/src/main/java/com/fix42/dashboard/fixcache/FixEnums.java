package com.fix42.dashboard.fixcache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The str-valued FIX 4.2 enumerations (doc 01 section 2), ported from
 * {@code fix42cache.fixtags}.
 *
 * <p>Each enum's <em>code</em> is the raw FIX value and its <em>name</em> is the readable label
 * published in Deephaven rows ({@code PARTIALLY_FILLED} rather than {@code 1}). Every
 * {@code fromFix} is deliberately lenient: an unknown or missing code maps to the {@code UNKNOWN}
 * sentinel instead of throwing, because the cache must never reject a message from an audit feed.
 *
 * <p>The python original returns {@code None} for an absent tag and {@code UNKNOWN} for a present
 * but unrecognised one; {@code fromFix(null)} here returns {@code UNKNOWN} to match
 * {@code _FixEnum.from_fix}, while the state machine's own {@code enumOf} helper returns
 * {@code null} for an absent tag (mirroring the python {@code _enum} helper).
 */
public final class FixEnums {

    /** Value of every enum's {@code UNKNOWN} sentinel; never a valid FIX code. */
    public static final String UNKNOWN_CODE = "?";

    private FixEnums() {}

    private static <E extends Enum<E>> Map<String, E> index(E[] values, java.util.function.Function<E, String> code) {
        Map<String, E> map = new HashMap<>();
        for (E value : values) {
            map.put(code.apply(value), value);
        }
        return Collections.unmodifiableMap(map);
    }

    /** Tag 39 OrdStatus. */
    public enum OrdStatus {
        NEW("0"),
        PARTIALLY_FILLED("1"),
        FILLED("2"),
        DONE_FOR_DAY("3"),
        CANCELED("4"),
        REPLACED("5"),
        PENDING_CANCEL("6"),
        REJECTED("8"),
        PENDING_NEW("A"),
        EXPIRED("C"),
        PENDING_REPLACE("E"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, OrdStatus> BY_CODE = index(values(), OrdStatus::code);
        private final String code;

        OrdStatus(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static OrdStatus fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 150 ExecType. */
    public enum ExecType {
        NEW("0"),
        PARTIAL_FILL("1"),
        FILL("2"),
        DONE_FOR_DAY("3"),
        CANCELED("4"),
        REPLACED("5"),
        PENDING_CANCEL("6"),
        REJECTED("8"),
        PENDING_NEW("A"),
        EXPIRED("C"),
        RESTATED("D"),
        PENDING_REPLACE("E"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, ExecType> BY_CODE = index(values(), ExecType::code);
        private final String code;

        ExecType(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static ExecType fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 20 ExecTransType (0=New, 1=Cancel/bust, 2=Correct, 3=Status). */
    public enum ExecTransType {
        NEW("0"),
        CANCEL("1"),
        CORRECT("2"),
        STATUS("3"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, ExecTransType> BY_CODE = index(values(), ExecTransType::code);
        private final String code;

        ExecTransType(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static ExecTransType fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 54 Side. */
    public enum Side {
        BUY("1"),
        SELL("2"),
        SELL_SHORT("5"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, Side> BY_CODE = index(values(), Side::code);
        private final String code;

        Side(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static Side fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 40 OrdType. */
    public enum OrdType {
        MARKET("1"),
        LIMIT("2"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, OrdType> BY_CODE = index(values(), OrdType::code);
        private final String code;

        OrdType(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static OrdType fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 59 TimeInForce. */
    public enum TimeInForce {
        DAY("0"),
        GTC("1"),
        IOC("3"),
        FOK("4"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, TimeInForce> BY_CODE = index(values(), TimeInForce::code);
        private final String code;

        TimeInForce(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static TimeInForce fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Tag 434 CxlRejResponseTo (1 = a cancel {@code F} was rejected, 2 = a replace {@code G}). */
    public enum CxlRejResponseTo {
        ORDER_CANCEL_REQUEST("1"),
        ORDER_CANCEL_REPLACE_REQUEST("2"),
        UNKNOWN(UNKNOWN_CODE);

        private static final Map<String, CxlRejResponseTo> BY_CODE = index(values(), CxlRejResponseTo::code);
        private final String code;

        CxlRejResponseTo(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public static CxlRejResponseTo fromFix(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return BY_CODE.getOrDefault(code, UNKNOWN);
        }
    }

    /** Statuses that make an order terminal (doc 01 section 4, {@code Terminal} column). */
    public static final Set<OrdStatus> TERMINAL_STATUSES = Collections.unmodifiableSet(
            java.util.EnumSet.of(
                    OrdStatus.FILLED,
                    OrdStatus.CANCELED,
                    OrdStatus.REJECTED,
                    OrdStatus.EXPIRED,
                    OrdStatus.DONE_FOR_DAY));

    /** True when {@code status} is one of the terminal statuses ({@code null} is not terminal). */
    public static boolean isTerminal(OrdStatus status) {
        return status != null && TERMINAL_STATUSES.contains(status);
    }
}
