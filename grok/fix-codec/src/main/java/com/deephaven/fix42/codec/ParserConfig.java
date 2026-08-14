package com.deephaven.fix42.codec;

public final class ParserConfig {
    private final boolean strictHeader;
    private final boolean validateChecksum;

    public ParserConfig(boolean strictHeader, boolean validateChecksum) {
        this.strictHeader = strictHeader;
        this.validateChecksum = validateChecksum;
    }

    public static ParserConfig defaults() {
        return new ParserConfig(true, true);
    }

    public static ParserConfig lenient() {
        return new ParserConfig(false, false);
    }

    public boolean strictHeader() {
        return strictHeader;
    }

    public boolean validateChecksum() {
        return validateChecksum;
    }
}
