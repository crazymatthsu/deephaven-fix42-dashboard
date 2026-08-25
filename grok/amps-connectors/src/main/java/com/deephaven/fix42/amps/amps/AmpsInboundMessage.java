package com.deephaven.fix42.amps.amps;

public final class AmpsInboundMessage {
    public enum Kind {
        SOW,
        PUBLISH,
        OOF,
        OTHER
    }

    private final Kind kind;
    private final String command;
    private final String sowKey;
    private final String data;

    public AmpsInboundMessage(Kind kind, String command, String sowKey, String data) {
        this.kind = kind;
        this.command = command == null ? "" : command;
        this.sowKey = sowKey == null ? "" : sowKey;
        this.data = data == null ? "" : data;
    }

    public static AmpsInboundMessage of(String command, String sowKey, String data) {
        return new AmpsInboundMessage(kindOf(command), command, sowKey, data);
    }

    public Kind kind() {
        return kind;
    }

    public String command() {
        return command;
    }

    public String sowKey() {
        return sowKey;
    }

    public String data() {
        return data;
    }

    public boolean isSowSnapshot() {
        return kind == Kind.SOW;
    }

    public boolean isOof() {
        return kind == Kind.OOF;
    }

    public boolean isData() {
        return kind == Kind.SOW || kind == Kind.PUBLISH;
    }

    static Kind kindOf(String command) {
        if (command == null) {
            return Kind.OTHER;
        }
        String c = command.trim();
        if (c.equalsIgnoreCase("sow")) {
            return Kind.SOW;
        }
        if (c.equalsIgnoreCase("publish") || c.equalsIgnoreCase("p")) {
            return Kind.PUBLISH;
        }
        if (c.equalsIgnoreCase("oof")) {
            return Kind.OOF;
        }
        return Kind.OTHER;
    }
}
