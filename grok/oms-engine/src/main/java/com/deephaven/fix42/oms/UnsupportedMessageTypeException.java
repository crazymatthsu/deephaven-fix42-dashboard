package com.deephaven.fix42.oms;

public final class UnsupportedMessageTypeException extends RuntimeException {
    public UnsupportedMessageTypeException(String msgType) {
        super("unsupported MsgType: " + msgType);
    }
}
