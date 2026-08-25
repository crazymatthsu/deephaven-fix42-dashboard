package com.deephaven.fix42.amps.decode;

public interface MessageDecoder {
    ParsedFields decode(String payload);
}
