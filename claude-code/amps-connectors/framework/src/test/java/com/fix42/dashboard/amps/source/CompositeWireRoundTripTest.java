package com.fix42.dashboard.amps.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.crankuptheamps.client.CompositeMessageBuilder;
import com.crankuptheamps.client.CompositeMessageParser;
import com.crankuptheamps.client.fields.Field;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the wire contract {@code AmpsClientSubscriber} relies on: what the 60East
 * {@code CompositeMessageBuilder} frames (4-byte binary length prefix per part), the
 * {@code CompositeMessageParser} unframes -- byte-exactly, through the raw field rather than
 * a string, which is the reason the subscriber parses the {@code Message} instead of
 * {@code getData()}.
 */
class CompositeWireRoundTripTest {

    private static final char SOH = (char) 0x01;

    @Test
    @DisplayName("builder -> parser round-trips every part byte-exactly")
    void roundTripsParts() {
        String json = "{\"orderId\":\"O-1\",\"note\":\"café → here\"}";
        String fix = "54=1" + SOH + "38=250" + SOH;
        // Long enough that the frame's length bytes exceed one byte -- the case a string
        // decode of the whole payload would mangle.
        String big = "x".repeat(70_000);

        CompositeMessageBuilder builder = new CompositeMessageBuilder();
        builder.append(json);
        builder.append(fix);
        byte[] bigBytes = big.getBytes(StandardCharsets.UTF_8);
        builder.append(bigBytes, 0, bigBytes.length);

        Field wire = new Field();
        builder.setField(wire);

        CompositeMessageParser parser = new CompositeMessageParser();
        int parts = parser.parse(wire);

        assertThat(parts).isEqualTo(3);
        assertThat(parser.getString(0, StandardCharsets.UTF_8)).isEqualTo(json);
        assertThat(parser.getString(1, StandardCharsets.UTF_8)).isEqualTo(fix);
        assertThat(parser.getString(2, StandardCharsets.UTF_8)).isEqualTo(big);
    }

    @Test
    @DisplayName("an empty field parses as zero parts, the empty-delete-body case")
    void emptyFieldHasNoParts() {
        CompositeMessageParser parser = new CompositeMessageParser();
        assertThat(parser.parse(new Field(""))).isZero();
    }
}
