package com.fix42.dashboard.connectors.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The on-disk form of a spool entry round-trips every shape a record can take. */
class SpoolCodecTest {

    private static SpoolEntry roundTrip(SpoolEntry entry) throws IOException {
        byte[] bytes = SpoolCodec.encode(entry);
        return SpoolCodec.decode(bytes, 0, bytes.length);
    }

    @Test
    void roundTripsASimpleRecord() throws IOException {
        SourceRecord record = SourceRecord.of("11=C-1\u000155=AAPL", "SOW-1");
        assertThat(roundTrip(SpoolEntry.of(record)).record()).isEqualTo(record);
    }

    @Test
    @DisplayName("null key, null data and an empty payload are all distinct after decoding")
    void keepsNullsApartFromEmptyStrings() throws IOException {
        assertThat(roundTrip(SpoolEntry.of(SourceRecord.of("{}"))).record())
                .isEqualTo(SourceRecord.of("{}"));
        assertThat(roundTrip(SpoolEntry.of(SourceRecord.delete("", "K"))).record())
                .isEqualTo(SourceRecord.delete("", "K"));
        assertThat(roundTrip(SpoolEntry.of(new SourceRecord(null, null, "K", SourceRecord.Action.DELETE))).record())
                .isEqualTo(new SourceRecord(null, null, "K", SourceRecord.Action.DELETE));
    }

    @Test
    void roundTripsCompositeParts() throws IOException {
        SourceRecord record = SourceRecord.composite(
                List.of("{\"orderId\":\"O-1\"}", "54=1\u000138=250\u0001", "", "x".repeat(70_000)),
                "SOW-9", SourceRecord.Action.UPSERT);
        assertThat(roundTrip(SpoolEntry.of(record)).record()).isEqualTo(record);
    }

    @Test
    void roundTripsUnicodeAsUtf8() throws IOException {
        SourceRecord record = SourceRecord.of("{\"note\":\"café → ünïcode ✓\"}", "ключ");
        assertThat(roundTrip(SpoolEntry.of(record)).record()).isEqualTo(record);
    }

    @Test
    void roundTripsTheSnapshotBoundaries() throws IOException {
        assertThat(roundTrip(SpoolEntry.snapshotBegin()).kind()).isEqualTo(SpoolEntry.Kind.SNAPSHOT_BEGIN);
        assertThat(roundTrip(SpoolEntry.snapshotEnd()).kind()).isEqualTo(SpoolEntry.Kind.SNAPSHOT_END);
        assertThat(SpoolCodec.encode(SpoolEntry.snapshotBegin())).hasSize(1);
    }

    @Test
    void rejectsAnUnknownKind() {
        byte[] bytes = {42};
        assertThatThrownBy(() -> SpoolCodec.decode(bytes, 0, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown spool entry kind");
    }
}
