package com.fix42.dashboard.amps.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeltaRowMergerTest {

    private static final Instant INGEST = Instant.parse("2024-01-15T14:30:00Z");

    private TableSchema schema;
    private FieldMapper mapper;
    private DeltaRowMerger merger;

    @BeforeEach
    void setUp() {
        schema = TableSchema.of(TestConnectors.nvfixPositions());
        mapper = new FieldMapper(schema);
        merger = new DeltaRowMerger(schema);
    }

    private MappedRow map(Map<String, String> fields) {
        return mapper.map(AmpsRecord.of("payload", "SOW-1"), fields, INGEST);
    }

    @Test
    @DisplayName("a delta keeps the columns it does not mention")
    void mergesOverThePreviousRow() {
        merger.merge(map(Map.of(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100", "AvgCost", "185.5")));

        MappedRow merged = merger.merge(map(Map.of(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "150")));

        assertThat(merged.values()[schema.indexOf("Quantity")]).isEqualTo(150.0d);
        assertThat(merged.values()[schema.indexOf("AvgCost")]).isEqualTo(185.5d);
    }

    @Test
    @DisplayName("an explicitly empty field clears the stored value")
    void anExplicitBlankOverwrites() {
        merger.merge(map(Map.of(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100", "AvgCost", "185.5")));

        MappedRow merged = merger.merge(map(Map.of(
                "Account", "ACC-1", "Symbol", "AAPL", "AvgCost", "")));

        assertThat(merged.values()[schema.indexOf("AvgCost")]).isNull();
        assertThat(merged.values()[schema.indexOf("Quantity")]).isEqualTo(100.0d);
    }

    @Test
    void keysAreTrackedIndependently() {
        merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100")));
        merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "MSFT", "Quantity", "200")));

        MappedRow apple = merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "AvgCost", "1.0")));
        assertThat(apple.values()[schema.indexOf("Quantity")]).isEqualTo(100.0d);
        assertThat(merger.size()).isEqualTo(2);
    }

    @Test
    void theFirstUpdateForAKeyPassesThroughUnchanged() {
        MappedRow first = merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100")));
        assertThat(first.values()[schema.indexOf("AvgCost")]).isNull();
        assertThat(first.values()[schema.indexOf("Quantity")]).isEqualTo(100.0d);
    }

    @Test
    @DisplayName("after a delete the key starts fresh rather than resurrecting old columns")
    void aDeleteForgetsTheKey() {
        merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100", "AvgCost", "185.5")));
        merger.merge(mapper.map(
                AmpsRecord.delete("", "SOW-1"),
                Map.of("Account", "ACC-1", "Symbol", "AAPL"),
                INGEST));
        assertThat(merger.size()).isZero();

        MappedRow reborn = merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "5")));
        assertThat(reborn.values()[schema.indexOf("AvgCost")]).isNull();
    }

    @Test
    @DisplayName("a merged row is fully present, so it can be published as a whole row")
    void mergedRowsAreFullyPresent() {
        merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100")));
        MappedRow merged = merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "AvgCost", "1.0")));
        assertThat(merged.present()).containsOnly(true);
    }

    @Test
    void clearForgetsEverything() {
        merger.merge(map(Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100")));
        merger.clear();
        assertThat(merger.size()).isZero();
    }

    @Test
    @DisplayName("an unkeyed schema has nothing to merge into, so rows pass through")
    void unkeyedRowsPassThrough() {
        TableSchema journal = TableSchema.of(TestConnectors.jsonTrades());
        FieldMapper journalMapper = new FieldMapper(journal);
        DeltaRowMerger journalMerger = new DeltaRowMerger(journal);

        MappedRow row = journalMapper.map(AmpsRecord.of("{}"), Map.of("tradeId", "T-1"), INGEST);
        assertThat(journalMerger.merge(row)).isSameAs(row);
    }
}
