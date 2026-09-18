package com.fix42.dashboard.amps.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ColumnTypeTest {

    @ParameterizedTest
    @CsvSource({
        "string, STRING", "STR, STRING", "text, STRING",
        "bool, BOOLEAN", "Boolean, BOOLEAN",
        "int, INT", "integer, INT", "int32, INT",
        "long, LONG", "int64, LONG",
        "double, DOUBLE", "float64, DOUBLE",
        "float, FLOAT", "float32, FLOAT",
        "short, SHORT", "byte, BYTE", "char, CHAR",
        "instant, INSTANT", "timestamp, INSTANT", "datetime, INSTANT",
    })
    @DisplayName("type names bind through their common aliases")
    void parsesAliases(String text, ColumnType expected) {
        assertThat(ColumnType.parse(text)).isEqualTo(expected);
    }

    @Test
    void rejectsUnknownTypeName() {
        assertThatThrownBy(() -> ColumnType.parse("uuid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown column type");
    }

    @Test
    void coercesEachScalarType() {
        assertThat(ColumnType.STRING.coerce(" AAPL ")).isEqualTo("AAPL");
        assertThat(ColumnType.INT.coerce("42")).isEqualTo(42);
        assertThat(ColumnType.LONG.coerce("42")).isEqualTo(42L);
        assertThat(ColumnType.DOUBLE.coerce("185.52")).isEqualTo(185.52d);
        assertThat(ColumnType.FLOAT.coerce("1.5")).isEqualTo(1.5f);
        assertThat(ColumnType.SHORT.coerce("7")).isEqualTo((short) 7);
        assertThat(ColumnType.BYTE.coerce("7")).isEqualTo((byte) 7);
        assertThat(ColumnType.CHAR.coerce("B")).isEqualTo('B');
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "T", "y", "YES", "1"})
    void coercesTruthyBooleans(String text) {
        assertThat(ColumnType.BOOLEAN.coerce(text)).isEqualTo(Boolean.TRUE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "f", "N", "no", "0"})
    void coercesFalsyBooleans(String text) {
        assertThat(ColumnType.BOOLEAN.coerce(text)).isEqualTo(Boolean.FALSE);
    }

    @Test
    @DisplayName("null and blank are absent values, not errors")
    void treatsBlankAsNull() {
        for (ColumnType type : ColumnType.values()) {
            assertThat(type.coerce(null)).as("%s of null", type).isNull();
            assertThat(type.coerce("   ")).as("%s of blank", type).isNull();
        }
    }

    @Test
    void parsesFixUtcTimestamp() {
        assertThat(ColumnType.INSTANT.coerce("20240115-14:30:00"))
                .isEqualTo(Instant.parse("2024-01-15T14:30:00Z"));
        assertThat(ColumnType.INSTANT.coerce("20240115-14:30:00.123"))
                .isEqualTo(Instant.parse("2024-01-15T14:30:00.123Z"));
    }

    @Test
    void parsesIso8601Timestamp() {
        assertThat(ColumnType.INSTANT.coerce("2024-01-15T14:30:00Z"))
                .isEqualTo(Instant.parse("2024-01-15T14:30:00Z"));
    }

    @Test
    @DisplayName("epoch numbers resolve their unit from the digit count")
    void parsesEpochTimestamps() {
        assertThat(ColumnType.INSTANT.coerce("1705329000")).isEqualTo(Instant.ofEpochSecond(1705329000L));
        assertThat(ColumnType.INSTANT.coerce("1705329000123")).isEqualTo(Instant.ofEpochMilli(1705329000123L));
        assertThat(ColumnType.INSTANT.coerce("1705329000123456"))
                .isEqualTo(Instant.ofEpochSecond(1705329000L, 123_456_000L));
        assertThat(ColumnType.INSTANT.coerce("1705329000123456789"))
                .isEqualTo(Instant.ofEpochSecond(1705329000L, 123_456_789L));
    }

    @Test
    void rejectsValuesThatDoNotFitTheColumnType() {
        assertThatThrownBy(() -> ColumnType.INT.coerce("not-a-number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a valid INT");
        assertThatThrownBy(() -> ColumnType.BOOLEAN.coerce("maybe"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a valid BOOLEAN");
        assertThatThrownBy(() -> ColumnType.INSTANT.coerce("12345"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a valid INSTANT");
    }

    @Test
    @DisplayName("each type names the deephaven dtype the generated python uses")
    void mapsToDeephavenDTypes() {
        assertThat(ColumnType.STRING.deephavenDType()).isEqualTo("dht.string");
        assertThat(ColumnType.INT.deephavenDType()).isEqualTo("dht.int32");
        assertThat(ColumnType.LONG.deephavenDType()).isEqualTo("dht.int64");
        assertThat(ColumnType.DOUBLE.deephavenDType()).isEqualTo("dht.double");
        assertThat(ColumnType.BOOLEAN.deephavenDType()).isEqualTo("dht.bool_");
        assertThat(ColumnType.INSTANT.deephavenDType()).isEqualTo("dht.Instant");
    }

    @Test
    void javaTypesMatchTheArrowColumnTypes() {
        assertThat(ColumnType.STRING.javaType()).isEqualTo(String.class);
        assertThat(ColumnType.LONG.javaType()).isEqualTo(Long.class);
        assertThat(ColumnType.INSTANT.javaType()).isEqualTo(Instant.class);
    }
}
