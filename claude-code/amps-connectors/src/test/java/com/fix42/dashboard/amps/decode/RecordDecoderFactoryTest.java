package com.fix42.dashboard.amps.decode;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.TestConnectors;
import org.junit.jupiter.api.Test;

class RecordDecoderFactoryTest {

    private final RecordDecoderFactory factory = new RecordDecoderFactory(new ObjectMapper());

    @Test
    void picksTheDecoderForEachFormat() {
        assertThat(factory.create(TestConnectors.fixOrders())).isInstanceOf(DelimitedRecordDecoder.class);
        assertThat(factory.create(TestConnectors.nvfixPositions())).isInstanceOf(DelimitedRecordDecoder.class);
        assertThat(factory.create(TestConnectors.jsonTrades())).isInstanceOf(JsonRecordDecoder.class);
    }

    @Test
    void delimitedDecodersUseTheConfiguredSeparator() {
        var connector = TestConnectors.fixOrders();
        connector.getSource().setFieldSeparator('|');
        assertThat(factory.create(connector).decode("11=C-1|55=AAPL|"))
                .containsEntry("11", "C-1")
                .containsEntry("55", "AAPL");
    }
}
