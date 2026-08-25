package com.deephaven.fix42.amps.dh;

import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.TopicKind;
import com.deephaven.fix42.amps.map.MappedRow;
import com.deephaven.fix42.amps.publish.TableSink;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class DeephavenInputTableSink implements TableSink {
    private static final Logger log = LoggerFactory.getLogger(DeephavenInputTableSink.class);

    private final FlightSession flight;
    private final BufferAllocator allocator;
    private final ConnectorProperties connector;
    private final Duration publishTimeout;
    private final ScopeId scopeId;

    public DeephavenInputTableSink(
            FlightSession flight,
            BufferAllocator allocator,
            ConnectorProperties connector,
            Duration publishTimeout) {
        this.flight = flight;
        this.allocator = allocator;
        this.connector = connector;
        this.publishTimeout = publishTimeout;
        this.scopeId = new ScopeId(connector.getTableName());
    }

    /**
     * @return true if the table was created in this call
     */
    public boolean ensureTable() throws Exception {
        if (tableExists()) {
            log.info("Deephaven table {} already exists", connector.getTableName());
            return false;
        }
        TableHeader header = QstTypes.tableHeader(connector.getFields());
        TableSpec spec;
        if (connector.getTopicKind() == TopicKind.SOW) {
            spec = InMemoryKeyBackedInputTable.of(header, List.copyOf(connector.getKeyColumns()));
        } else {
            spec = InMemoryAppendOnlyInputTable.of(header);
        }
        try (TableHandle handle = flight.session().execute(spec)) {
            // 0.40 Session interface only has ticket-to-ticket publish; SessionImpl binds a query-scope name.
            ((SessionImpl) flight.session())
                    .publish(connector.getTableName(), handle)
                    .get(timeoutSec(), TimeUnit.SECONDS);
        }
        log.info(
                "created Deephaven {} table {}",
                connector.getTopicKind() == TopicKind.SOW ? "keyed" : "append-only",
                connector.getTableName());
        return true;
    }

    private boolean tableExists() {
        List<String> expected = Arrays.asList("scope", connector.getTableName());
        for (FlightInfo info : flight.list()) {
            if (info.getDescriptor() != null && expected.equals(info.getDescriptor().getPath())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void upsert(MappedRow row) {
        NewTable table = QstTypes.toNewTable(connector.getFields(), row);
        try {
            flight.addToInputTable(scopeId, table, allocator).get(timeoutSec(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to add row to " + connector.getTableName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(MappedRow keyRow) {
        if (connector.getTopicKind() != TopicKind.SOW) {
            return;
        }
        NewTable table = QstTypes.keyTable(connector.getFields(), keyRow);
        try {
            flight.deleteFromInputTable(scopeId, table, allocator).get(timeoutSec(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to delete row from " + connector.getTableName() + ": " + e.getMessage(), e);
        }
    }

    private long timeoutSec() {
        long sec = publishTimeout.toSeconds();
        return sec <= 0 ? 10 : sec;
    }
}
