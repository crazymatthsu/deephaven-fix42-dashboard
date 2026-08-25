package com.deephaven.fix42.amps.runtime;

import com.deephaven.fix42.amps.amps.AmpsInboundMessage;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.TopicKind;
import com.deephaven.fix42.amps.decode.MessageDecoder;
import com.deephaven.fix42.amps.decode.ParsedFields;
import com.deephaven.fix42.amps.map.FieldMapper;
import com.deephaven.fix42.amps.map.MappedRow;
import com.deephaven.fix42.amps.map.RowKey;
import com.deephaven.fix42.amps.map.RowMerger;
import com.deephaven.fix42.amps.publish.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AMPS message → mapped Deephaven row. Unconfigured AMPS fields never reach the sink.
 */
public final class ConnectorRuntime {
    private static final Logger log = LoggerFactory.getLogger(ConnectorRuntime.class);

    private final ConnectorProperties connector;
    private final MessageDecoder decoder;
    private final FieldMapper mapper;
    private final RowMerger merger;
    private final TableSink sink;
    private final Map<String, MappedRow> latestByKey = new ConcurrentHashMap<>();
    private final Map<String, String> sowKeyToRowKey = new ConcurrentHashMap<>();

    public ConnectorRuntime(
            ConnectorProperties connector,
            MessageDecoder decoder,
            FieldMapper mapper,
            RowMerger merger,
            TableSink sink) {
        this.connector = connector;
        this.decoder = decoder;
        this.mapper = mapper;
        this.merger = merger;
        this.sink = sink;
    }

    public void onMessage(AmpsInboundMessage message) {
        if (message == null || message.kind() == AmpsInboundMessage.Kind.OTHER) {
            return;
        }
        if (message.isOof()) {
            handleOof(message);
            return;
        }
        if (!message.isData()) {
            return;
        }
        ParsedFields parsed = decoder.decode(message.data());
        MappedRow incoming = mapper.map(parsed);
        if (connector.getTopicKind() == TopicKind.JOURNAL) {
            sink.upsert(merger.merge(null, incoming, true));
            return;
        }
        String rowKey = resolveRowKey(incoming, message.sowKey());
        if (rowKey == null) {
            log.warn("connector {}: skipping message with no key columns / sowKey", connector.getName());
            return;
        }
        MappedRow previous = latestByKey.get(rowKey);
        MappedRow merged = merger.merge(previous, incoming, message.isSowSnapshot());
        String mergedKey = RowKey.of(merged, connector.getKeyColumns());
        if (mergedKey == null) {
            log.warn("connector {}: merged row is missing key columns", connector.getName());
            return;
        }
        latestByKey.put(mergedKey, merged);
        if (!message.sowKey().isBlank()) {
            sowKeyToRowKey.put(message.sowKey(), mergedKey);
        }
        sink.upsert(merged);
    }

    public void resetLocalState() {
        latestByKey.clear();
        sowKeyToRowKey.clear();
    }

    private void handleOof(AmpsInboundMessage message) {
        MappedRow incoming = MappedRow.empty();
        if (!message.data().isBlank()) {
            incoming = mapper.map(decoder.decode(message.data()));
        }
        String rowKey = resolveRowKey(incoming, message.sowKey());
        if (rowKey == null) {
            log.warn("connector {}: OOF with no resolvable key", connector.getName());
            return;
        }
        MappedRow cached = latestByKey.remove(rowKey);
        if (!message.sowKey().isBlank()) {
            sowKeyToRowKey.remove(message.sowKey());
        }
        MappedRow keyRow = cached != null
                ? RowKey.keyOnly(cached, connector.getKeyColumns())
                : RowKey.keyOnly(incoming, connector.getKeyColumns());
        if (RowKey.of(keyRow, connector.getKeyColumns()) == null) {
            log.warn("connector {}: OOF key row incomplete for {}", connector.getName(), rowKey);
            return;
        }
        sink.delete(keyRow);
    }

    private String resolveRowKey(MappedRow incoming, String sowKey) {
        String fromRow = RowKey.of(incoming, connector.getKeyColumns());
        if (fromRow != null) {
            return fromRow;
        }
        if (sowKey != null && !sowKey.isBlank()) {
            return sowKeyToRowKey.get(sowKey);
        }
        return null;
    }

    List<String> keyColumns() {
        return connector.getKeyColumns();
    }
}
