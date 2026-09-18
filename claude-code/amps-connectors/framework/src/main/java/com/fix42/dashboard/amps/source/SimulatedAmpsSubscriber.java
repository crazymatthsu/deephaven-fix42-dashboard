package com.fix42.dashboard.amps.source;

import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.ExplodeProperties;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.config.SourceFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmpsSubscriber} that synthesises records in process, with no AMPS server.
 *
 * <p>Selected with {@code source.driver: SIMULATED}. It generates payloads in the connector's
 * configured {@link SourceFormat} using the connector's own field mappings, so the decoder,
 * mapper, batcher and publisher all run exactly as they would against a real server. That is
 * what lets the demo stack and the end-to-end test exercise the whole pipeline; AMPS itself is
 * commercial software with no public image to run alongside Kafka and Deephaven.
 *
 * <p>A SOW topic replays one record per simulated key first -- the analogue of the SOW replay
 * that rehydrates the table -- and only then starts emitting live updates.
 *
 * <p>A {@code COMPOSITE} connector's mappings are grouped by their part index and each part is
 * rendered in its own format; the parts are handed over unframed, the shape the real
 * subscriber produces after {@code CompositeMessageParser}. An {@code explode} configuration
 * gets a dynamic-membered object at its tag -- members come and go across ticks, so the
 * exploder's vanish-deletes have something to do.
 */
public class SimulatedAmpsSubscriber implements AmpsSubscriber {

    private static final Logger log = LoggerFactory.getLogger(SimulatedAmpsSubscriber.class);

    /** Candidate member names an {@code explode} object cycles through. */
    private static final List<String> MEMBERS = List.of("AAPL", "MSFT", "NVDA");

    private final ConnectorProperties connector;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong();

    private ScheduledExecutorService scheduler;

    public SimulatedAmpsSubscriber(ConnectorProperties connector) {
        this.connector = connector;
    }

    @Override
    public void start(RecordHandler handler) {
        AmpsSourceProperties source = connector.getSource();
        running.set(true);

        if (source.isSow()) {
            for (int key = 0; key < source.getSimulatedKeys(); key++) {
                handler.onRecord(record(key));
            }
            log.info("[{}] simulated SOW replay: {} records", connector.getName(),
                    source.getSimulatedKeys());
        }

        long periodMicros = Math.max(1L, 1_000_000L / source.getSimulatedRate());
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "amps-sim-" + connector.getName());
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(() -> {
            if (!running.get()) {
                return;
            }
            try {
                int key = (int) (sequence.get() % source.getSimulatedKeys());
                handler.onRecord(record(key));
            } catch (RuntimeException e) {
                log.error("[{}] simulated record failed", connector.getName(), e);
            }
        }, periodMicros, periodMicros, TimeUnit.MICROSECONDS);

        log.info("[{}] simulated AMPS source started ({} rec/s across {} keys)",
                connector.getName(), source.getSimulatedRate(), source.getSimulatedKeys());
    }

    private AmpsRecord record(int key) {
        long tick = sequence.incrementAndGet();
        String sowKey = "SIM-" + key;
        if (connector.getFormat() == SourceFormat.COMPOSITE) {
            return AmpsRecord.composite(encodeParts(key, tick), sowKey, AmpsRecord.Action.UPSERT);
        }
        return new AmpsRecord(encode(key, tick), sowKey, AmpsRecord.Action.UPSERT);
    }

    /** Render one synthetic record in the connector's wire format. */
    String encode(int key, long tick) {
        return encode(connector.getFields(), connector.getFormat(), explodeTagFor(null), key, tick);
    }

    /**
     * Render the parts of one synthetic composite record. Mappings are routed to the part
     * their tag's index prefix names; an unprefixed tag goes to part 0, which is where the
     * decoder's bare-tag alias reads from first.
     */
    List<String> encodeParts(int key, long tick) {
        List<SourceFormat> formats = connector.getCompositeParts();
        List<List<FieldMapping>> byPart = new ArrayList<>(formats.size());
        for (int i = 0; i < formats.size(); i++) {
            byPart.add(new ArrayList<>());
        }
        for (FieldMapping field : connector.getFields()) {
            int part = partIndex(field.getTag(), formats.size());
            byPart.get(part).add(withTag(field, partRelative(field.getTag())));
        }
        List<String> parts = new ArrayList<>(formats.size());
        for (int i = 0; i < formats.size(); i++) {
            parts.add(encode(byPart.get(i), formats.get(i), explodeTagFor(i), key, tick));
        }
        return parts;
    }

    private String encode(List<FieldMapping> fields, SourceFormat format, String explodeTag,
            int key, long tick) {
        char separator = connector.getSource().getFieldSeparator();
        if (format == SourceFormat.JSON) {
            Map<String, Object> root = jsonTree(fields, key, tick);
            if (explodeTag != null) {
                putPath(root, explodeTag, explodeObject(key, tick));
            }
            return renderJson(root);
        }
        StringBuilder payload = new StringBuilder();
        for (FieldMapping field : fields) {
            if (omit(field, tick)) {
                continue;
            }
            payload.append(field.getTag()).append('=').append(value(field, key, tick))
                    .append(separator);
        }
        return payload.toString();
    }

    /**
     * The explode tag as this payload (or this part of it) addresses it, or {@code null} when
     * the connector does not explode or the tag belongs to another part.
     *
     * @param part the part being rendered, or {@code null} for a simple payload
     */
    private String explodeTagFor(Integer part) {
        ExplodeProperties explode = connector.getExplode();
        if (explode == null) {
            return null;
        }
        if (part == null) {
            return explode.getTag();
        }
        int owner = partIndex(explode.getTag(), connector.getCompositeParts().size());
        return owner == part ? partRelative(explode.getTag()) : null;
    }

    /**
     * The members object for an {@code explode} tag. Membership shifts across ticks -- each
     * candidate sits out one tick in five -- so republished records drop members now and then
     * and the exploder's vanish-deletes are exercised, not just its upserts.
     */
    private Map<String, Object> explodeObject(int key, long tick) {
        ExplodeProperties explode = connector.getExplode();
        Map<String, Object> members = new LinkedHashMap<>();
        for (int i = 0; i < MEMBERS.size(); i++) {
            if (Math.floorMod(tick + i, 5L) == 0L) {
                continue;
            }
            members.put(MEMBERS.get(i), memberValue(explode.getFields(), key, tick + i));
        }
        return members;
    }

    /**
     * One member's value: an object built from the explode mappings, or a bare scalar when
     * the only mapping is {@code "."} (or there are none).
     */
    private Object memberValue(List<FieldMapping> fields, int key, long tick) {
        List<FieldMapping> named = fields.stream()
                .filter(field -> !".".equals(field.getTag()))
                .toList();
        if (named.isEmpty()) {
            FieldMapping dot = fields.isEmpty() ? null : fields.get(0);
            return dot == null
                    ? "\"member-" + tick + "\""
                    : jsonScalar(dot, key, tick);
        }
        Map<String, Object> value = jsonTree(named, key, tick);
        return value;
    }

    /** Part index a composite tag names; an unprefixed tag reads from part 0's alias. */
    private static int partIndex(String tag, int partCount) {
        int dot = tag.indexOf('.');
        if (dot > 0) {
            String prefix = tag.substring(0, dot);
            if (prefix.chars().allMatch(Character::isDigit)) {
                int index = Integer.parseInt(prefix);
                if (index < partCount) {
                    return index;
                }
            }
        }
        return 0;
    }

    /** The tag with its part prefix stripped, when it has one. */
    private static String partRelative(String tag) {
        int dot = tag.indexOf('.');
        if (dot > 0 && tag.substring(0, dot).chars().allMatch(Character::isDigit)) {
            return tag.substring(dot + 1);
        }
        return tag;
    }

    private static FieldMapping withTag(FieldMapping field, String tag) {
        FieldMapping copy = new FieldMapping();
        copy.setTag(tag);
        copy.setColumn(field.getColumn());
        copy.setType(field.getType());
        copy.setDecode(field.getDecode());
        copy.setValues(field.getValues());
        copy.setDefaultValue(field.getDefaultValue());
        return copy;
    }

    /**
     * Build the document a set of mappings describes. A dotted tag such as
     * {@code execution.venue} nests, so the generated payload is one the configured mapping
     * actually resolves against rather than a flattened approximation of it.
     */
    private Map<String, Object> jsonTree(List<FieldMapping> fields, int key, long tick) {
        Map<String, Object> root = new LinkedHashMap<>();
        for (FieldMapping field : fields) {
            if (omit(field, tick)) {
                continue;
            }
            putPath(root, field.getTag(), jsonScalar(field, key, tick));
        }
        return root;
    }

    /** Set a value at a dotted path, creating the objects along the way. */
    private static void putPath(Map<String, Object> root, String tag, Object value) {
        String[] path = tag.split("\\.");
        Map<String, Object> node = root;
        for (int i = 0; i < path.length - 1; i++) {
            Object child = node.computeIfAbsent(path[i], name -> new LinkedHashMap<String, Object>());
            if (!(child instanceof Map)) {
                break;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) child;
            node = next;
        }
        node.put(path[path.length - 1], value);
    }

    /** A pre-rendered JSON scalar: quoted for the textual column types, bare for the rest. */
    private String jsonScalar(FieldMapping field, int key, long tick) {
        String value = value(field, key, tick);
        boolean quoted = field.getType() == ColumnType.STRING
                || field.getType() == ColumnType.CHAR
                || field.getType() == ColumnType.INSTANT;
        return quoted ? "\"" + value + "\"" : value;
    }

    @SuppressWarnings("unchecked")
    private static String renderJson(Map<String, Object> node) {
        List<String> members = new ArrayList<>(node.size());
        for (Map.Entry<String, Object> member : node.entrySet()) {
            Object value = member.getValue();
            String rendered = value instanceof Map
                    ? renderJson((Map<String, Object>) value)
                    : String.valueOf(value);
            members.add("\"" + member.getKey() + "\":" + rendered);
        }
        return "{" + String.join(",", members) + "}";
    }

    /**
     * Leave a defaulted field out of one record in four.
     *
     * <p>A {@code default-value} exists to cover a field the payload does not carry, so a
     * simulator that always emits every mapping could never show it working. Demo affordance
     * only -- a real feed decides for itself what it sends.
     *
     * <p>Keyed on {@code tick} alone. The caller derives {@code key} from the same counter, so
     * {@code key + tick} has a fixed parity for an even {@code simulated-keys} and would make
     * this fire on every record or none.
     */
    private static boolean omit(FieldMapping field, long tick) {
        return field.getDefaultValue() != null && Math.floorMod(tick, 4L) == 0L;
    }

    /**
     * One synthetic value for a mapping.
     *
     * <p>A mapping with a code -> value table gets one of its <em>codes</em>, so the configured
     * decode has something to decode and the demo shows {@code BUY} rather than a synthetic
     * string passing through untouched.
     */
    private String value(FieldMapping field, int key, long tick) {
        Map<String, String> table = field.resolveValueTable();
        if (!table.isEmpty()) {
            // Cycles on tick alone, for the reason given on omit(): key is a function of the
            // same counter, so key + tick cannot address an even-sized table evenly.
            List<String> codes = List.copyOf(table.keySet());
            return codes.get((int) Math.floorMod(tick, codes.size()));
        }
        return switch (field.getType()) {
            case STRING -> field.getColumn() + "-" + key;
            case CHAR -> String.valueOf((char) ('A' + (key % 26)));
            case BOOLEAN -> String.valueOf(tick % 2 == 0);
            case BYTE, SHORT, INT, LONG -> String.valueOf(100 + key * 10L + (tick % 50));
            case FLOAT, DOUBLE -> String.format(java.util.Locale.ROOT, "%.2f",
                    100.0 + key + (tick % 100) / 100.0);
            case INSTANT -> Instant.now().toString();
        };
    }

    @Override
    public boolean isConnected() {
        return running.get();
    }

    @Override
    public void close() {
        running.set(false);
        ScheduledExecutorService current = scheduler;
        scheduler = null;
        if (current != null) {
            current.shutdownNow();
        }
    }
}
