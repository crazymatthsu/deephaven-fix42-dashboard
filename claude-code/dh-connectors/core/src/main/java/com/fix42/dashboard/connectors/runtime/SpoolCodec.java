package com.fix42.dashboard.connectors.runtime;

import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The on-disk form of a {@link SpoolEntry}: one byte for the kind, then for a record its
 * action, key, payload and composite parts as length-prefixed UTF-8.
 *
 * <p>Deliberately not {@code writeUTF}: that caps a string at 64 KB, and a composite part can
 * be bigger. The framing around each entry (a 4-byte length) belongs to {@link RecordSpool};
 * this class only knows the entry.
 */
final class SpoolCodec {

    private static final byte KIND_RECORD = 0;
    private static final byte KIND_SNAPSHOT_BEGIN = 1;
    private static final byte KIND_SNAPSHOT_END = 2;

    private static final byte ACTION_UPSERT = 0;
    private static final byte ACTION_DELETE = 1;

    private static final int NULL_LENGTH = -1;

    private SpoolCodec() {
    }

    static byte[] encode(SpoolEntry entry) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) Math.min(entry.sizeEstimate(), Integer.MAX_VALUE));
        DataOutputStream out = new DataOutputStream(bytes);
        try {
            switch (entry.kind()) {
                case SNAPSHOT_BEGIN -> out.writeByte(KIND_SNAPSHOT_BEGIN);
                case SNAPSHOT_END -> out.writeByte(KIND_SNAPSHOT_END);
                case RECORD -> {
                    SourceRecord record = entry.record();
                    out.writeByte(KIND_RECORD);
                    out.writeByte(record.action() == SourceRecord.Action.DELETE ? ACTION_DELETE : ACTION_UPSERT);
                    writeString(out, record.key());
                    writeString(out, record.data());
                    if (record.parts() == null) {
                        out.writeInt(NULL_LENGTH);
                    } else {
                        out.writeInt(record.parts().size());
                        for (String part : record.parts()) {
                            writeString(out, part);
                        }
                    }
                }
            }
            out.flush();
        } catch (IOException e) {
            // A byte array stream never throws.
            throw new IllegalStateException(e);
        }
        return bytes.toByteArray();
    }

    static SpoolEntry decode(byte[] buffer, int offset, int length) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer, offset, length));
        byte kind = in.readByte();
        return switch (kind) {
            case KIND_SNAPSHOT_BEGIN -> SpoolEntry.snapshotBegin();
            case KIND_SNAPSHOT_END -> SpoolEntry.snapshotEnd();
            case KIND_RECORD -> {
                SourceRecord.Action action = in.readByte() == ACTION_DELETE
                        ? SourceRecord.Action.DELETE
                        : SourceRecord.Action.UPSERT;
                String key = readString(in);
                String data = readString(in);
                int partCount = in.readInt();
                List<String> parts = null;
                if (partCount != NULL_LENGTH) {
                    parts = new ArrayList<>(partCount);
                    for (int i = 0; i < partCount; i++) {
                        parts.add(readString(in));
                    }
                }
                yield SpoolEntry.of(new SourceRecord(data, parts, key, action));
            }
            default -> throw new IOException("unknown spool entry kind " + kind);
        };
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(NULL_LENGTH);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length < 0) {
            throw new IOException("corrupt string length " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
