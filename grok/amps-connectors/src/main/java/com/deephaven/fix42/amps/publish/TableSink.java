package com.deephaven.fix42.amps.publish;

import com.deephaven.fix42.amps.map.MappedRow;

/** Destination for mapped AMPS rows. */
public interface TableSink {
    void upsert(MappedRow row);

    void delete(MappedRow keyRow);
}
