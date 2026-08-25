package com.deephaven.fix42.amps.map;

import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.decode.FieldCoercer;
import com.deephaven.fix42.amps.decode.ParsedFields;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FieldMapper {
    private final List<FieldMappingProperties> fields;

    public FieldMapper(List<FieldMappingProperties> fields) {
        this.fields = List.copyOf(fields);
    }

    public MappedRow map(ParsedFields parsed) {
        Map<String, Object> values = new LinkedHashMap<>();
        Set<String> present = new LinkedHashSet<>();
        for (FieldMappingProperties field : fields) {
            if (!parsed.isPresent(field.getSource())) {
                continue;
            }
            present.add(field.getColumn());
            String raw = parsed.get(field.getSource()).orElse(null);
            values.put(field.getColumn(), FieldCoercer.coerce(raw, field.getType()));
        }
        return new MappedRow(values, present);
    }

    public List<FieldMappingProperties> fields() {
        return fields;
    }
}
