package com.fix42.dashboard.amps.config;

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

/**
 * Binds {@code type:} values through {@link ColumnType#parse(String)} so configuration can
 * use the natural aliases ({@code integer}, {@code bool}, {@code timestamp}) rather than
 * only the exact enum constant names.
 */
@Component
@ConfigurationPropertiesBinding
public class ColumnTypeConverter implements Converter<String, ColumnType> {

    @Override
    public ColumnType convert(String source) {
        return ColumnType.parse(source);
    }
}
