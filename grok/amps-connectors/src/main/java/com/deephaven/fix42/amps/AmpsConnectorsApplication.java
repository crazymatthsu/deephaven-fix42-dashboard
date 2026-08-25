package com.deephaven.fix42.amps;

import com.deephaven.fix42.amps.config.AmpsProperties;
import com.deephaven.fix42.amps.config.DeephavenClientProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({AmpsProperties.class, DeephavenClientProperties.class})
public class AmpsConnectorsApplication {
    public static void main(String[] args) {
        SpringApplication.run(AmpsConnectorsApplication.class, args);
    }
}
