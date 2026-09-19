package com.fix42.dashboard.connectors.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The generic AMPS -> Deephaven connector application.
 *
 * <p>Deliberately empty: the pipeline arrives from the framework's auto-configuration, and which
 * connectors run arrives as configuration -- baked {@code application.yml} defaults, the
 * {@code demo} profile's simulated examples, or (in a container) the mounted instance directory
 * from {@code config/<env>/<flow>/<app-name>/}. Fifty deployments of this one class are fifty
 * different applications because they mount fifty different configurations.
 *
 * <p>An application that needs custom code (its own decoder, an enricher) gets its own module
 * under {@code apps/} with a main class like this one next to the code it adds.
 */
@SpringBootApplication
public class ConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConnectorApplication.class, args);
    }
}
