package com.fix42.dashboard.amps.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

/**
 * Asserts things about the CONFIG TREE IN THIS REPO ({@code amps-connectors/config/}), not
 * about the code: every deployable application configuration binds, validates and layers the
 * way it will at runtime — without booting anything. With one application per directory and
 * potentially dozens of them, the expensive failures are all configuration mistakes that
 * produce a perfectly healthy-looking process; this test is where they fail loudly instead.
 *
 * <p>Each instance is bound exactly as the container layers it: baked {@code application.yml}
 * underneath, then {@code config/<env>/common/}, then {@code config/<env>/<flow>/<app>/} on
 * top (first source wins in a {@link Binder}, so they are stacked in reverse).
 */
class ConfigTreeTest {

    private static final Path CONFIG_ROOT = Path.of("..", "config").normalize();

    record Instance(String env, String flow, String app, Path file) {
        @Override
        public String toString() {
            return env + "/" + flow + "/" + app;
        }
    }

    // ---- discovery ---------------------------------------------------------------

    private static List<Path> envDirs() throws IOException {
        try (Stream<Path> envs = Files.list(CONFIG_ROOT)) {
            return envs.filter(Files::isDirectory).sorted().toList();
        }
    }

    static List<Instance> instances() throws IOException {
        List<Instance> out = new ArrayList<>();
        for (Path env : envDirs()) {
            try (Stream<Path> flows = Files.list(env)) {
                for (Path flow : flows.filter(Files::isDirectory).sorted().toList()) {
                    if (flow.getFileName().toString().equals("common")) {
                        continue;
                    }
                    try (Stream<Path> apps = Files.list(flow)) {
                        for (Path app : apps.filter(Files::isDirectory).sorted().toList()) {
                            out.add(new Instance(env.getFileName().toString(),
                                    flow.getFileName().toString(),
                                    app.getFileName().toString(),
                                    app.resolve("application.yml")));
                        }
                    }
                }
            }
        }
        return out;
    }

    // ---- binding, layered the way the container layers it ------------------------

    private static AmpsConnectorsProperties bind(Instance instance) throws IOException {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        MutablePropertySources sources = new MutablePropertySources();
        for (PropertySource<?> source : loader.load(instance.toString(),
                new FileSystemResource(instance.file()))) {
            sources.addLast(source);
        }
        Path common = CONFIG_ROOT.resolve(instance.env()).resolve("common")
                .resolve("application.yml");
        if (Files.exists(common)) {
            for (PropertySource<?> source : loader.load(instance.env() + "/common",
                    new FileSystemResource(common))) {
                sources.addLast(source);
            }
        }
        for (PropertySource<?> source : loader.load("baked",
                new ClassPathResource("application.yml"))) {
            sources.addLast(source);
        }
        ApplicationConversionService conversion = new ApplicationConversionService();
        conversion.addConverter(new ColumnTypeConverter());
        Binder binder = new Binder(ConfigurationPropertySources.from(sources),
                new PropertySourcesPlaceholdersResolver(sources), conversion, null, null);
        return binder.bind("amps", Bindable.of(AmpsConnectorsProperties.class)).get();
    }

    // ---- per-instance rules --------------------------------------------------------

    @ParameterizedTest
    @MethodSource("instances")
    void everyInstanceBindsValidatesAndDefinesConnectors(Instance instance) throws IOException {
        AmpsConnectorsProperties properties = bind(instance);
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(properties))
                    .as("%s: jakarta constraint violations", instance).isEmpty();
        }
        assertThat(ConnectorValidator.validate(properties))
                .as("%s: connector validation", instance).isEmpty();
        assertThat(properties.getConnectors())
                .as("%s: an instance with no connectors deploys a process that does nothing",
                        instance)
                .isNotEmpty();
    }

    // ---- cross-file rules ----------------------------------------------------------

    @Test
    void theTreeIsEnvFlowApp() throws IOException {
        // Every application.yml sits at exactly config/<env>/<flow>/<app>/application.yml
        // (or <env>/common/application.yml). A file at the wrong depth is silently never
        // mounted, which is the worst kind of missing.
        try (Stream<Path> files = Files.walk(CONFIG_ROOT)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                Path rel = CONFIG_ROOT.relativize(file);
                assertThat(rel.getFileName().toString())
                        .as("%s: only application.yml belongs in the tree", rel)
                        .isEqualTo("application.yml");
                boolean common = rel.getNameCount() == 3
                        && rel.getName(1).toString().equals("common");
                boolean instance = rel.getNameCount() == 4
                        && !rel.getName(1).toString().equals("common");
                assertThat(common || instance)
                        .as("%s: expected <env>/common/application.yml or "
                                + "<env>/<flow>/<app>/application.yml", rel)
                        .isTrue();
            }
        }
    }

    @Test
    void commonNeverDefinesConnectors() throws IOException {
        // Two amps.connectors lists merge BY INDEX: a common entry would bleed through
        // underneath every app's own list. The file still parses, the app still starts,
        // and connector [0] is quietly not the one the instance file defines.
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        for (Path env : envDirs()) {
            Path common = env.resolve("common").resolve("application.yml");
            if (!Files.exists(common)) {
                continue;
            }
            for (PropertySource<?> source : loader.load(env.toString(),
                    new FileSystemResource(common))) {
                assertThat(((EnumerablePropertySource<?>) source).getPropertyNames())
                        .as("%s: amps.connectors must live in the instance files", common)
                        .noneMatch(name -> name.startsWith("amps.connectors"));
            }
        }
    }

    @Test
    void noTwoAppsInAnEnvironmentWriteTheSameTable() throws IOException {
        // Two connectors feeding one Deephaven table fight over its rows and its schema;
        // split by environment because dev deliberately mirrors local's tables.
        Map<String, String> owners = new HashMap<>();
        for (Instance instance : instances()) {
            for (ConnectorProperties connector : bind(instance).getConnectors()) {
                String table = instance.env() + "/" + connector.getDeephaven().getTable();
                String previous = owners.put(table, instance.toString());
                assertThat(previous)
                        .as("%s is written by both %s and %s", table, previous, instance)
                        .isNull();
            }
        }
    }

    @Test
    void noCredentialLiteralsInTheTree() throws IOException {
        // These files are plaintext in git; secrets arrive as environment variables.
        try (Stream<Path> files = Files.walk(CONFIG_ROOT)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String body = Files.readString(file).replaceAll("#.*", "");
                assertThat(body.matches("(?s).*\\b(password|passwd|secret|token)\\s*:\\s*\\S.*"))
                        .as("%s: a credential literal must never appear in a config file", file)
                        .isFalse();
            }
        }
    }

    @Test
    void theLocalEnvironmentExists() throws IOException {
        // Guards the discovery itself: if the tree moves, every walking test above would
        // pass vacuously on an empty list.
        assertThat(instances()).extracting(Instance::env).contains("local");
    }
}
