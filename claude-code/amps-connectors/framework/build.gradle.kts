// :amps-connectors:framework -- the AMPS -> Deephaven pipeline as a LIBRARY (doc 07).
//
// Deliberately not a Spring Boot application: applications depend on this project,
// and depending on an executable Boot module drags in someone else's main class and
// baked application.yml. The framework contributes its beans through
// AmpsConnectorsAutoConfiguration instead, and ships its shared test doubles
// (FakeAmpsSubscriber, RecordingDeephavenGateway, TestConnectors) as test fixtures.
//
// Versions pinned to what this repo already runs against:
//   deephaven java client 42.4  == ghcr.io/deephaven/server:42.4 in docker-compose.yml
//   arrow 18.3.0                == the version the flight client is published with
plugins {
    `java-library`
    `java-test-fixtures`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "AMPS -> Deephaven connector framework: decode, map, explode, batch, publish"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val deephavenVersion = "42.4"
val arrowVersion = "18.3.0"
val ampsVersion = "5.3.4.1"

dependencies {
    // The Boot BOM as an api platform: consumers inherit the same Spring versions
    // the framework compiled against.
    api(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    // jackson, for the JSON record decoder (no web server is started)
    implementation("org.springframework.boot:spring-boot-starter-json")
    annotationProcessor(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // 60East AMPS java client -- Client/HAClient, Command, Message.
    implementation("com.crankuptheamps:amps-client:$ampsVersion")

    // Deephaven client: ConsoleSession.executeCode (table bootstrap) +
    // FlightSession.addToInputTable (row publishing).
    implementation("io.deephaven:deephaven-java-client-flight-dagger:$deephavenVersion")
    // flight-core ships arrow-memory-core only; an allocator implementation is
    // required at runtime or BufferAllocator construction fails.
    runtimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Arrow 18 reaches into java.nio internals for its off-heap allocator; without this
// the first BufferAllocator allocation fails on JDK 21.
val arrowJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")

tasks.test {
    useJUnitPlatform()
    jvmArgs = arrowJvmArgs
    testLogging {
        events("passed", "skipped", "failed")
    }
}
