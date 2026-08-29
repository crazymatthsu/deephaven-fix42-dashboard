// :amps-connectors -- Spring Boot application bridging 60East AMPS topics into
// Deephaven input tables (doc 07).
//
// Versions pinned to what this repo already runs against:
//   deephaven java client 42.4  == ghcr.io/deephaven/server:42.4 in docker-compose.yml
//   arrow 18.3.0                == the version the flight client is published with
plugins {
    java
    application
    id("org.springframework.boot") version "3.5.16"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "AMPS -> Deephaven connectors: Spring Boot, application.yml driven, FIX/NVFIX/JSON"

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
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    // jackson, for the JSON record decoder (no web server is started)
    implementation("org.springframework.boot:spring-boot-starter-json")
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

application {
    mainClass = "com.fix42.dashboard.amps.AmpsConnectorsApplication"
}

// Arrow 18 reaches into java.nio internals for its off-heap allocator; without this
// the first BufferAllocator allocation fails on JDK 21.
val arrowJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs = arrowJvmArgs
}

tasks.test {
    useJUnitPlatform()
    jvmArgs = arrowJvmArgs
    // LiveTableTypeTest is opt-in and needs a real server; forward its switches to the test JVM.
    //   ./gradlew :amps-connectors:test --tests '*LiveTableTypeTest' -Damps.live=true
    for (key in listOf("amps.live", "amps.live.port")) {
        System.getProperty(key)?.let { systemProperty(key, it) }
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
}
