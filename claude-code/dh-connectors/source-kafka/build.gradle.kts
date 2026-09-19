// :dh-connectors:source-kafka -- the Kafka driver for the connector framework.
//
// One module per transport, so an application carries the clients it actually dials:
// kafka-clients lives HERE rather than in :dh-connectors:core, and an AMPS-only or
// TCP-only deployment never sees it. Core knows nothing about Kafka; this module
// contributes a KafkaSourceFactory through its own auto-configuration and the
// SourceResolver picks it up.
plugins {
    `java-library`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "Kafka RecordSource for dh-connectors: consumer subscription -> SourceRecord"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // Same Boot BOM as core, so a source module never writes a Spring version either.
    api(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))

    implementation(project(":dh-connectors:core"))
    implementation("org.springframework.boot:spring-boot-starter")
    annotationProcessor(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // Version managed by the Boot BOM, which is what the rest of the repo's Kafka
    // consumers are built against.
    implementation("org.apache.kafka:kafka-clients")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(testFixtures(project(":dh-connectors:core")))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Arrow 18 reaches into java.nio internals for its off-heap allocator; without this
// the first BufferAllocator allocation fails on JDK 21. Inherited through core's
// gateway, so the flag is needed here too.
val arrowJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")

tasks.test {
    useJUnitPlatform()
    jvmArgs = arrowJvmArgs
    testLogging {
        events("passed", "skipped", "failed")
    }
}
