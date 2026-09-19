// :dh-connectors:source-amps -- the 60East AMPS driver for the connector framework.
//
// One module per transport, so an application carries the clients it actually dials:
// the 60East jar lives HERE rather than in :dh-connectors:core, and a Kafka-only or
// TCP-only deployment never sees it. Core knows nothing about AMPS; this module
// contributes an AmpsSourceFactory through its own auto-configuration and the
// SourceResolver picks it up.
plugins {
    `java-library`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "AMPS RecordSource for dh-connectors: HAClient subscription -> SourceRecord"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val ampsVersion = "5.3.4.1"

dependencies {
    // Same Boot BOM as core, so a source module never writes a Spring version either.
    api(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))

    implementation(project(":dh-connectors:core"))
    implementation("org.springframework.boot:spring-boot-starter")
    annotationProcessor(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // 60East AMPS java client -- Client/HAClient, Command, Message. The reason this
    // module exists.
    implementation("com.crankuptheamps:amps-client:$ampsVersion")

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
