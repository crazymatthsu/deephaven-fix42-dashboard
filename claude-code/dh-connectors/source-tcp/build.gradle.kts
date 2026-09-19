// :dh-connectors:source-tcp -- the raw-socket driver for the connector framework.
//
// The one source module with no client dependency at all: a framed TCP reader is
// java.net plus the framing rules in TcpSourceProperties. It still gets its own module
// rather than folding into core, because core knows nothing about transports -- this
// module contributes a TcpSourceFactory through its own auto-configuration and the
// SourceResolver picks it up.
plugins {
    `java-library`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "TCP RecordSource for dh-connectors: framed socket stream -> SourceRecord"

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
