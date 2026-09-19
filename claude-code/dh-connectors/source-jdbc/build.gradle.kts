// :dh-connectors:source-jdbc -- the database driver for the connector framework.
//
// One module per transport, so an application carries the clients it actually dials.
// java.sql itself is in the JDK, but a JDBC *driver* is not: this module ships the one
// blessed driver (PostgreSQL) so the generic runner image can reach a real database
// without a rebuild, and any other driver arrives with the custom app under apps/ that
// needs it. Core knows nothing about JDBC; this module contributes a JdbcSourceFactory
// through its own auto-configuration and the SourceResolver picks it up.
plugins {
    `java-library`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "JDBC RecordSource for dh-connectors: a polled query -> SourceRecord"

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

    // A result-set row has no wire format, so this source builds one: jackson writes each
    // row as the JSON object the connector's decoder then reads. Core keeps its own copy
    // `implementation`, so it does not reach this module's compile classpath.
    implementation("com.fasterxml.jackson.core:jackson-databind")

    // The one driver the generic runner image carries, so a config-only application can
    // dial a real database. runtimeOnly: nothing here compiles against it -- the source
    // speaks java.sql and DriverManager finds this on the classpath. Another database
    // means another module under apps/ with its own driver, not a second entry here.
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(testFixtures(project(":dh-connectors:core")))
    // A real database for the tests, in-process: DDL, DML and result-set typing are what
    // this source is made of, and none of it is worth asserting against a mock.
    testImplementation("com.h2database:h2")
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
