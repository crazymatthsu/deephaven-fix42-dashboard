// :dh-connectors:source-s3 -- the object-store driver for the connector framework.
//
// One module per transport, so an application carries the clients it actually dials. The
// AWS SDK is the heaviest of them, which is exactly why it lives HERE: an AMPS-only or
// JDBC-only deployment never sees it. Core knows nothing about S3; this module contributes
// an S3SourceFactory through its own auto-configuration and the SourceResolver picks it up.
plugins {
    `java-library`
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "S3 RecordSource for dh-connectors: polled object listings -> SourceRecord"

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

    // The Boot BOM does NOT manage the AWS SDK, so this module imports the SDK's own BOM and
    // pins it in one place -- the same bargain source-amps makes with the 60East client.
    implementation(platform("software.amazon.awssdk:bom:2.55.0"))
    implementation("software.amazon.awssdk:s3") {
        // The SDK pulls in two HTTP stacks of its own by default -- apache5-client for sync
        // calls and netty-nio-client for async ones -- and service-loads whichever it finds.
        // Neither is wanted on 50 connector classpaths: this source builds its client
        // explicitly (SdkS3ObjectStore), and the one below is what it builds it with. The
        // module names are the ones the pinned version ships; check them on an upgrade.
        exclude(group = "software.amazon.awssdk", module = "apache5-client")
        exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
    }
    // The light sync HTTP client: java.net.HttpURLConnection and nothing else, with no event
    // loop and no second connection pool for a connector that lists a bucket twice a minute.
    // It also registers itself for service loading, so the excludes above leave the SDK a
    // working default rather than no default at all.
    implementation("software.amazon.awssdk:url-connection-client")

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
