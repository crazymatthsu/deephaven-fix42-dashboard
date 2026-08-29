// :deephaven-app-java -- the FIX 4.2 dashboard app, rewritten in Java against the Deephaven
// *engine* API and run inside the same server as the python original (doc 06 section 3 calls this
// the escape hatch; this module is it).
//
// Versions are pinned to what this repo already runs:
//   deephaven engine 42.4 == ghcr.io/deephaven/server:42.4 in docker/docker-compose.yml
//
// Every io.deephaven artifact is compileOnly ON PURPOSE. The server image already contains
// deephaven-engine-table-42.4.jar and friends on its own classpath; shipping a second copy in the
// deployable jar would put two versions of every engine class in front of the same classloader.
// The AMPS client is the opposite case -- it is NOT in the image, so it has to travel with us.
plugins {
    java
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "FIX 4.2 order-state Deephaven app in Java: fixcache state machine + engine-API DAG"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val deephavenVersion = "42.4"
val ampsVersion = "5.3.4.1"

dependencies {
    // --- provided by ghcr.io/deephaven/server:42.4 -------------------------------------------
    // These are on the server's own classpath (verified: /opt/deephaven/server-jetty-42.4/lib holds
    // deephaven-engine-table-42.4.jar, deephaven-extensions-kafka-42.4.jar,
    // deephaven-application-mode-42.4.jar and friends). compileOnly, never bundled.
    compileOnly("io.deephaven:deephaven-engine-table:$deephavenVersion")
    compileOnly("io.deephaven:deephaven-engine-api:$deephavenVersion")
    compileOnly("io.deephaven:deephaven-table-api:$deephavenVersion")
    compileOnly("io.deephaven:deephaven-qst:$deephavenVersion")
    compileOnly("io.deephaven:deephaven-Util:$deephavenVersion")
    compileOnly("io.deephaven:deephaven-application-mode:$deephavenVersion")
    // isTransitive=false: this artifact declares Confluent's kafka-clients:8.2.1-ccs, which is
    // published to packages.confluent.io rather than Maven Central. We only need KafkaTools' own
    // API surface to compile (no Kafka types appear in the calls we make), and at runtime the image
    // supplies both. Adding the Confluent repository just to resolve a compileOnly dependency we
    // never ship would be a wider change than the problem.
    compileOnly("io.deephaven:deephaven-extensions-kafka:$deephavenVersion") { isTransitive = false }

    // --- NOT in the image, and deliberately still compileOnly ---------------------------------
    // AMPS is commercial software; the python app has exactly the same caveat (doc 03 section 2.1:
    // "amps-python-client is not in ghcr.io/deephaven/server"). FIX42_SOURCE=kafka never loads
    // AmpsIngest, so the jar is only needed by a deployment that actually reads AMPS -- see the
    // module README for how to add it to EXTRA_CLASSPATH.
    compileOnly("com.crankuptheamps:amps-client:$ampsVersion")

    // The engine at TEST scope only. The schemas and the batch builder are worth exercising
    // against the real TableDefinition/ColumnHolder types rather than a stub -- a drifted column
    // name or dtype is precisely the failure this module must not ship. Test scope never reaches
    // the deployable jar, so the "never ship a deephaven jar" rule above still holds.
    testImplementation("io.deephaven:deephaven-engine-table:$deephavenVersion")
    testImplementation("io.deephaven:deephaven-table-api:$deephavenVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The jar is mounted into the container and named on the classpath, so give it a stable,
// self-describing name rather than the directory-derived default.
base {
    archivesName = "fix42-dashboard-java"
}

// docker-compose mounts this directory at /apps/libs and the server start script prepends
// EXTRA_CLASSPATH=/apps/libs/* to its own classpath. Sync (not Copy) is deliberate: a stale jar
// left behind by a version bump would put two copies of every class in front of the engine.
val stageDeployLibs = tasks.register<Sync>("stageDeployLibs") {
    group = "distribution"
    description = "Stages the deployable jar(s) into build/deploy/libs for the compose mount."
    into(layout.buildDirectory.dir("deploy/libs"))
    from(tasks.named("jar"))
    // Empty today -- every dependency is compileOnly because the server image already has it.
    // Anything that later becomes a genuine runtime dependency lands here automatically.
    from(configurations.runtimeClasspath)
}

tasks.named("assemble") {
    dependsOn(stageDeployLibs)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
