pluginManagement {
    // Convention plugins for the connector applications (dh.connector-app).
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "deephaven-fix42-dashboard"

include(":fix-mock-generator")
include(":deephaven-scripts")
// dh-connectors is framework + drivers + applications (docs/07 §9,
// dh-connectors/README.md): the core library, one module per source transport, the
// generic runner every config-only connector app deploys as, and auto-discovered custom
// apps. Adding custom app #51 means creating dh-connectors/apps/<name>/build.gradle.kts
// — not editing this file.
include(":dh-connectors:core")
include(":dh-connectors:source-amps")
include(":dh-connectors:source-kafka")
include(":dh-connectors:source-tcp")
include(":dh-connectors:source-jdbc")
include(":dh-connectors:source-s3")
include(":dh-connectors:connector-app")
file("dh-connectors/apps").listFiles()
    ?.filter { it.isDirectory && File(it, "build.gradle.kts").exists() }
    ?.sortedBy { it.name }
    ?.forEach { include(":dh-connectors:apps:${it.name}") }
include(":deephaven-app-java")
include(":deephaven-app-multi-oms-blotter")
include(":deephaven-remote-uri")
include(":market-data-demo")
include(":basket-oms-demo")
