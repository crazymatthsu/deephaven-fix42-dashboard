pluginManagement {
    // Convention plugins for the AMPS connector applications (dh.connector-app).
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
// amps-connectors is framework + applications (docs/07 §9, amps-connectors/README.md):
// the framework library, the generic runner every config-only connector app deploys
// as, and auto-discovered custom apps. Adding custom app #51 means creating
// amps-connectors/apps/<name>/build.gradle.kts — not editing this file.
include(":amps-connectors:framework")
include(":amps-connectors:connector-app")
file("amps-connectors/apps").listFiles()
    ?.filter { it.isDirectory && File(it, "build.gradle.kts").exists() }
    ?.sortedBy { it.name }
    ?.forEach { include(":amps-connectors:apps:${it.name}") }
include(":deephaven-app-java")
include(":deephaven-app-multi-oms-blotter")
include(":deephaven-remote-uri")
include(":market-data-demo")
include(":basket-oms-demo")
