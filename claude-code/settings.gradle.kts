pluginManagement {
    // Convention plugins for the connector applications (dh.connector-app).
    includeBuild("build-logic")
    // Every repository -- plugins and dependencies, for this build and for build-logic --
    // comes from repos.env (docs/15). Modules declare no repositories of their own.
    apply(from = "gradle/repos.settings.gradle.kts")
}

plugins {
    // JDK 21 auto-download from api.foojay.io; applied below unless repos.env sets
    // GRADLE_JDK_AUTO_DOWNLOAD=false (corporate networks block it; install JDK 21 instead).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0" apply false
}

@Suppress("UNCHECKED_CAST")
val repos = gradle.extra["repos"] as Map<String, String>
if (!repos["GRADLE_JDK_AUTO_DOWNLOAD"].equals("false", ignoreCase = true)) {
    apply(plugin = "org.gradle.toolchains.foojay-resolver-convention")
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
include(":order-tree-recon")
