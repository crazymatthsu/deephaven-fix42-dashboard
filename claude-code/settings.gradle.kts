pluginManagement {
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
include(":amps-connectors")
include(":deephaven-app-java")
include(":deephaven-app-multi-oms-blotter")
