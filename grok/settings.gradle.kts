pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "deephaven-fix42-dashboard"

include("fix-codec")
include("oms-engine")
include("fix-demo-producer")
include("dh-app")
include("amps-connectors")
