plugins {
    java
}

allprojects {
    group = "com.deephaven.fix42"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    repositories {
        mavenCentral()
    }
}

val javaProjects = listOf("fix-codec", "oms-engine", "fix-demo-producer", "amps-connectors")

subprojects {
    if (name in javaProjects) {
        pluginManager.apply("java")

        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
            withSourcesJar()
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            }
        }

        dependencies {
            "testImplementation"(rootProject.libs.junit.jupiter)
            "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
        }
    }
}

tasks.register("prepareCompose") {
    group = "distribution"
    description = "Package Deephaven Application Mode assets and the AMPS connector boot JAR"
    dependsOn(":dh-app:prepareDeephavenImage", ":amps-connectors:bootJar")
}
