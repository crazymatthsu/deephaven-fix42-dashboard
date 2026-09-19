// The plugin every connector APPLICATION applies (core does not: it is a plain
// java-library). This is why an app's build file is ~5 lines, and why 50 of them
// stay cheap: Boot version, core dependency, test suites, JVM flags and the
// container image tasks all live here once.
//
// Source modules (:dh-connectors:source-amps and friends) are NOT added here: an
// application declares the transports it dials, and the generic runner declares
// all of them.

import org.springframework.boot.gradle.plugin.SpringBootPlugin
import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
    java
    `jvm-test-suite`
    id("org.springframework.boot")
}

group = "com.fix42.dashboard"
version = "0.1.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // The Boot BOM as a platform: connector apps never write a Spring version anywhere.
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))

    // The whole pipeline arrives as a library + auto-configuration; an app is a main
    // class and configuration.
    implementation(project(":dh-connectors:core"))

    // Actuator (and the web server it needs) is not optional: the container
    // HEALTHCHECK in docker/spring-boot.Dockerfile and the compose readiness
    // probes depend on /actuator/health existing in every app.
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // @Validated @ConfigurationProperties silently skips @NotBlank without a
    // validator on the classpath; mandate it here rather than trusting 50 apps.
    implementation("org.springframework.boot:spring-boot-starter-validation")
}

// Arrow 18 reaches into java.nio internals for its off-heap allocator; without this
// the first BufferAllocator allocation fails on JDK 21. The image bakes the same
// flag via JAVA_TOOL_OPTIONS.
val arrowJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")

testing {
    suites {
        // Unit tests run on every build; integrationTest is a separate suite for
        // tests that want a real server (they self-skip unless -Damps.live=true,
        // so `check` may still run them).
        register<JvmTestSuite>("integrationTest") {
            targets.configureEach {
                testTask.configure {
                    shouldRunAfter(tasks.named("test"))
                }
            }
        }

        withType<JvmTestSuite>().configureEach {
            useJUnitJupiter()
            dependencies {
                implementation(project())
                implementation(project(":dh-connectors:core"))
                implementation(testFixtures(project(":dh-connectors:core")))
                implementation(platform(SpringBootPlugin.BOM_COORDINATES))
                implementation("org.springframework.boot:spring-boot-starter-test")
                runtimeOnly("org.junit.platform:junit-platform-launcher")
            }
            targets.configureEach {
                testTask.configure {
                    jvmArgs = arrowJvmArgs
                    // Live suites are opt-in; forward their switches to the test JVM:
                    //   ./gradlew :dh-connectors:connector-app:integrationTest -Damps.live=true
                    for (key in listOf("amps.live", "amps.live.port")) {
                        System.getProperty(key)?.let { systemProperty(key, it) }
                    }
                    testLogging {
                        events("passed", "skipped", "failed")
                    }
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}

tasks.named<BootRun>("bootRun") {
    jvmArgs = arrowJvmArgs
}

// ---- container image ---------------------------------------------------------
// ONE shared Dockerfile serves every app because the staged build context is
// generic: exactly `application.jar` + `Dockerfile` in build/docker. There is no
// per-app Dockerfile to drift, and `podman build build/docker` is the whole story.

val stageDockerContext by tasks.registering(Sync::class) {
    group = "docker"
    description = "Stages application.jar + the shared Dockerfile into build/docker."
    into(layout.buildDirectory.dir("docker"))
    from(tasks.named("bootJar")) {
        rename { "application.jar" }
    }
    from(layout.settingsDirectory.file("dh-connectors/docker/spring-boot.Dockerfile")) {
        rename { "Dockerfile" }
    }
}

tasks.register<Exec>("dockerBuildLocal") {
    group = "docker"
    description = "Builds this app's image into podman as localhost/dh-<app>:local."
    dependsOn(stageDockerContext)
    workingDir = layout.buildDirectory.dir("docker").get().asFile
    // --format docker keeps the Dockerfile HEALTHCHECK; the default OCI format drops it.
    commandLine("podman", "build", "--format", "docker",
            "-t", "localhost/dh-${project.name}:local", ".")
}
