plugins {
    java
    application
}

group = "com.fix42.dashboard"
version = "0.1.0"
description = "FIX 4.2 mock order-flow generator: scenario engine + Kafka/AMPS producer CLI"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.9.1")
    // 60East AMPS java client -- the --amps-uri sink (doc 10 §10). Same coordinates as
    // :amps-connectors, so both modules resolve one artifact from Maven Central.
    implementation("com.crankuptheamps:amps-client:5.3.4.1")
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "com.fix42.dashboard.gen.GeneratorMain"
    applicationDefaultJvmArgs = listOf("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
