plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

description = "AMPS to Deephaven Spring Boot connectors"

springBoot {
    mainClass.set("com.deephaven.fix42.amps.AmpsConnectorsApplication")
}

dependencies {
    implementation(libs.amps.client)
    implementation(libs.deephaven.java.client.flight)
    implementation(libs.deephaven.qst)
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-json")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName.set("amps-connectors.jar")
}

tasks.named<JavaExec>("bootRun") {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
