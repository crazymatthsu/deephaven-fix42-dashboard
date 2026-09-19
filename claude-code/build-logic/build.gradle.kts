plugins {
    `kotlin-dsl`
}

dependencies {
    // Putting the Boot plugin on build-logic's classpath is what lets
    // dh.connector-app apply it and reference SpringBootPlugin.BOM_COORDINATES
    // directly. The version lives here and nowhere else in the connector modules.
    implementation("org.springframework.boot:spring-boot-gradle-plugin:3.5.16")
}
