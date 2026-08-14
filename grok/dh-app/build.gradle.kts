plugins {
    base
}

description = "Deephaven Application Mode Python scripts (not compiled)"

val packageApp by tasks.registering(Copy::class) {
    from("src/main/app.d")
    into(layout.buildDirectory.dir("app.d"))
}

tasks.named("assemble") {
    dependsOn(packageApp)
}

val collectEngineJar by tasks.registering(Copy::class) {
    dependsOn(":oms-engine:fatJar")
    from(project(":oms-engine").tasks.named("fatJar"))
    into(layout.buildDirectory.dir("libs"))
    rename { "oms-engine-all.jar" }
}

tasks.register("prepareDeephavenImage") {
    group = "distribution"
    description = "Collect Application Mode scripts and the oms-engine JAR for the Deephaven image"
    dependsOn(packageApp, collectEngineJar)
}
