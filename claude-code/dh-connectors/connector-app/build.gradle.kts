// :dh-connectors:connector-app -- the GENERIC runner (doc 07 §9).
//
// One image, deployed N times: every config-only connector application in
// config/<env>/<flow>/<app-name>/ runs as this app under its own
// spring.application.name. An app that needs custom code gets its own module
// under apps/ instead; this build file is the template for what that costs.
plugins {
    id("dh.connector-app")
}

description = "Generic source -> Deephaven connector application; connectors arrive as mounted configuration"

dependencies {
    // Every source module, because this is the ONE image the whole fleet deploys: an
    // instance picks its transport in configuration (source.amps / source.kafka /
    // source.tcp), and a driver missing from the image would turn that into a startup
    // failure on the day someone writes a different block. An app under apps/ that only
    // ever dials one broker can depend on just that module.
    implementation(project(":dh-connectors:source-amps"))
    implementation(project(":dh-connectors:source-kafka"))
    implementation(project(":dh-connectors:source-tcp"))
}
