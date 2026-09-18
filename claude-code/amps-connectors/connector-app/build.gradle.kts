// :amps-connectors:connector-app -- the GENERIC runner (doc 07 §9).
//
// One image, deployed N times: every config-only connector application in
// config/<env>/<flow>/<app-name>/ runs as this app under its own
// spring.application.name. An app that needs custom code gets its own module
// under apps/ instead; this build file is the template for what that costs.
plugins {
    id("dh.connector-app")
}

description = "Generic AMPS -> Deephaven connector application; connectors arrive as mounted configuration"
