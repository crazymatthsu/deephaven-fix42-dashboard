# apps/ — custom-code connector applications

Most connector applications need **no directory here**: they are the generic
`connector-app` image plus one configuration directory under
`config/<env>/<flow>/<app-name>/`. This directory is the escape hatch for the
application that needs code of its own — a bespoke decoder, an enricher, an
extra health indicator.

To add one:

```
apps/<app-name>/
├── build.gradle.kts        # plugins { id("dh.connector-app") }  + extra deps
└── src/
    ├── main/java/...       # a @SpringBootApplication main class + the custom code
    ├── main/resources/application.yml
    ├── test/java/...
    └── integrationTest/java/...
```

`settings.gradle.kts` discovers the module automatically (any directory here with
a `build.gradle.kts`), `./gradlew :amps-connectors:apps:<app-name>:dockerBuildLocal`
builds its image as `localhost/dh-<app-name>:local`, and
`scripts/dh-connectors-compose.sh` prefers that image over the generic runner for
any configured app whose name matches a directory here.

Promoting a config-only app to a code app is mechanical: create the module, keep
the same app-name, and its existing `config/` directories keep working.
