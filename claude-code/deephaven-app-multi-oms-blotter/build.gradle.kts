// :deephaven-app-multi-oms-blotter -- a python module wrapped in Gradle (doc 09 §9).
//
// This is not a Java project: only the `base` lifecycle plugin is applied, which
// supplies the `check` / `build` / `clean` tasks. `check` depends on `pytest`, so
// the root `./gradlew build` runs the multi_oms unit tests alongside the Java ones,
// exactly like :deephaven-scripts.
plugins {
    base
}

description = "Multi-OMS drop-copy blotter: cross-hub FIX 4.2 linking and per-edge reconciliation (multi_oms)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs multi_oms in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    commandLine("bash", testRunner.absolutePath)
    // Unit tests are cheap and depend on the interpreter/venv state, so never
    // cache them as up-to-date.
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}
