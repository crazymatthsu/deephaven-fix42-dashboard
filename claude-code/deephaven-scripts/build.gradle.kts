// :deephaven-scripts -- a python module wrapped in Gradle (doc 05 §1).
//
// This is not a Java project: only the `base` lifecycle plugin is applied, which
// supplies the `check` / `build` / `clean` tasks. `check` depends on `pytest`, so
// the root `./gradlew build` runs the fix42cache unit tests alongside the Java ones.
plugins {
    base
}

description = "Pure-python FIX 4.2 state machine (fix42cache) + Deephaven server scripts (dh_app)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs fix42cache in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    commandLine("bash", testRunner.absolutePath)
    // Unit tests are cheap and depend on the interpreter/venv state, so never
    // cache them as up-to-date.
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}
