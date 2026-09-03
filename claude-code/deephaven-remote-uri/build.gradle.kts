// :deephaven-remote-uri -- a python module wrapped in Gradle (doc 10 §12).
//
// This is not a Java project: only the `base` lifecycle plugin is applied, which
// supplies the `check` / `build` / `clean` tasks. `check` depends on `pytest`, so
// the root `./gradlew build` runs the remote_uri unit tests alongside the Java ones,
// exactly like :deephaven-app-multi-oms-blotter.
plugins {
    base
}

description =
    "Remote-URI leaves and collector: N Deephaven servers folding AMPS FIX 4.2 hub tapes (remote_uri)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs remote_uri in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    commandLine("bash", testRunner.absolutePath)
    // Unit tests are cheap and depend on the interpreter/venv state, so never
    // cache them as up-to-date.
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}
