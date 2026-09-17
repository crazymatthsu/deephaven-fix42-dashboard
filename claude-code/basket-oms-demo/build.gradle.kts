// :basket-oms-demo -- a python module wrapped in Gradle (doc 13 §7).
//
// Not a Java project: only the `base` lifecycle plugin is applied (check / build /
// clean). `check` depends on `pytest`, so the root `./gradlew build` runs the
// basket_oms_demo unit tests alongside everything else, exactly like :market-data-demo.
plugins {
    base
}

description =
    "Deephaven as an equities OMS front end: basket ticket, basket list, order blotter with a context menu (basket_oms_demo)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs basket_oms_demo in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    commandLine("bash", testRunner.absolutePath)
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}
