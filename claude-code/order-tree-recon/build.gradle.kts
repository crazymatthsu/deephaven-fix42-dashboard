// :order-tree-recon -- a python module wrapped in Gradle (doc 14 section 9).
//
// Not a Java project: only the `base` lifecycle plugin is applied (check / build /
// clean). `check` depends on `pytest`, so the root `./gradlew build` runs the
// order_tree_recon unit tests alongside everything else, exactly like :basket-oms-demo.
plugins {
    base
}

description =
    "Raptor -> algo -> order router order tree with per-edge and per-level CumQty/LeavesQty reconciliation (order_tree_recon)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs order_tree_recon in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    // Forward slashes even on Windows: Git Bash's dirname cannot split C:\a\b paths.
    commandLine("bash", testRunner.invariantSeparatorsPath)
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}
