// :market-data-demo -- a python module wrapped in Gradle (doc 11 §12).
//
// This is not a Java project: only the `base` lifecycle plugin is applied, which
// supplies the `check` / `build` / `clean` tasks. `check` depends on `pytest`, so
// the root `./gradlew build` runs the market_data_demo unit tests alongside the
// Java ones, exactly like :deephaven-scripts and :deephaven-app-multi-oms-blotter.
plugins {
    base
}

description =
    "Historical OHLC market data (parquet, local or S3) as a Deephaven candlestick / time-series dashboard (market_data_demo)"

val moduleDir = layout.projectDirectory
val testRunner = moduleDir.file("run_tests.sh").asFile

val pytest = tasks.register<Exec>("pytest") {
    group = "verification"
    description =
        "Creates .venv (python3 -m venv), installs market_data_demo in editable mode and runs the pytest suite."
    workingDir(moduleDir)
    commandLine("bash", testRunner.absolutePath)
    // Unit tests are cheap and depend on the interpreter/venv state, so never
    // cache them as up-to-date.
    outputs.upToDateWhen { false }
}

tasks.named("check") {
    dependsOn(pytest)
}

// `./gradlew :market-data-demo:generateMockData` -- the same venv, the generator CLI.
// Symbols / period / seed come from the module's defaults; override with
//   ./gradlew :market-data-demo:generateMockData --args="--symbols AAPL,MSFT --start 2026-08-03 --end 2026-09-04"
val generateMockData = tasks.register<Exec>("generateMockData") {
    group = "application"
    description = "Generates the mock parquet market data under market-data-demo/data (python -m market_data_demo generate)."
    workingDir(moduleDir)
    val extra = (project.findProperty("args") as String?)?.split(" ")?.filter { it.isNotBlank() } ?: emptyList()
    commandLine(listOf("bash", moduleDir.file("scripts/generate_mock_data.sh").asFile.absolutePath) + extra)
    outputs.upToDateWhen { false }
}
