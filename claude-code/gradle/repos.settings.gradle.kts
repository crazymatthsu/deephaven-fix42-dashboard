// Every Gradle repository, from repos.env (docs/15).
//
// Applied from the pluginManagement block of settings.gradle.kts AND of
// build-logic/settings.gradle.kts, so the plugin repositories, the dependency repositories
// and the build-logic included build all follow the same file. It reads the same files with
// the same rules as scripts/repos.sh:
//
//   precedence, first definition wins: environment > override file > repos.env
//   override file = $REPOS_ENV if set, else repos.local.env next to repos.env
//   KEY=value lines, optional `export `, ${KEY} references, '#' comments on their own line
//
// Keys used here: MAVEN_REPO_URL, GRADLE_PLUGIN_REPO_URL (empty = Maven Central / Gradle
// Plugin Portal), MAVEN_REPO_USERNAME / MAVEN_REPO_PASSWORD (also read from
// ~/.gradle/gradle.properties). GRADLE_JDK_AUTO_DOWNLOAD is read by settings.gradle.kts, and
// JRE_BASE_IMAGE / APT_MIRROR by the dh.connector-app image build, from the map this script
// publishes as gradle.extra["repos"].
//
// dependencyResolutionManagement uses FAIL_ON_PROJECT_REPOS: a module that declares its own
// `repositories { mavenCentral() }` fails the build instead of quietly bypassing the mirror.

import java.io.File

val reposDefaults: File = generateSequence(settingsDir) { it.parentFile }
    .map { File(it, "repos.env") }
    .firstOrNull { it.isFile }
    ?: throw GradleException("repos.env not found in $settingsDir or any parent directory (docs/15)")

val reposConfig: Map<String, String> = run {
    val keyPattern = Regex("[A-Za-z_][A-Za-z0-9_]*")
    val refPattern = Regex("""\$\{([^}]*)}""")
    val values = linkedMapOf<String, String>()
    fun env(key: String): String? = providers.environmentVariable(key).orNull

    fun load(file: File) {
        file.readLines().forEach { raw ->
            val line = raw.trimEnd('\r').trim().removePrefix("export ").trim()
            if (line.isEmpty() || line.startsWith("#")) return@forEach
            val eq = line.indexOf('=')
            if (eq < 0) return@forEach
            val key = line.substring(0, eq).trim()
            if (!keyPattern.matches(key) || key in values) return@forEach
            var value = line.substring(eq + 1).trim()
            if (value.length >= 2 && value.first() == value.last() && value.first() in "\"'") {
                value = value.substring(1, value.length - 1)
            }
            values[key] = env(key) ?: refPattern.replace(value) { ref ->
                val name = ref.groupValues[1]
                env(name) ?: values[name] ?: ""
            }
        }
    }

    val explicit = env("REPOS_ENV")?.takeIf { it.isNotBlank() }
    val override = if (explicit != null) {
        File(explicit).also {
            if (!it.isFile) throw GradleException("REPOS_ENV=$explicit does not exist (docs/15)")
        }
    } else {
        File(reposDefaults.parentFile, "repos.local.env").takeIf { it.isFile }
    }
    override?.let(::load)
    load(reposDefaults)
    values
}

fun repoSetting(key: String): String? = reposConfig[key]?.takeIf { it.isNotBlank() }
fun credential(key: String): String? =
    repoSetting(key) ?: providers.gradleProperty(key).orNull?.takeIf { it.isNotBlank() }

fun RepositoryHandler.mirror(repoName: String, repoUrl: String) {
    maven {
        name = repoName
        setUrl(repoUrl)
        val user = credential("MAVEN_REPO_USERNAME")
        val pass = credential("MAVEN_REPO_PASSWORD")
        if (user != null && pass != null) {
            credentials {
                username = user
                password = pass
            }
        }
    }
}

fun RepositoryHandler.pluginRepository() {
    repoSetting("GRADLE_PLUGIN_REPO_URL")?.let { mirror("pluginMirror", it) } ?: gradlePluginPortal()
}

fun RepositoryHandler.mavenRepository() {
    repoSetting("MAVEN_REPO_URL")?.let { mirror("mavenMirror", it) } ?: mavenCentral()
}

// `settings.pluginManagement`, not a top-level `pluginManagement {}` block: the Kotlin DSL
// lifts that block into a separate compile stage where the functions above do not exist.
settings.pluginManagement.repositories {
    // Maven first: most plugin implementation jars live there; the plugin repository
    // supplies the plugin markers (and anything else only it has).
    mavenRepository()
    pluginRepository()
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenRepository()
    }
}

gradle.extra["repos"] = reposConfig
