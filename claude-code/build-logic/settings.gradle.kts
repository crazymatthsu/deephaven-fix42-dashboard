// build-logic is an INCLUDED BUILD (wired in via pluginManagement in the root
// settings.gradle.kts), not buildSrc: buildSrc invalidates the whole build's
// configuration on any change, an included build is compiled and cached like any
// other project. It exists so that a connector application's build file stays a
// few lines however many applications there are.

pluginManagement {
    // The same repositories as the root build, from repos.env (docs/15).
    apply(from = "../gradle/repos.settings.gradle.kts")
}

rootProject.name = "build-logic"
