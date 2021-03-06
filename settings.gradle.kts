/*
 * This file was generated by the Gradle 'init' task.
 *
 * The settings file is used to specify which projects to include in your build.
 *
 * Detailed information about configuring a multi-project build in Gradle can be found
 * in the user manual at https://docs.gradle.org/7.3/userguide/multi_project_builds.html
 */

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "kaukus"
include("node", "persistence", "utilities")
project(":node").projectDir = file("src/node")
project(":persistence").projectDir = file("src/persistence")
project(":utilities").projectDir = file("src/utilities")