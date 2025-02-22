pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
}

plugins {
    id("de.fayard.refreshVersions") version "0.60.5"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        mavenLocal()
    }
}

rootProject.name = "parquet-kotlin"
include(":parquet-column")
include(":parquet-common")
include(":parquet-encoding")
include(":parquet-format-structures")
include(":parquet-generator")
include(":parquet-hadoop")
