pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
}

plugins {
    id("jp.co.gahojin.refreshVersions") version "0.1.4"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        mavenLocal()
    }
}

refreshVersions {
    sortSection = true
}

rootProject.name = "parquet-kotlin"
include(":parquet-column")
include(":parquet-common")
include(":parquet-encoding")
include(":parquet-format-structures")
include(":parquet-generator")
include(":parquet-hadoop")
