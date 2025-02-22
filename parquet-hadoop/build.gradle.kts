import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.kover)
    alias(libs.plugins.detekt)
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)
    alias(libs.plugins.maven.publish)
    id("signing")
}

val artifactId = "parquet-hadoop"

group = Maven.GROUP_ID
version = Maven.VERSION

dependencies {
    api(project(":parquet-column"))
    api(project(":parquet-common"))
    api(project(":parquet-format-structures"))

    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib)
    implementation(libs.okio)
    implementation(libs.commons.pool)
    implementation(libs.fastutil)
    implementation(libs.slf4j.api)

    api(libs.hadoop.client)
    api(libs.hadoop.common) {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
    }
    // compressor
    implementation(libs.snappy.java)
    implementation(libs.aircompressor)
    implementation(libs.zstd.jni)

    testImplementation(project(":parquet-column").dependencyProject.sourceSets["test"].output)
    testImplementation(libs.junit)
    testImplementation(libs.junit.vintage.engine)
    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.mockk)
    testImplementation(libs.mockito)
    testImplementation(libs.okhttp3)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.zero.allocation.hashing)
}

detekt {
    parallel = true
    buildUponDefaultConfig = true
    allRules = false
    autoCorrect = true
    config.setFrom(rootDir.resolve("config/detekt.yml"))
}

java {
    toolchain {
        sourceCompatibility = Build.sourceCompatibility
        targetCompatibility = Build.targetCompatibility
    }
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_11
        freeCompilerArgs.add("-Xjvm-default=all")
    }
}

tasks.withType<Detekt>().configureEach {
    jvmTarget = "11"
    reports {
        html.required.set(false)
        xml.required.set(false)
        txt.required.set(false)
        sarif.required.set(true)
        md.required.set(true)
    }
    exclude("build/")
    exclude("resources/")
}

tasks.withType<DetektCreateBaselineTask>().configureEach {
    jvmTarget = "11"
    exclude("build/")
    exclude("resources/")
}

tasks.withType<Test>().configureEach {
//    systemProperty("org.slf4j.simpleLogger.defaultLogLevel","DEBUG")
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
    maxHeapSize = "2G"
}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

mavenPublishing {
    configure(KotlinJvm(
        javadocJar = JavadocJar.Dokka("dokkaGenerateModuleJavadoc"),
        sourcesJar = true,
    ))

    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)

    coordinates(Maven.GROUP_ID, artifactId, Maven.VERSION)

    pom {
        name = artifactId
        description = Maven.DESCRIPTION
        url = "https://github.com/${Maven.GITHUB_REPOSITORY}/"
        licenses {
            license {
                name = Maven.LICENSE_NAME
                url = Maven.LICENSE_URL
                distribution = Maven.LICENSE_DIST
            }
        }
        developers {
            developer {
                id = Maven.DEVELOPER_ID
                name = Maven.DEVELOPER_NAME
                url = Maven.DEVELOPER_URL
            }
        }
        scm {
            url = "https://github.com/${Maven.GITHUB_REPOSITORY}/"
            connection = "scm:git:git://github.com/${Maven.GITHUB_REPOSITORY}.git"
            developerConnection = "scm:git:ssh://git@github.com/${Maven.GITHUB_REPOSITORY}.git"
        }
    }
}
