import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.JavadocJar
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import jp.co.gahojin.thrifty.gradle.FieldNameStyle
import jp.co.gahojin.thrifty.gradle.ThriftyTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.kover)
    alias(libs.plugins.detekt)
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.thrifty)
    id("signing")
}

val artifactId = "parquet-format-structures"

group = Maven.GROUP_ID
version = Maven.VERSION

configurations {
    register("thrift") { isTransitive = false }
}

dependencies {
    val thrift by configurations

    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib)

    thrift(libs.parquet.format)

    api(libs.thrifty.runtime) {
        exclude(group = "io.ktor")
    }

    testImplementation(libs.junit)
    testImplementation(libs.junit.vintage.engine)
    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.mockk)
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

tasks.register<Copy>("copySchema") {
    from(zipTree(configurations.named("thrift").map { it.singleFile })) {
        destinationDir = project.layout.buildDirectory.dir("thrift").get().asFile
    }
}

thrifty {
    kotlin {
        isGenerateServer = false
        generateServiceClients = false
        mutableFields = true
        jvmOverloads = true
        deepCopyFunc = true
        nameStyle = FieldNameStyle.JAVA
        jvmSuppressWildcards = true
    }
    sourceDir(project.layout.buildDirectory.dir("thrift").get().toString()) {
        include("**/*.thrift")
    }
}

tasks.withType<ThriftyTask>().configureEach {
    dependsOn("copySchema")
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
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
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

    publishToMavenCentral()

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
