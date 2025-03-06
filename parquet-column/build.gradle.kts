import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import org.gradle.kotlin.dsl.withType
import org.jetbrains.dokka.gradle.tasks.DokkaGenerateTask
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

val artifactId = "parquet-column"

group = Maven.GROUP_ID
version = Maven.VERSION

dependencies {
    api(project(":parquet-common"))
    api(project(":parquet-encoding"))

    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib)
    implementation(libs.okio)
    implementation(libs.fastutil)
    implementation(libs.slf4j.api)
    implementation(libs.zero.allocation.hashing)

    testImplementation(libs.junit)
    testImplementation(libs.junit.vintage.engine)
    testImplementation(libs.junit.benchmarks)
    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.mockk)
    testImplementation(libs.mockito)
    testImplementation(libs.slf4j.simple)
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

sourceSets {
    named("main") {
        java.srcDirs(project.layout.buildDirectory.dir("generated/sources/generator"))
        kotlin.srcDirs(project.layout.buildDirectory.dir("generated/sources/generator"))
    }
    named("test") {
        java.srcDirs(project.layout.buildDirectory.dir("generated/sources/generator"))
        kotlin.srcDirs(project.layout.buildDirectory.dir("generated/sources/generator"))
    }
}

abstract class GeneratorTask @Inject constructor(
    private val execOperations: ExecOperations,
) : DefaultTask() {
    @get:InputFiles
    lateinit var serverClasspath: Provider<FileCollection>

    @get:Input
    lateinit var generatorClass: String

    @get:OutputDirectory
    lateinit var outputDir: Provider<Directory>

    @TaskAction
    fun run() {
        execOperations.exec {
            commandLine("java", "-cp", serverClasspath.get().asPath, generatorClass, outputDir.get().toString())
        }
    }
}

tasks.register<GeneratorTask>("filter2Generator") {
    group = JavaBasePlugin.BUILD_TASK_NAME
    description = "generate filter2 files"

    serverClasspath = project(":parquet-generator").sourceSets.named("main").map { it.runtimeClasspath }
    generatorClass = "org.apache.parquet.filter2.Generator"
    outputDir = project.layout.buildDirectory.dir("generated/sources/generator")
}

tasks.withType<DokkaGenerateTask> { dependsOn("filter2Generator") }
tasks.withType<Jar> { dependsOn("filter2Generator") }
tasks.findByName("compileKotlin")?.dependsOn("filter2Generator")

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
    exclude("**/benchmark/*")
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
