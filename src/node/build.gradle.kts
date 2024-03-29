/*
 * This file was generated by the Gradle 'init' task.
 */

import org.gradle.jvm.tasks.Jar

plugins {
    id("io.hnsn.kaukus.java-application-conventions")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.3.0"
}

dependencies {
    implementation(project(":persistence"))
    implementation(project(":utilities"))

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("org.slf4j:slf4j-simple:1.7.30")

    // State Machinary
    implementation("com.github.stateless4j:stateless4j:2.6.0")

    // Command line parsing
    implementation("com.beust:jcommander:1.82")

    // Configurations
    implementation("com.typesafe:config:1.4.1")

    // Dependency Injection
    implementation("com.google.inject:guice:5.1.0")

    // Avro / Binary Encoding
    implementation("org.apache.avro:avro:1.11.0")
    implementation("org.xerial.snappy:snappy-java:1.1.8.4")

    // Jetty
    implementation("org.eclipse.jetty:jetty-server:9.4.3.v20170317")
    implementation("org.eclipse.jetty:jetty-servlet:9.4.3.v20170317")

    compileOnly("org.projectlombok:lombok:1.18.22")
	annotationProcessor("org.projectlombok:lombok:1.18.22")

    testCompileOnly("org.projectlombok:lombok:1.18.22")
	testAnnotationProcessor("org.projectlombok:lombok:1.18.22")
}

application {
    // Define the main class for the application.
    mainClass.set("io.hnsn.kaukus.App")
}

val fatJar = task("fatJar", type = Jar::class) {
    baseName = "${project.name}-fat"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Implementation-Title"] = "Kaukus"
        attributes["Implementation-Version"] = archiveVersion
        attributes["Main-Class"] = "io.hnsn.kaukus.App"
    }
    from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))
    with(tasks.jar.get() as CopySpec)
}

tasks {
    "build" {
        dependsOn(fatJar)
    }
}