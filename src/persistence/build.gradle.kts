/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("io.hnsn.kaukus.java-library-conventions")
}

dependencies {
    api(project(":utilities"))
    implementation("com.esotericsoftware:kryo:5.0.4")

    compileOnly("org.projectlombok:lombok:1.18.22")
	annotationProcessor("org.projectlombok:lombok:1.18.22")

    testCompileOnly("org.projectlombok:lombok:1.18.22")
	testAnnotationProcessor("org.projectlombok:lombok:1.18.22")
}