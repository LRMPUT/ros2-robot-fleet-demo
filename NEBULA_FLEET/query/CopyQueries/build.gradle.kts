plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.chistera"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("stream.nebula:nebulastream-java-client:0.0.93")
}

application {
    mainClass.set("org.chistera.CopyQueries")
}