plugins {
    id("java")
    id("com.gradleup.shadow") version "8.3.5"
}

group = "me.latiao"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "opencollab"
        url = uri("https://repo.opencollab.dev/main/")
    }
}

dependencies {
    implementation("org.geysermc.mcprotocollib:protocol:26.2")
    implementation("net.kyori:adventure-text-serializer-plain:5.2.0")
    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}


tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "me.latiao.Bot"
    }
}


tasks.assemble {
    dependsOn(tasks.shadowJar)
}

tasks.test {
    useJUnitPlatform()
}