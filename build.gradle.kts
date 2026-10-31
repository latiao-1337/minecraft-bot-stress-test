plugins {
    id("java")
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
tasks.jar {
    manifest {
        attributes["Main-Class"] = "me.latiao.proxy_test"
    }
}
tasks.test {
    useJUnitPlatform()
}