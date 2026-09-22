plugins {
    id("io.micronaut.application") version "5.0.2"
    id("com.gradleup.shadow") version "9.6.1"
    id("io.micronaut.aot") version "5.0.2"
    id("com.diffplug.spotless") version "8.10.2"
    id("io.github.ben-manes.versions") version "0.64.0"
}

version = "0.5.1"
group = "com.dalugm.opcdapter"

repositories {
    maven {
        url = uri("https://maven.aliyun.com/repository/public")
        // The mirror currently publishes metadata but not the macOS runtime artifact. Once
        // Gradle selects a repository for a module, it will not fetch the missing jar elsewhere.
        content { excludeModule("io.micronaut", "micronaut-runtime-osx") }
    }
    mavenCentral()
}

configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.bouncycastle" && requested.name == "bcprov-jdk15on") {
            useVersion("1.70")
        }
    }
}

dependencies {
    // Versions are aligned by the Micronaut Platform BOM imported by the application plugin.
    implementation("com.google.protobuf:protobuf-java")
    implementation("io.grpc:grpc-stub")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.micronaut.grpc:micronaut-grpc-server-runtime")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    implementation("io.micronaut.toml:micronaut-toml")
    implementation("io.micronaut:micronaut-management")
    implementation("org.openscada.utgard:org.openscada.opc.lib:1.5.0")
    implementation("org.slf4j:jul-to-slf4j")
    runtimeOnly("ch.qos.logback:logback-classic")
    testImplementation("io.grpc:grpc-testing")
    testImplementation("io.grpc:grpc-inprocess")
    testImplementation("ch.qos.logback:logback-classic")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "com.dalugm.opcdapter.Application"
}

sourceSets.main {
    java.srcDir("gen/java")
}

java {
    sourceCompatibility = JavaVersion.toVersion("25")
    targetCompatibility = JavaVersion.toVersion("25")
}

graalvmNative.toolchainDetection = false
graalvmNative {
    binaries {
        all {
            buildArgs.add("-H:+SharedArenaSupport")
        }
    }
}

micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("com.dalugm.opcdapter.*")
    }
    aot {
        // Please review carefully the optimizations enabled below
        // Check https://micronaut-projects.github.io/micronaut-aot/latest/guide/ for more details
        optimizeServiceLoading = false
        convertYamlToJava = false
        precomputeOperations = true
        cacheEnvironment = true
        optimizeClassLoading = true
        deduceEnvironment = true
        optimizeNetty = true
        replaceLogbackXml = true
    }
}

// Forward OPC DA live-test system properties to the test JVM so that the
// @EnabledIfSystemProperty-gated OpcDaReadTest / OpcDaWriteTest can pick them up.
// Only forward when actually set (-D) so unset properties fall back to the
// test-level defaults rather than being overridden by an empty string.
tasks.test {
    listOf("opcda.it.enabled").forEach {
        val v = System.getProperty(it)
        if (v != null) systemProperty(it, v)
    }
    // Echo test stdout/stderr to the console, but only when running the live opcda
    // tests — keeps the normal `./gradlew test` output quiet.
    testLogging {
        showStandardStreams = System.getProperty("opcda.it.enabled") != null
    }
    // Live opcda tests probe changing data — always re-run them; normal test
    // runs keep standard up-to-date caching.
    outputs.upToDateWhen { System.getProperty("opcda.it.enabled") == null }
}

// https://docs.gradle.org/current/userguide/upgrading_major_version_9.html#test_task_fails_when_no_tests_are_discovered
tasks.withType<AbstractTestTask>().configureEach {
    failOnNoDiscoveredTests = false
}

spotless {
    // // optional: limit format enforcement to just the files changed by this feature branch
    // ratchetFrom("origin/main")

    format("misc") {
        // define the files to apply `misc` to
        target("*.gradle", ".gitattributes", ".gitignore")

        // define the steps to apply to those files
        trimTrailingWhitespace()
        leadingSpacesToTabs() // or leadingTabsToSpaces(4)
        endWithNewline()
    }

    kotlin {
        ktlint()

        trimTrailingWhitespace()
        endWithNewline()
    }

    kotlinGradle {
        ktlint()

        trimTrailingWhitespace()
        endWithNewline()
    }

    java {
        targetExclude("gen/java/**")
        // don't need to set target, it is inferred from java

        // apply a specific flavor of google-java-format
        googleJavaFormat("1.35.0").aosp().reflowLongStrings()

        // fix formatting of type annotations
        formatAnnotations()

        // Group imports: stdlib → framework → third-party → our code.
        importOrder("java", "javax", "io", "jakarta", "org", "com")

        // make sure every file has the following copyright header.
        // optionally, Spotless can set copyright years by digging
        // through git history (see "license" section below)
        licenseHeader("/* (C) \$YEAR */")
    }
}
