plugins {
    id("truesim.java-library-conventions")
    id("truesim.allocation-free")
}

description = "Deterministic parallel jobs for true-sim: a fixed worker pool running index ranges in stable chunks."

coverage {
    minimum = 1.0
}

// Benchmarks (TRUESIM_BENCHMARK=true) measure the code, not its coverage probes.
tasks.test {
    if (providers.environmentVariable("TRUESIM_BENCHMARK").orNull == "true") {
        extensions.configure<JacocoTaskExtension> { isEnabled = false }
    }
}
