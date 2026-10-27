plugins {
    id("truesim.java-library-conventions")
    id("truesim.allocation-free")
}

description = "Deterministic parallel jobs for true-sim: a fixed worker pool running index ranges in stable chunks."

coverage {
    minimum = 1.0
}
