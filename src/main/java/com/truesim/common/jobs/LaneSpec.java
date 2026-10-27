package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;

/**
 * The definition of a lane, without threads: safe to keep in a {@code static final} field and use from anywhere.
 * Each {@link TaskScheduler} creates its own {@link Lane} from it on first use ({@link TaskScheduler#lane(LaneSpec)})
 * and returns that lane from then on, so the definitions are global while threads and tasks stay with the scheduler
 * that owns them.
 *
 * <pre>{@code
 * public final class Lanes {
 *     public static final LaneSpec DOWNLOADS = LaneSpec.virtual("downloads", 1);
 *     public static final LaneSpec FILES = LaneSpec.serial("files");
 *     public static final LaneSpec DECODE = LaneSpec.platform("decode", 4);
 * }
 *
 * tasks.lane(Lanes.DOWNLOADS).submit(() -> fetch(url));
 * }</pre>
 *
 * <p>Specs are compared by identity: the spec object is the key, and its name only labels threads. Two specs with the
 * same name are two lanes.
 */
@AllowAllocation("definitions are made once")
public final class LaneSpec {

    private final String name;
    private final boolean virtual;
    private final int maxConcurrent;

    private LaneSpec(String name, boolean virtual, int maxConcurrent) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("A lane needs at least one slot: " + maxConcurrent);
        }
        this.name = name;
        this.virtual = virtual;
        this.maxConcurrent = maxConcurrent;
    }

    /** A lane on virtual threads, for I/O: at most {@code maxConcurrent} of its tasks run at once. */
    public static LaneSpec virtual(String name, int maxConcurrent) {
        return new LaneSpec(name, true, maxConcurrent);
    }

    /** A lane on {@code threads} platform threads, for CPU-bound background work. */
    public static LaneSpec platform(String name, int threads) {
        return new LaneSpec(name, false, threads);
    }

    /** A lane that runs one task at a time, in order of priority, then submission, on a virtual thread. */
    public static LaneSpec serial(String name) {
        return virtual(name, 1);
    }

    public String name() {
        return name;
    }

    /** Whether the lane runs on virtual threads; otherwise on platform threads. */
    public boolean isVirtual() {
        return virtual;
    }

    /** How many of the lane's tasks may run at once; for platform lanes also the number of threads. */
    public int maxConcurrent() {
        return maxConcurrent;
    }

    @Override
    public String toString() {
        return "LaneSpec " + name + " (" + (virtual ? "virtual" : "platform") + ", " + maxConcurrent + ")";
    }
}
