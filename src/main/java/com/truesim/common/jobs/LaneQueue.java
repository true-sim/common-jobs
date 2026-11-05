package com.truesim.common.jobs;

/**
 * How a lane's ready queue handles threads that submit and finish tasks at the same time. Chosen per lane with
 * {@link LaneSpec#withQueue(LaneQueue)}; measured with {@code LaneBenchmark}.
 */
public enum LaneQueue {

    /**
     * The default: submitting a task never waits for threads that finish tasks, about 0.1 to 0.3 µs while tasks run,
     * against 1 to 6 µs with {@link #LOCKED}. Also works off a backlog of queued tasks at least as fast, and runs
     * tasks from one producer, or on virtual threads, faster.
     */
    LOCK_FREE,

    /**
     * One lock for submitting and finishing tasks: submitting waits for threads that finish tasks. Gets about 10 to
     * 25% more tiny tasks per second through a platform lane of several threads while several threads submit at once.
     */
    LOCKED
}
