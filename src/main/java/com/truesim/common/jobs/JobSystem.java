package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * A fixed pool of threads that runs index ranges in parallel, deterministically.
 *
 * <p>{@link #parallelFor} splits {@code 0..count} into chunks of {@code grain} indices. The chunk boundaries depend only
 * on {@code count} and {@code grain}, never on the number of threads, and every chunk runs exactly once. Threads claim
 * chunks as they become free, which balances uneven work; which thread runs a chunk is not fixed. So a task gives the
 * same result on any number of threads when its output depends only on the indices it is given: write results by
 * index, and combine per-chunk partial results in chunk order ({@link #parallelChunks}) rather than in completion
 * order.
 *
 * <pre>{@code
 * try (JobSystem jobs = new JobSystem(Runtime.getRuntime().availableProcessors())) {
 *     double[] partial = new double[JobSystem.chunkCount(n, 256)];
 *     jobs.parallelChunks(n, 256, (chunk, from, to, worker) -> {
 *         double sum = 0;
 *         for (int i = from; i < to; i++) {
 *             sum += values[i];
 *         }
 *         partial[chunk] = sum;
 *     });
 *     double total = 0;
 *     for (double p : partial) {
 *         total += p; // in chunk order: the same total on 1 or 100 threads
 *     }
 * }
 * }</pre>
 *
 * <p>The calling thread works too: a system with {@code threads} threads starts {@code threads - 1} workers, and one
 * thread runs everything inline. Tasks get a worker index from 0 to {@code threads - 1}, e.g. to pick a per-thread
 * context; the caller is worker 0. Jobs are submitted from one thread at a time; a job started from inside a task runs
 * inline on that thread. Submitting a job allocates nothing; tasks are best created once and reused.
 */
public final class JobSystem implements AutoCloseable {

    /** Work on the indices {@code from} (inclusive) to {@code to} (exclusive), on worker {@code worker}. */
    @FunctionalInterface
    public interface RangeTask {
        void run(int from, int to, int worker);
    }

    /** Like {@link RangeTask}, with the chunk's index: chunk {@code c} covers {@code c * grain} up to the next one. */
    @FunctionalInterface
    public interface ChunkTask {
        void run(int chunk, int from, int to, int worker);
    }

    /** Busy-wait rounds before a waiting thread parks: short waits stay fast, long ones free the core. */
    private static final int SPINS = 2_000;

    private final int threads;
    private final Worker[] workers;

    // The current job: written by the submitting thread before the generation, read by workers after it.
    private ChunkTask task;
    private int count;
    private int grain;
    private int chunks;
    private final AtomicInteger nextChunk = new AtomicInteger();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile long generation;
    private final AtomicReference<Thread> submitter = new AtomicReference<>();
    private volatile boolean closed;

    // parallelFor runs a RangeTask through this adapter, so submitting allocates nothing.
    private final ChunkTask rangeAdapter;
    private RangeTask rangeTask;

    /** A system with {@code threads} threads in all: the caller plus {@code threads - 1} workers. */
    @AllowAllocation("starting the workers")
    public JobSystem(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("Need at least one thread: " + threads);
        }
        this.threads = threads;
        this.rangeAdapter = this::runRange;
        this.workers = new Worker[threads - 1];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(this, i + 1);
            workers[i].start();
        }
    }

    /** A system with one thread per available processor. */
    public static JobSystem ofAvailableProcessors() {
        return new JobSystem(Runtime.getRuntime().availableProcessors());
    }

    /** Number of threads, caller included: worker indices run from 0 to {@code threads() - 1}. */
    public int threads() {
        return threads;
    }

    /** Number of chunks {@code count} indices make in chunks of {@code grain}. */
    public static int chunkCount(int count, int grain) {
        return (int) (((long) count + grain - 1) / grain);
    }

    /**
     * Runs {@code task} over {@code 0..count} in chunks of {@code grain} indices, in parallel, and returns when every
     * chunk is done. If a task throws, no further chunks start and the first exception is rethrown here.
     */
    public void parallelFor(int count, int grain, RangeTask task) {
        int chunks = validate(count, grain);
        if (runsInline(chunks)) {
            int worker = workerIndex();
            for (int chunk = 0; chunk < chunks; chunk++) {
                task.run(chunk * grain, Math.min(count, chunk * grain + grain), worker);
            }
            return;
        }
        rangeTask = task;
        try {
            submit(count, grain, chunks, rangeAdapter);
        } finally {
            rangeTask = null;
        }
    }

    /** Like {@link #parallelFor}, with each chunk's index, for per-chunk results combined in chunk order. */
    public void parallelChunks(int count, int grain, ChunkTask task) {
        int chunks = validate(count, grain);
        if (runsInline(chunks)) {
            int worker = workerIndex();
            for (int chunk = 0; chunk < chunks; chunk++) {
                task.run(chunk, chunk * grain, Math.min(count, chunk * grain + grain), worker);
            }
            return;
        }
        submit(count, grain, chunks, task);
    }

    private void runRange(int chunk, int from, int to, int worker) {
        rangeTask.run(from, to, worker);
    }

    private int validate(int count, int grain) {
        if (count < 0 || grain < 1) {
            throw new IllegalArgumentException("Need count >= 0 and grain >= 1: " + count + ", " + grain);
        }
        if (closed) {
            throw new IllegalStateException("The job system is closed");
        }
        return chunkCount(count, grain);
    }

    /**
     * Whether to run on the current thread: with no workers, a single chunk, or when called from inside a running
     * task (by a worker, or by the submitter itself).
     */
    private boolean runsInline(int chunks) {
        if (workers.length == 0 || chunks <= 1) {
            return true;
        }
        Thread current = Thread.currentThread();
        if (current instanceof Worker worker && worker.system == this || current == submitter.get()) {
            return true;
        }
        if (!submitter.compareAndSet(null, current)) {
            throw new IllegalStateException("Jobs are submitted from one thread at a time");
        }
        return false; // claimed: this thread submits; submit() releases the claim
    }

    private int workerIndex() {
        return Thread.currentThread() instanceof Worker worker && worker.system == this ? worker.index : 0;
    }

    private void submit(int count, int grain, int chunks, ChunkTask task) {
        this.task = task;
        this.count = count;
        this.grain = grain;
        this.chunks = chunks;
        nextChunk.set(0);
        active.set(workers.length);
        generation++; // publishes the job to the workers
        for (Worker worker : workers) {
            LockSupport.unpark(worker);
        }
        runChunks(0);
        // Wait until every worker has finished with this job, so the next one starts clean.
        for (int spins = 0; active.get() > 0; spins++) {
            if (spins < SPINS) {
                Thread.onSpinWait();
            } else {
                LockSupport.park(this);
            }
        }
        this.task = null;
        submitter.set(null);
        Throwable thrown = failure.getAndSet(null);
        if (thrown != null) {
            throw rethrow(thrown);
        }
    }

    @AllowAllocation("only when a task failed")
    private static RuntimeException rethrow(Throwable thrown) {
        if (thrown instanceof RuntimeException runtime) {
            return runtime;
        }
        if (thrown instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("A job failed", thrown);
    }

    /** Claims and runs chunks until none are left. */
    private void runChunks(int worker) {
        for (int chunk = nextChunk.getAndIncrement(); chunk < chunks; chunk = nextChunk.getAndIncrement()) {
            int from = chunk * grain;
            try {
                task.run(chunk, from, Math.min(count, from + grain), worker);
            } catch (Throwable thrown) {
                failure.compareAndSet(null, thrown);
                nextChunk.set(chunks); // start no further chunks
            }
        }
    }

    /** Stops the workers. Jobs can no longer be submitted. */
    public void close() {
        closed = true;
        for (Worker worker : workers) {
            LockSupport.unpark(worker);
        }
        for (Worker worker : workers) {
            boolean interrupted = false;
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** A worker thread: waits for each new job, helps run its chunks, and reports when done. */
    private static final class Worker extends Thread {

        final JobSystem system;
        final int index;

        @AllowAllocation("naming the thread")
        Worker(JobSystem system, int index) {
            super("truesim-jobs-" + index);
            this.system = system;
            this.index = index;
            setDaemon(true);
        }

        @Override
        public void run() {
            long seen = 0;
            int spins = 0;
            while (true) {
                long current = system.generation;
                if (current != seen) {
                    seen = current;
                    spins = 0;
                    system.runChunks(index);
                    if (system.active.decrementAndGet() == 0) {
                        LockSupport.unpark(system.submitter.get());
                    }
                } else if (system.closed) {
                    return;
                } else if (spins++ < SPINS) {
                    Thread.onSpinWait();
                } else {
                    LockSupport.park(system);
                }
            }
        }
    }
}
