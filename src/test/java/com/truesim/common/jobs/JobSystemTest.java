package com.truesim.common.jobs;

import static com.truesim.testing.AllocationAssertions.assertAllocationFree;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class JobSystemTest {

    private static final int[] THREADS = {1, 2, 3, 8, 100};

    @Test
    void everyIndexRunsExactlyOnce() {
        for (int threads : THREADS) {
            try (JobSystem jobs = new JobSystem(threads)) {
                assertThat(jobs.threads()).isEqualTo(threads);
                for (int count : new int[] {0, 1, 7, 1000, 12345}) {
                    for (int grain : new int[] {1, 7, 64}) {
                        AtomicIntegerArray hits = new AtomicIntegerArray(count);
                        AtomicInteger badWorker = new AtomicInteger();
                        jobs.parallelFor(count, grain, (from, to, worker) -> {
                            if (worker < 0 || worker >= threads || to - from > grain || from >= to) {
                                badWorker.incrementAndGet();
                            }
                            for (int i = from; i < to; i++) {
                                hits.incrementAndGet(i);
                            }
                        });
                        for (int i = 0; i < count; i++) {
                            assertThat(hits.get(i)).as("index %d, %d threads", i, threads).isEqualTo(1);
                        }
                        assertThat(badWorker.get()).isZero();
                    }
                }
            }
        }
    }

    @Test
    void chunkOrderedResultsAreIdenticalOnAnyNumberOfThreads() {
        Random random = new Random(1);
        double[] values = new double[100_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextGaussian() * Math.pow(10, random.nextInt(12));
        }
        int grain = 333;
        double[] reference = null;
        for (int threads : THREADS) {
            double[] partial = new double[JobSystem.chunkCount(values.length, grain)];
            try (JobSystem jobs = new JobSystem(threads)) {
                jobs.parallelChunks(values.length, grain, (chunk, from, to, worker) -> {
                    double sum = 0;
                    for (int i = from; i < to; i++) {
                        sum += values[i];
                    }
                    partial[chunk] = sum;
                });
            }
            if (reference == null) {
                reference = partial;
            }
            assertThat(partial).as("%d threads", threads).containsExactly(reference);
        }
    }

    @Test
    void callerWaitsForSlowWorkers() {
        try (JobSystem jobs = new JobSystem(4)) {
            AtomicIntegerArray workers = new AtomicIntegerArray(4);
            CountDownLatch claimed = new CountDownLatch(1);
            jobs.parallelFor(12, 1, (from, to, worker) -> {
                workers.incrementAndGet(worker);
                if (worker == 0) {
                    await(claimed); // the caller runs out of work while a worker still sleeps: it must wait
                } else {
                    claimed.countDown();
                    sleep(50);
                }
            });
            int total = 0;
            for (int w = 0; w < 4; w++) {
                total += workers.get(w);
            }
            assertThat(total).isEqualTo(12);
            assertThat(workers.get(0)).isPositive(); // the caller helps
            // Idle workers park between jobs and wake for the next one.
            sleep(50);
            AtomicInteger runs = new AtomicInteger();
            jobs.parallelFor(100, 1, (from, to, worker) -> runs.incrementAndGet());
            assertThat(runs.get()).isEqualTo(100);
        }
    }

    @Test
    void failuresReachTheCaller() {
        try (JobSystem jobs = new JobSystem(3)) {
            IllegalArgumentException runtime = new IllegalArgumentException("chunk 5");
            assertThatThrownBy(() -> jobs.parallelFor(100, 1, (from, to, worker) -> {
                if (from == 5) {
                    throw runtime;
                }
            })).isSameAs(runtime);
            AssertionError error = new AssertionError("broken");
            assertThatThrownBy(() -> jobs.parallelChunks(100, 1, (chunk, from, to, worker) -> {
                if (chunk == 50) {
                    throw error;
                }
            })).isSameAs(error);
            IOException checked = new IOException("disk");
            assertThatIllegalStateException().isThrownBy(() -> jobs.parallelFor(100, 1, (from, to, worker) -> {
                if (from == 99) {
                    sneakyThrow(checked);
                }
            })).withCause(checked);
            // Still usable afterwards.
            AtomicInteger runs = new AtomicInteger();
            jobs.parallelFor(100, 3, (from, to, worker) -> runs.addAndGet(to - from));
            assertThat(runs.get()).isEqualTo(100);
        }
    }

    @Test
    void nestedJobsRunInline() {
        try (JobSystem jobs = new JobSystem(4); JobSystem other = new JobSystem(1); JobSystem third = new JobSystem(2)) {
            AtomicIntegerArray hits = new AtomicIntegerArray(4 * 8);
            AtomicReference<String> problem = new AtomicReference<>();
            // Four chunks that each wait for all four threads: every thread, the caller included, runs exactly one.
            CyclicBarrier together = new CyclicBarrier(4);
            jobs.parallelFor(4, 1, (outerFrom, outerTo, outerWorker) -> {
                await(together);
                // A job from inside a task runs on this thread, with this thread's worker index.
                jobs.parallelChunks(8, 1, (chunk, from, to, worker) -> {
                    if (worker != outerWorker) {
                        problem.set("worker " + worker + " inside " + outerWorker);
                    }
                    hits.incrementAndGet(outerFrom * 8 + from);
                });
                jobs.parallelFor(8, 2, (from, to, worker) -> {
                    if (worker != outerWorker) {
                        problem.set("worker " + worker + " inside " + outerWorker);
                    }
                });
                // Another system, used from this one's threads.
                other.parallelFor(2, 1, (from, to, worker) -> {
                    if (worker != 0) {
                        problem.set("other system's worker " + worker);
                    }
                });
            });
            assertThat(problem.get()).isNull();
            for (int i = 0; i < hits.length(); i++) {
                assertThat(hits.get(i)).isEqualTo(1);
            }
            // A worker of one system can submit to another (one submitter at a time).
            AtomicInteger runs = new AtomicInteger();
            jobs.parallelFor(4, 1, (from, to, worker) -> {
                await(together);
                if (worker == 1) {
                    third.parallelFor(10, 1, (a, b, w) -> runs.incrementAndGet());
                }
            });
            assertThat(runs.get()).isEqualTo(10);
        }
    }

    @Test
    void oneSubmitterAtATime() throws InterruptedException {
        try (JobSystem jobs = new JobSystem(2)) {
            AtomicReference<Throwable> other = new AtomicReference<>();
            Thread outsider = new Thread(() -> {
                try {
                    jobs.parallelFor(10, 1, (from, to, worker) -> { });
                } catch (Throwable thrown) {
                    other.set(thrown);
                }
            });
            jobs.parallelFor(2, 1, (from, to, worker) -> {
                if (from == 0) {
                    outsider.start();
                    try {
                        outsider.join();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
            assertThat(other.get()).isInstanceOf(IllegalStateException.class)
                .hasMessage("Jobs are submitted from one thread at a time");
        }
    }

    @Test
    void validationAndClosing() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JobSystem(0));
        JobSystem jobs = new JobSystem(3);
        assertThatIllegalArgumentException().isThrownBy(() -> jobs.parallelFor(-1, 1, (from, to, worker) -> { }));
        assertThatIllegalArgumentException().isThrownBy(() -> jobs.parallelFor(10, 0, (from, to, worker) -> { }));
        assertThat(JobSystem.chunkCount(0, 5)).isZero();
        assertThat(JobSystem.chunkCount(10, 5)).isEqualTo(2);
        assertThat(JobSystem.chunkCount(11, 5)).isEqualTo(3);
        assertThat(JobSystem.chunkCount(Integer.MAX_VALUE, 2)).isEqualTo(1 << 30);
        // Closing while interrupted still stops the workers, and keeps the interrupt.
        Thread.currentThread().interrupt();
        jobs.close();
        assertThat(Thread.interrupted()).isTrue();
        jobs.close(); // twice is fine
        assertThatIllegalStateException().isThrownBy(() -> jobs.parallelFor(10, 1, (from, to, worker) -> { }));
        try (JobSystem all = JobSystem.ofAvailableProcessors()) {
            assertThat(all.threads()).isEqualTo(Runtime.getRuntime().availableProcessors());
        }
    }

    private long sink;

    @Test
    void submittingAllocatesNothing() {
        try (JobSystem jobs = new JobSystem(4)) {
            long[] sums = new long[JobSystem.chunkCount(10_000, 100)];
            JobSystem.ChunkTask sum = (chunk, from, to, worker) -> {
                long s = 0;
                for (int i = from; i < to; i++) {
                    s += i;
                }
                sums[chunk] = s;
            };
            JobSystem.RangeTask touch = (from, to, worker) -> sums[from / 100] += worker;
            assertAllocationFree(() -> {
                jobs.parallelChunks(10_000, 100, sum);
                jobs.parallelFor(10_000, 100, touch);
                sink += sums[7];
            }, 200, 200, 10);
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | java.util.concurrent.BrokenBarrierException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable thrown) throws T {
        throw (T) thrown;
    }
}
