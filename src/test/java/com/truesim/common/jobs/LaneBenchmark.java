package com.truesim.common.jobs;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * What submitting a task costs, and how many tasks a lane gets through, while other threads submit and finish tasks.
 * Prints medians over measured rounds after warm-up rounds. Run with
 * {@code TRUESIM_BENCHMARK=true ./gradlew test --tests '*LaneBenchmark'}.
 */
@EnabledIfEnvironmentVariable(named = "TRUESIM_BENCHMARK", matches = "true")
class LaneBenchmark {

    private static final int TASKS = 4096;
    private static final int WARMUP = 5;
    private static final int ROUNDS = 15;

    /** As in scene's streaming: 4,096 submissions on a virtual lane of 16, with the lane full or tasks finishing. */
    @Test
    void submissions() throws InterruptedException {
        double[][] full = new double[2][ROUNDS];
        double[][] finishing = new double[2][ROUNDS];
        for (int round = -WARMUP; round < ROUNDS; round++) {
            submit(round, true, full);
            submit(round, false, finishing);
        }
        print("virtual lane of 16, lane full (tasks wait)", full);
        print("virtual lane of 16, tasks finishing      ", finishing);
    }

    private void submit(int round, boolean blocked, double[][] samples) throws InterruptedException {
        CountDownLatch gate = new CountDownLatch(blocked ? 1 : 0);
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.virtualLane("bench", 16);
            AtomicInteger done = new AtomicInteger();
            Runnable work = () -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                done.incrementAndGet();
            };
            long t0 = System.nanoTime();
            for (int i = 0; i < TASKS; i++) {
                lane.execute(work);
            }
            long t1 = System.nanoTime();
            gate.countDown();
            while (done.get() < TASKS) {
                Thread.onSpinWait();
            }
            long t2 = System.nanoTime();
            if (round >= 0) {
                samples[0][round] = (t1 - t0) / 1e3 / TASKS;
                samples[1][round] = TASKS / ((t2 - t0) / 1e6);
            }
        }
    }

    /** Several threads submit tiny tasks (four priorities) to one platform lane at once: cost and throughput. */
    @Test
    void contention() throws InterruptedException {
        int[] producers = {1, 4, 16};
        int[] slots = {1, 4, 16};
        double[][][][] samples = new double[producers.length][slots.length][2][ROUNDS];
        for (int round = -WARMUP; round < ROUNDS; round++) {
            for (int p = 0; p < producers.length; p++) {
                for (int s = 0; s < slots.length; s++) {
                    contend(round, producers[p], slots[s], samples[p][s]);
                }
            }
        }
        for (int p = 0; p < producers.length; p++) {
            for (int s = 0; s < slots.length; s++) {
                print(String.format(Locale.ROOT, "%2d producers, platform lane of %2d", producers[p], slots[s]),
                    samples[p][s]);
            }
        }
    }

    private void contend(int round, int producers, int slots, double[][] samples) throws InterruptedException {
        int perProducer = 4 * TASKS / producers;
        int total = perProducer * producers;
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.platformLane("bench", slots);
            AtomicInteger done = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            long[] submitNanos = new long[producers];
            Thread[] threads = new Thread[producers];
            for (int p = 0; p < producers; p++) {
                int index = p;
                threads[p] = Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    long t0 = System.nanoTime();
                    for (int i = 0; i < perProducer; i++) {
                        lane.task(done::incrementAndGet).priority(i & 3).submit();
                    }
                    submitNanos[index] = System.nanoTime() - t0;
                });
            }
            long t0 = System.nanoTime();
            start.countDown();
            while (done.get() < total) {
                Thread.onSpinWait();
            }
            long t1 = System.nanoTime();
            long submit = 0;
            for (int p = 0; p < producers; p++) {
                threads[p].join();
                submit += submitNanos[p];
            }
            if (round >= 0) {
                samples[0][round] = submit / 1e3 / total;
                samples[1][round] = total / ((t1 - t0) / 1e6);
            }
        }
    }

    /** Median submission cost (and the best round) and median rate over the measured rounds. */
    private static void print(String label, double[][] samples) {
        Arrays.sort(samples[0]);
        Arrays.sort(samples[1]);
        System.out.printf(Locale.ROOT, "%s: submit %.3f us per task (best %.3f), %.0f tasks per ms%n", label,
            samples[0][ROUNDS / 2], samples[0][0], samples[1][ROUNDS / 2]);
    }
}
