package com.truesim.common.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The lock-free lane under contention: many threads submitting, finishing and cancelling at once must keep the
 * concurrency limit, the priority-then-submission order and serial keys, run every task at most once, and leave no
 * ready task behind while a slot is free. Plus the races, replayed deterministically on one thread.
 */
class LaneTest {

    private static final Duration PATIENCE = Duration.ofSeconds(30);

    /** Starts {@code threads} threads at once, each running {@code body} with its index, and waits for all. */
    private static void concurrently(int threads, ThrowingIntConsumer body) throws InterruptedException {
        CyclicBarrier start = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        List<Thread> started = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int index = t;
            started.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    body.accept(index);
                } catch (Throwable e) {
                    failures.add(e);
                }
            }));
        }
        for (Thread thread : started) {
            thread.join();
        }
        assertThat(failures).isEmpty();
    }

    @FunctionalInterface
    private interface ThrowingIntConsumer {
        void accept(int value) throws Exception;
    }

    private static void awaitAll(List<? extends Task<?>> tasks) throws InterruptedException {
        for (Task<?> task : tasks) {
            assertThat(task.await(PATIENCE)).as("%s finished", task).isTrue();
        }
    }

    /**
     * Waits until the lane has released every slot and the scheduler has let go of every task: a task's waiters wake
     * just before that.
     */
    private static void awaitIdle(TaskScheduler tasks, Lane lane) {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        while ((lane.running() > 0 || tasks.pending() > 0) && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(lane.running()).isZero();
        assertThat(tasks.pending()).isZero();
    }

    /** A record of one submitted task: who submitted it, its priority and its sequence number. */
    private record Submitted(int producer, int priority, long sequence) {
    }

    @ParameterizedTest
    @EnumSource(LaneQueue.class)
    void manyProducersQueueInPriorityThenSubmissionOrder(LaneQueue queue) throws InterruptedException {
        for (int round = 0; round < 20; round++) {
            try (TaskScheduler tasks = new TaskScheduler()) {
                Lane lane = tasks.lane(LaneSpec.platform("serial", 1).withQueue(queue));
                CountDownLatch gate = new CountDownLatch(1);
                Task<?> blocker = lane.execute(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                });
                int producers = 8;
                int each = 200;
                ConcurrentLinkedQueue<Submitted> submitted = new ConcurrentLinkedQueue<>();
                ConcurrentLinkedQueue<Submitted> ran = new ConcurrentLinkedQueue<>();
                ConcurrentLinkedQueue<Task<?>> all = new ConcurrentLinkedQueue<>();
                long seed = round;
                concurrently(producers, p -> {
                    SplittableRandom random = new SplittableRandom(seed * 31 + p);
                    for (int i = 0; i < each; i++) {
                        int priority = random.nextInt(4);
                        Submitted[] self = new Submitted[1];
                        Task<?> task = lane.task(() -> ran.add(self[0])).priority(priority).submit();
                        self[0] = new Submitted(p, priority, task.sequence);
                        submitted.add(self[0]);
                        all.add(task);
                    }
                });
                assertThat(lane.running()).isEqualTo(1);
                assertThat(lane.queued()).isEqualTo(producers * each);
                gate.countDown();
                awaitAll(List.copyOf(all));
                blocker.join();
                // Everything was queued behind the blocker, so it ran strictly by priority, then submission.
                List<Submitted> expected = new ArrayList<>(submitted);
                expected.sort(Comparator.comparingInt((Submitted s) -> -s.priority())
                    .thenComparingLong(Submitted::sequence));
                assertThat(List.copyOf(ran)).containsExactlyElementsOf(expected);
                assertThat(lane.queued()).isZero();
                awaitIdle(tasks, lane);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(LaneQueue.class)
    void manyProducersWhileTasksFinishKeepTheLimitAndRunEveryTaskOnce(LaneQueue queue) throws InterruptedException {
        for (int slots : new int[] {1, 3, 8}) {
            try (TaskScheduler tasks = new TaskScheduler()) {
                Lane lane = tasks.lane(LaneSpec.platform("busy", slots).withQueue(queue));
                int producers = 8;
                int each = 2000;
                AtomicIntegerArray runs = new AtomicIntegerArray(producers * each);
                AtomicInteger concurrent = new AtomicInteger();
                AtomicInteger mostConcurrent = new AtomicInteger();
                ConcurrentLinkedQueue<Task<?>> all = new ConcurrentLinkedQueue<>();
                concurrently(producers, p -> {
                    for (int i = 0; i < each; i++) {
                        int id = p * each + i;
                        all.add(lane.task(() -> {
                            mostConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                            runs.incrementAndGet(id);
                            Thread.onSpinWait();
                            return concurrent.decrementAndGet();
                        }).priority(i % 3).submit());
                    }
                });
                awaitAll(List.copyOf(all));
                for (int id = 0; id < runs.length(); id++) {
                    assertThat(runs.get(id)).as("runs of task %d", id).isEqualTo(1);
                }
                assertThat(mostConcurrent.get()).isBetween(1, slots);
                awaitIdle(tasks, lane);
                assertThat(lane.queued()).isZero();
                assertThat(tasks.pending()).isZero();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(LaneQueue.class)
    void serialKeysKeepEachProducersOrderUnderContention(LaneQueue queue) throws InterruptedException {
        record Key(int producer) {
        }
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.lane(LaneSpec.platform("keys", 4).withQueue(queue));
            int producers = 8;
            int each = 500;
            int[][] order = new int[producers][each];
            int[] next = new int[producers];
            ConcurrentLinkedQueue<Task<?>> all = new ConcurrentLinkedQueue<>();
            concurrently(producers, p -> {
                for (int i = 0; i < each; i++) {
                    int index = i;
                    // Tasks of one key never overlap, so plain fields suffice; the lane publishes them in order.
                    all.add(lane.task(() -> order[p][next[p]++] = index).serialKey(new Key(p)).priority(-i).submit());
                }
            });
            awaitAll(List.copyOf(all));
            for (int p = 0; p < producers; p++) {
                for (int i = 0; i < each; i++) {
                    assertThat(order[p][i]).isEqualTo(i);
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(LaneQueue.class)
    void cancellingWhileProducersSubmitAndTasksFinish(LaneQueue queue) throws InterruptedException {
        for (int round = 0; round < 10; round++) {
            try (TaskScheduler tasks = new TaskScheduler()) {
                Lane lane = tasks.lane(LaneSpec.platform("cancel", 2).withQueue(queue));
                int producers = 4;
                int each = 1000;
                Task<?>[] all = new Task<?>[producers * each];
                AtomicIntegerArray runs = new AtomicIntegerArray(all.length);
                AtomicIntegerArray cancelled = new AtomicIntegerArray(all.length);
                CountDownLatch submittedHalf = new CountDownLatch(producers);
                concurrently(producers + 2, t -> {
                    if (t < producers) {
                        for (int i = 0; i < each; i++) {
                            int id = t * each + i;
                            all[id] = lane.submit(() -> {
                                runs.incrementAndGet(id);
                                return id;
                            });
                            if (i == each / 2) {
                                submittedHalf.countDown();
                            }
                        }
                    } else {
                        // Cancellers: every other task of the half submitted so far, racing the lane threads.
                        submittedHalf.await();
                        for (int id = t - producers; id < all.length; id += 2) {
                            Task<?> task = all[id];
                            if (task != null && task.cancel()) {
                                cancelled.incrementAndGet(id);
                            }
                        }
                    }
                });
                awaitAll(List.of(all));
                for (int id = 0; id < all.length; id++) {
                    Task<?> task = all[id];
                    assertThat(runs.get(id)).isLessThanOrEqualTo(1);
                    if (task.state() == TaskState.SUCCEEDED) {
                        assertThat(runs.get(id)).isEqualTo(1);
                        assertThat(task.result()).isEqualTo(id);
                        assertThat(cancelled.get(id)).isZero();
                    } else {
                        assertThat(task.state()).isEqualTo(TaskState.CANCELLED);
                        assertThat(cancelled.get(id)).isEqualTo(1);
                    }
                }
                awaitIdle(tasks, lane);
                assertThat(lane.queued()).isZero();
                assertThat(tasks.pending()).isZero();
            }
        }
    }

    // ---------------------------------------------------------------- races, replayed on one thread

    /** Runs a hook while the lane hands over a task, as if another thread acted during dispatch. */
    private static final class HookExecutor extends TaskSchedulerTest.ManualExecutor {

        Runnable hook;

        @Override
        public void execute(Runnable command) {
            super.execute(command);
            Runnable run = hook;
            hook = null;
            if (run != null) {
                run.run();
            }
        }
    }

    @Test
    void submittingDuringADispatchLeavesANoteForTheDispatcher() {
        HookExecutor executor = new HookExecutor();
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.lane("hooked", 3, executor);
            List<String> order = new ArrayList<>();
            lane.task(() -> order.add("first")).priority(2).submit();
            // While the lane hands over the second task, another "thread" submits two more: the dispatcher goes
            // round once more and starts the better one in the last slot.
            executor.hook = () -> {
                lane.submit(() -> order.add("fourth"));
                lane.task(() -> order.add("third")).priority(1).submit();
            };
            lane.submit(() -> order.add("second"));
            assertThat(lane.running()).isEqualTo(3);
            assertThat(lane.queued()).isEqualTo(1);
            executor.runAll();
            assertThat(order).containsExactly("first", "second", "third", "fourth");
            assertThat(tasks.pending()).isZero();
        }
    }

    @Test
    void aTaskCancelledWhileTheDispatcherHandsOutOthersIsSkipped() {
        HookExecutor executor = new HookExecutor();
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.lane("hooked", 2, executor);
            Task<String> a = lane.submit(() -> "a");
            Task<String> b = lane.submit(() -> "b");
            Task<String> later = lane.submit(() -> "later");
            Task<String> first = lane.task(() -> "first").priority(1).submit();
            assertThat(lane.queued()).isEqualTo(2);
            // a ends; while the dispatcher hands out the first task, b ends too and the later task is cancelled.
            // With a slot free again, the dispatcher polls the later task: it must skip it, not start it.
            executor.hook = () -> {
                executor.runNext();
                assertThat(later.cancel()).isTrue();
            };
            executor.runNext();
            executor.runAll();
            assertThat(a.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(b.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(first.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(later.state()).isEqualTo(TaskState.CANCELLED);
            assertThat(lane.queued()).isZero();
            awaitIdle(tasks, lane);
        }
    }

    @Test
    void cancellingAQueuedTaskLetsGoOfItAtOnce() {
        TaskSchedulerTest.ManualExecutor executor = new TaskSchedulerTest.ManualExecutor();
        try (TaskScheduler tasks = new TaskScheduler()) {
            Lane lane = tasks.lane("manual", 1, executor);
            lane.submit(() -> "busy");
            Task<String> cancelled = lane.submit(() -> "cancelled");
            Task<String> kept = lane.submit(() -> "kept");
            assertThat(cancelled.cancel()).isTrue();
            assertThat(lane.queued()).isEqualTo(1);
            executor.runAll();
            assertThat(kept.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(cancelled.state()).isEqualTo(TaskState.CANCELLED);
        }
    }
}
