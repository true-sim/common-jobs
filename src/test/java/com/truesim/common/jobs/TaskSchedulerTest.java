package com.truesim.common.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TaskSchedulerTest {

    /** Holds the tasks a lane hands over until the test runs them: fully deterministic scheduling. */
    static final class ManualExecutor extends AbstractExecutorService {

        final Queue<Runnable> handed = new ArrayDeque<>();
        boolean shutdown;

        @Override
        public void execute(Runnable command) {
            handed.add(command);
        }

        /** Runs the oldest handed-over task on this thread. */
        void runNext() {
            handed.remove().run();
        }

        void runAll() {
            while (!handed.isEmpty()) {
                runNext();
            }
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private final TaskScheduler tasks = new TaskScheduler();
    private final ManualExecutor executor = new ManualExecutor();

    // ---------------------------------------------------------------- lanes

    @Test
    void lanesLimitConcurrencyAndOrderByPriority() {
        Lane lane = tasks.lane("manual", 2, executor);
        assertThat(lane.name()).isEqualTo("manual");
        assertThat(lane.maxConcurrent()).isEqualTo(2);
        assertThat(lane.toString()).isEqualTo("Lane manual");
        List<String> order = new ArrayList<>();
        lane.submit(() -> order.add("a"));
        lane.submit(() -> order.add("b"));
        lane.task(() -> order.add("low")).priority(-1).submit();
        lane.submit(() -> order.add("c"));
        lane.task(() -> order.add("high")).priority(5).submit();
        // Two slots: a and b were handed over at once, the rest wait in priority order.
        assertThat(lane.running()).isEqualTo(2);
        assertThat(lane.queued()).isEqualTo(3);
        assertThat(executor.handed).hasSize(2);
        executor.runAll();
        assertThat(order).containsExactly("a", "b", "high", "c", "low");
        assertThat(lane.running()).isZero();
        assertThat(tasks.pending()).isZero();
        assertThatIllegalArgumentException().isThrownBy(() -> tasks.lane("none", 0, executor));
    }

    @Test
    void serialKeysRunOneAtATimeAndIgnoreFailures() throws InterruptedException {
        Lane lane = tasks.lane("files", 4, executor);
        List<String> writes = Collections.synchronizedList(new ArrayList<>());
        Task<Void> first = lane.<Void>task(() -> {
            writes.add("save-1");
            throw new IOException("disk full");
        }).serialKey("save").submit();
        Task<Boolean> second = lane.task(() -> writes.add("save-2")).serialKey("save").submit();
        Task<Boolean> other = lane.task(() -> writes.add("config")).serialKey("config").submit();
        // The second save waits for the first; other keys do not.
        assertThat(second.state()).isEqualTo(TaskState.WAITING);
        assertThat(executor.handed).hasSize(2);
        executor.runAll();
        assertThat(writes).containsExactly("save-1", "config", "save-2");
        assertThat(first.state()).isEqualTo(TaskState.FAILED);
        assertThat(first.failure()).isInstanceOf(IOException.class);
        assertThat(second.join()).isTrue(); // the failed earlier write does not fail the next one
        assertThat(other.state()).isEqualTo(TaskState.SUCCEEDED);
        // A key whose last task has ended starts fresh.
        Task<Boolean> third = lane.task(() -> writes.add("save-3")).serialKey("save").submit();
        assertThat(third.state()).isEqualTo(TaskState.QUEUED);
        executor.runAll();
        assertThat(writes).endsWith("save-3");
    }

    @Test
    void realLanes() throws InterruptedException {
        Lane downloads = tasks.virtualLane("downloads", 1);
        Lane decode = tasks.platformLane("decode", 3);
        Lane files = tasks.serialLane("files");
        assertThat(files.maxConcurrent()).isEqualTo(1);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        List<Task<Integer>> fetched = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            int n = i;
            fetched.add(downloads.submit(() -> {
                maximum.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                Thread.sleep(5);
                concurrent.decrementAndGet();
                assertThat(Thread.currentThread().isVirtual()).isTrue();
                return n;
            }));
        }
        Task<Integer> sum = decode.submit(() -> {
            assertThat(Thread.currentThread().isVirtual()).isFalse();
            int total = 0;
            for (Task<Integer> task : fetched) {
                total += task.result();
            }
            return total;
        }, fetched.toArray(Task<?>[]::new));
        Task<Void> stored = files.execute(() -> assertThat(sum.result()).isEqualTo(15), sum);
        stored.await();
        assertThat(stored.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(stored.result()).isNull();
        assertThat(maximum.get()).isEqualTo(1); // one download at a time
    }

    @Test
    void mixedWithTheJobSystem() throws InterruptedException {
        // A lane with one platform thread is the single submitter a job system needs.
        try (JobSystem jobs = new JobSystem(4)) {
            Lane compute = tasks.platformLane("compute", 1);
            long[] squares = new long[1000];
            Task<Long> total = compute.submit(() -> {
                jobs.parallelFor(squares.length, 64, (from, to, worker) -> {
                    for (int i = from; i < to; i++) {
                        squares[i] = (long) i * i;
                    }
                });
                long sum = 0;
                for (long square : squares) {
                    sum += square;
                }
                return sum;
            });
            assertThat(total.join()).isEqualTo(332_833_500L);
        }
    }

    // ---------------------------------------------------------------- dependencies

    @Test
    void dependenciesRunFirst() throws InterruptedException {
        Lane lane = tasks.lane("manual", 4, executor);
        Task<Integer> one = lane.submit(() -> 1);
        Task<Integer> three = lane.submit(() -> 3);
        Task<Integer> five = lane.submit(() -> 5);
        Task<Integer> two = lane.submit(() -> one.result() + three.result() + five.result(), one, three, five);
        assertThat(two.state()).isEqualTo(TaskState.WAITING);
        executor.runNext();
        executor.runNext();
        assertThat(two.state()).isEqualTo(TaskState.WAITING);
        executor.runAll();
        assertThat(two.join()).isEqualTo(9);
        // Depending on tasks that have already ended.
        Task<Integer> after = lane.submit(() -> two.result() * 2, two);
        executor.runAll();
        assertThat(after.result()).isEqualTo(18);
    }

    @Test
    void failuresPropagateToDependents() {
        Lane lane = tasks.lane("manual", 4, executor);
        Task<Integer> broken = lane.submit(() -> {
            throw new IllegalStateException("corrupt file");
        });
        Task<Integer> slow = lane.submit(() -> 2);
        Task<Integer> dependent = lane.submit(() -> 1, broken, slow);
        Task<Integer> chained = lane.submit(() -> 2, dependent);
        executor.runNext(); // broken fails: dependent and chained fail without running
        assertThat(dependent.state()).isEqualTo(TaskState.FAILED);
        assertThat(dependent.failure()).isInstanceOf(TaskFailedException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(chained.state()).isEqualTo(TaskState.FAILED);
        assertThatThrownBy(chained::result).isInstanceOf(TaskFailedException.class)
            .hasMessage(chained + " did not succeed");
        executor.runAll(); // slow ends later: the failed dependent stays failed
        assertThat(dependent.state()).isEqualTo(TaskState.FAILED);
        // Depending on a task that has already failed.
        Task<Integer> late = lane.submit(() -> 3, broken);
        assertThat(late.state()).isEqualTo(TaskState.FAILED);
        assertThat(executor.handed).isEmpty();
    }

    // ---------------------------------------------------------------- cancelling

    @Test
    void cancelling() throws InterruptedException {
        Lane lane = tasks.lane("manual", 1, executor);
        Task<Integer> gate = lane.submit(() -> 1);
        Task<Integer> queued = lane.submit(() -> 2);
        Task<Integer> waiting = lane.submit(() -> 3, queued);
        Task<Integer> dependent = lane.submit(() -> 4, waiting);
        // Waiting for a prerequisite: cancelled at once, and its dependent fails.
        assertThat(waiting.cancel()).isTrue();
        assertThat(waiting.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(waiting.failure()).isInstanceOf(CancellationException.class);
        assertThat(dependent.state()).isEqualTo(TaskState.FAILED);
        // Queued behind the gate: taken out of the lane.
        assertThat(queued.cancel()).isTrue();
        assertThat(lane.queued()).isZero();
        assertThatThrownBy(queued::result).isInstanceOf(TaskFailedException.class)
            .hasCauseInstanceOf(CancellationException.class);
        // Handed to a thread that has not started it yet: it ends cancelled when the thread gets to it.
        assertThat(gate.state()).isEqualTo(TaskState.QUEUED);
        assertThat(gate.cancel()).isTrue();
        assertThat(gate.isDone()).isFalse();
        executor.runNext();
        assertThat(gate.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(lane.running()).isZero();
        // Finished tasks cannot be cancelled.
        Task<Integer> done = lane.submit(() -> 5);
        executor.runAll();
        assertThat(done.cancel()).isFalse();
        assertThat(done.join()).isEqualTo(5);
    }

    @Test
    void cancellingRunningTasksAndTimeouts() throws InterruptedException {
        Lane lane = tasks.virtualLane("io", 2);
        CountDownLatch started = new CountDownLatch(1);
        Task<Integer> running = lane.submit(() -> {
            started.countDown();
            Thread.sleep(60_000);
            return 1;
        });
        started.await();
        assertThat(running.state()).isEqualTo(TaskState.RUNNING);
        assertThat(running.cancel()).isTrue(); // interrupts the sleep
        running.await();
        assertThat(running.state()).isEqualTo(TaskState.CANCELLED);
        // A timeout cancels a task that runs too long; a quick one is not affected.
        Task<Integer> hanging = lane.task(() -> {
            Thread.sleep(60_000);
            return 1;
        }).timeout(Duration.ofMillis(30)).submit();
        Task<Integer> quick = lane.task(() -> 2).timeout(Duration.ofSeconds(10)).submit();
        assertThat(hanging.await(Duration.ofSeconds(10))).isTrue();
        assertThat(hanging.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(quick.join()).isEqualTo(2);
    }

    // ---------------------------------------------------------------- hand-over

    @Test
    void finishedTasksAreHandedOverInSubmissionOrder() {
        Lane lane = tasks.lane("manual", 3, executor);
        List<String> delivered = new ArrayList<>();
        Thread main = Thread.currentThread();
        Task<String> a = lane.task(() -> "a").deliver(task -> delivered.add(task.result())).submit();
        Task<String> b = lane.<String>task(() -> {
            throw new IOException("offline");
        }).deliver(task -> delivered.add(task.state().name())).submit();
        lane.task(() -> "c").deliver(task -> {
            assertThat(Thread.currentThread()).isSameAs(main);
            delivered.add(task.result());
        }).submit();
        lane.submit(() -> "not delivered");
        // Finish in reverse order: handed over in submission order anyway, and only when drained.
        List<Runnable> handed = new ArrayList<>(executor.handed);
        executor.handed.clear();
        for (int i = handed.size() - 1; i >= 0; i--) {
            handed.get(i).run();
        }
        executor.runAll();
        assertThat(delivered).isEmpty();
        assertThat(tasks.drainCompleted()).isEqualTo(3);
        assertThat(delivered).containsExactly("a", "FAILED", "c");
        assertThat(tasks.drainCompleted()).isZero();
        assertThat(a.isDone()).isTrue();
        assertThat(b.lane()).isSameAs(lane);
    }

    // ---------------------------------------------------------------- states and closing

    @Test
    void statesAndWaiting() throws InterruptedException {
        Lane lane = tasks.lane("manual", 1, executor);
        Task<Integer> task = lane.submit(() -> 7);
        assertThatIllegalStateException().isThrownBy(task::result).withMessage(task + " has not finished");
        assertThat(task.await(Duration.ofMillis(5))).isFalse();
        assertThat(task.await(Duration.ZERO)).isFalse();
        assertThat(task.toString()).isEqualTo("Task " + 1 + " on manual");
        executor.runAll();
        assertThat(task.await(Duration.ofMillis(5))).isTrue();
        assertThat(task.failure()).isNull();
        for (TaskState state : TaskState.values()) {
            assertThat(state.isDone()).isEqualTo(
                state == TaskState.SUCCEEDED || state == TaskState.FAILED || state == TaskState.CANCELLED);
        }
    }

    @Test
    void builderValidates() {
        Lane lane = tasks.lane("manual", 1, executor);
        assertThatNullPointerException().isThrownBy(() -> lane.task(null));
        TaskBuilder<Integer> builder = lane.task(() -> 1);
        assertThatNullPointerException().isThrownBy(() -> builder.serialKey(null));
        assertThatNullPointerException().isThrownBy(() -> builder.timeout(null));
        assertThatNullPointerException().isThrownBy(() -> builder.deliver(null));
    }

    @Test
    void closingCancelsUnfinishedTasks() {
        Lane lane = tasks.lane("manual", 1, executor);
        Task<Integer> handedOver = lane.submit(() -> 1);
        Task<Integer> queued = lane.submit(() -> 2);
        Task<Integer> waiting = lane.submit(() -> 3, queued);
        tasks.close();
        assertThat(queued.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(waiting.state()).isIn(TaskState.CANCELLED, TaskState.FAILED);
        assertThat(executor.isShutdown()).isTrue();
        executor.runAll(); // the handed-over task ends cancelled
        assertThat(handedOver.state()).isEqualTo(TaskState.CANCELLED);
        tasks.close(); // twice is fine
        assertThatIllegalStateException().isThrownBy(() -> lane.submit(() -> 4));
        assertThatIllegalStateException().isThrownBy(() -> tasks.virtualLane("late", 1));
        assertThat(tasks.pending()).isZero();
    }
}
