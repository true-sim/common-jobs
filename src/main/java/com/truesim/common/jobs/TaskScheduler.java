package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Engine work that is not data-parallel compute: downloads, file access, loading, background tasks. Work runs as
 * {@link Task}s on {@link Lane}s, each with its own threads and limits; tasks can depend on each other, and finished
 * tasks are handed back to the main thread at a point of its choosing.
 *
 * <pre>{@code
 * // Lane definitions can live anywhere, e.g. static final fields: see LaneSpec.
 * static final LaneSpec DOWNLOADS = LaneSpec.virtual("downloads", 1); // one download at a time
 * static final LaneSpec FILES = LaneSpec.serial("files");              // one file access at a time, in order
 * static final LaneSpec DECODE = LaneSpec.platform("decode", 4);
 *
 * try (TaskScheduler tasks = new TaskScheduler()) {
 *     Lane downloads = tasks.lane(DOWNLOADS);
 *     Lane files = tasks.lane(FILES);
 *     Lane decode = tasks.lane(DECODE);
 *
 *     Task<byte[]> bytes = downloads.submit(() -> fetch(url));
 *     files.task(() -> write(slot)).serialKey(new SaveSlot(1)).submit(); // typed key: one write per slot at a time
 *     decode.task(() -> decodePng(bytes.result()))
 *         .after(bytes)
 *         .deliver(done -> textures.add(done.result()))
 *         .submit();
 *
 *     // Once per frame, on the main thread:
 *     tasks.drainCompleted();
 * }
 * }</pre>
 *
 * <p>The simulation stays deterministic when it only sees task results through {@link #drainCompleted()} at a fixed
 * point of the frame: handlers run there, in submission order, never in between. For data-parallel work inside a
 * task, use a {@link JobSystem} from a lane with one thread, as a job system takes one submitter at a time.
 */
@AllowAllocation("tasks are for I/O and background work, not for hot loops")
public final class TaskScheduler implements AutoCloseable {

    private final AtomicLong sequence = new AtomicLong();
    private final List<Lane> lanes = new ArrayList<>();
    private final Map<LaneSpec, Lane> bySpec = new IdentityHashMap<>();
    private final Set<Task<?>> live = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Task<?>> completed = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().daemon().name("truesim-tasks-timer").factory());
    private volatile boolean closed;

    /**
     * This scheduler's lane for {@code spec}: created on first use, the same lane on every later call. Lanes made
     * from one spec by different schedulers are separate, each with its own threads.
     */
    public synchronized Lane lane(LaneSpec spec) {
        Lane lane = bySpec.get(spec);
        if (lane == null) {
            lane = spec.isVirtual() ? virtualLane(spec.name(), spec.maxConcurrent())
                : platformLane(spec.name(), spec.maxConcurrent());
            bySpec.put(spec, lane);
        }
        return lane;
    }

    /** A lane on virtual threads, for I/O: at most {@code maxConcurrent} of its tasks run at once. */
    public Lane virtualLane(String name, int maxConcurrent) {
        return lane(name, maxConcurrent, Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(name + "-", 0).factory()));
    }

    /** A lane on {@code threads} platform threads, for CPU-bound background work. */
    public Lane platformLane(String name, int threads) {
        return lane(name, threads,
            Executors.newFixedThreadPool(threads, Thread.ofPlatform().daemon().name(name + "-", 0).factory()));
    }

    /** A lane that runs one task at a time, in order of priority, then submission, on a virtual thread. */
    public Lane serialLane(String name) {
        return virtualLane(name, 1);
    }

    /** A lane on any executor, e.g. one that runs tasks when a test says so. */
    synchronized Lane lane(String name, int maxConcurrent, ExecutorService executor) {
        checkOpen();
        Lane lane = new Lane(this, name, maxConcurrent, executor);
        lanes.add(lane);
        return lane;
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("The task scheduler is closed");
        }
    }

    <T> Task<T> submit(Lane lane, Callable<T> work, List<Task<?>> after, int priority, Object serialKey,
        Duration timeout, Consumer<? super Task<T>> handler) {
        checkOpen();
        Task<T> task = new Task<>(lane, sequence.incrementAndGet(), priority, work, timeout, handler, serialKey);
        live.add(task);
        for (Task<?> prerequisite : after) {
            register(prerequisite, task, true);
        }
        if (serialKey != null) {
            Task<?> previous = lane.exchangeLast(serialKey, task);
            if (previous != null) {
                register(previous, task, false);
            }
        }
        task.prerequisiteEnded(null, false); // all registered: it may start now
        return task;
    }

    private static void register(Task<?> prerequisite, Task<?> task, boolean dependent) {
        task.addPrerequisite();
        if (!prerequisite.addNext(task, dependent)) {
            task.prerequisiteEnded(prerequisite, dependent); // already ended
        }
    }

    void cancelAfter(Task<?> task, Duration timeout) {
        timer.schedule(task::cancel, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    void ended(Task<?> task, boolean deliver) {
        live.remove(task);
        if (deliver) {
            completed.add(task);
        }
    }

    /**
     * Runs the hand-over handlers of tasks that finished since the last call, on this thread, in submission order.
     * Returns how many ran.
     */
    public int drainCompleted() {
        List<Task<?>> done = new ArrayList<>();
        for (Task<?> task = completed.poll(); task != null; task = completed.poll()) {
            done.add(task);
        }
        done.sort(Comparator.comparingLong(task -> task.sequence));
        for (Task<?> task : done) {
            task.deliver();
        }
        return done.size();
    }

    /** Number of submitted tasks that have not finished. */
    public int pending() {
        return live.size();
    }

    /** Cancels every unfinished task and stops all lanes. */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        for (Task<?> task : List.copyOf(live)) {
            task.cancel();
        }
        for (Lane lane : lanes) {
            lane.close();
        }
        timer.shutdownNow();
    }
}
