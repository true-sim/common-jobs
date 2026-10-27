package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * A queue of tasks with its own threads and rules: at most {@link #maxConcurrent()} of its tasks run at once, the
 * ready ones in order of priority, then submission. Created by a {@link TaskScheduler}: virtual threads for I/O,
 * platform threads for CPU-bound background work, or a serial lane that runs one task at a time in order.
 */
@AllowAllocation("tasks are for I/O and background work, not for hot loops")
public final class Lane {

    private static final Comparator<Task<?>> ORDER =
        Comparator.<Task<?>>comparingInt(task -> -task.priority).thenComparingLong(task -> task.sequence);

    final TaskScheduler scheduler;
    private final String name;
    private final int maxConcurrent;
    private final ExecutorService executor;
    private final PriorityQueue<Task<?>> ready = new PriorityQueue<>(ORDER);
    private final Map<Object, Task<?>> lastByKey = new HashMap<>();
    private int running;

    Lane(TaskScheduler scheduler, String name, int maxConcurrent, ExecutorService executor) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("A lane needs at least one slot: " + maxConcurrent);
        }
        this.scheduler = scheduler;
        this.name = name;
        this.maxConcurrent = maxConcurrent;
        this.executor = executor;
    }

    public String name() {
        return name;
    }

    /** How many of this lane's tasks may run at once. */
    public int maxConcurrent() {
        return maxConcurrent;
    }

    /** Tasks running now (or handed to a thread). */
    public synchronized int running() {
        return running;
    }

    /** Tasks ready to run, waiting for a free slot. */
    public synchronized int queued() {
        return ready.size();
    }

    /** Starts describing a task: dependencies, priority, serial key, timeout, hand-over. */
    public <T> TaskBuilder<T> task(Callable<T> work) {
        return new TaskBuilder<>(this, work);
    }

    /** Submits a task that runs once every task in {@code after} has succeeded. */
    public <T> Task<T> submit(Callable<T> work, Task<?>... after) {
        return task(work).after(after).submit();
    }

    /** Submits a task without a result that runs once every task in {@code after} has succeeded. */
    public Task<Void> execute(Runnable work, Task<?>... after) {
        return submit(() -> {
            work.run();
            return null;
        }, after);
    }

    /** The last task submitted with {@code key}, which the next one with that key waits for; remembers {@code next}. */
    synchronized Task<?> exchangeLast(Object key, Task<?> next) {
        return lastByKey.put(key, next);
    }

    synchronized void forget(Object key, Task<?> task) {
        lastByKey.remove(key, task);
    }

    synchronized void enqueue(Task<?> task) {
        ready.add(task);
        dispatch();
    }

    /** Takes a task out of the ready queue; {@code false} if it was already handed to a thread. */
    synchronized boolean remove(Task<?> task) {
        return ready.remove(task);
    }

    synchronized void finished() {
        running--;
        dispatch();
    }

    private void dispatch() {
        while (running < maxConcurrent && !ready.isEmpty()) {
            Task<?> task = ready.poll();
            running++;
            executor.execute(task::run);
        }
    }

    void close() {
        executor.shutdownNow();
    }

    @Override
    public String toString() {
        return "Lane " + name;
    }
}
