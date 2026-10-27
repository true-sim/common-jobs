package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/**
 * A piece of work on a {@link Lane}: it starts once the tasks it depends on have succeeded and the lane has a free
 * slot, and ends {@link TaskState#SUCCEEDED succeeded}, {@link TaskState#FAILED failed} or
 * {@link TaskState#CANCELLED cancelled}. Thread-safe.
 *
 * @param <T> the result type; {@code Void} for tasks without a result
 */
@AllowAllocation("tasks are for I/O and background work, not for hot loops")
public final class Task<T> {

    final Lane lane;
    final long sequence;
    final int priority;
    private final Callable<T> work;
    private final Duration timeout;
    private final Consumer<? super Task<T>> handler;
    private final Object serialKey;

    private TaskState state = TaskState.WAITING;
    private T result;
    private Throwable failure;
    /** Unfinished prerequisites, plus one until submission has registered them all. */
    private int waiting = 1;
    private final List<Task<?>> dependents = new ArrayList<>();
    private final List<Task<?>> followers = new ArrayList<>();
    private Thread runner;
    private boolean cancelRequested;

    Task(Lane lane, long sequence, int priority, Callable<T> work, Duration timeout, Consumer<? super Task<T>> handler,
        Object serialKey) {
        this.lane = lane;
        this.sequence = sequence;
        this.priority = priority;
        this.work = work;
        this.timeout = timeout;
        this.handler = handler;
        this.serialKey = serialKey;
    }

    public synchronized TaskState state() {
        return state;
    }

    public synchronized boolean isDone() {
        return state.isDone();
    }

    /** The lane the task runs on. */
    public Lane lane() {
        return lane;
    }

    /**
     * The result of a task that succeeded.
     *
     * @throws TaskFailedException if it failed or was cancelled, with the reason as cause
     * @throws IllegalStateException if it has not finished yet
     */
    public synchronized T result() {
        return switch (state) {
            case SUCCEEDED -> result;
            case FAILED, CANCELLED -> throw new TaskFailedException(this + " did not succeed", failure);
            default -> throw new IllegalStateException(this + " has not finished");
        };
    }

    /** Why the task failed or was cancelled; {@code null} otherwise. */
    public synchronized Throwable failure() {
        return failure;
    }

    /** Waits until the task has finished. */
    public synchronized void await() throws InterruptedException {
        while (!state.isDone()) {
            wait();
        }
    }

    /** Waits at most {@code timeout} for the task to finish; returns whether it has. */
    public synchronized boolean await(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (long left = timeout.toNanos(); !state.isDone() && left > 0; left = deadline - System.nanoTime()) {
            wait(Math.max(1, left / 1_000_000));
        }
        return state.isDone();
    }

    /** Waits until the task has finished and returns its result, like {@link #result()}. */
    public T join() throws InterruptedException {
        await();
        return result();
    }

    /**
     * Cancels the task: one that has not started never will; a running one is interrupted and ends cancelled when its
     * work returns. Tasks that depend on it fail. Returns {@code false} if the task had already finished.
     */
    public boolean cancel() {
        Ended ended;
        synchronized (this) {
            if (state.isDone()) {
                return false;
            }
            cancelRequested = true;
            if (state == TaskState.RUNNING || state == TaskState.QUEUED && !lane.remove(this)) {
                // Running, or handed to a lane thread that has not started it: it ends when that thread gets to it.
                if (runner != null) {
                    runner.interrupt();
                }
                return true;
            }
            ended = end(TaskState.CANCELLED, null, cancellation());
        }
        announce(ended);
        return true;
    }

    private CancellationException cancellation() {
        return new CancellationException(this + " was cancelled");
    }

    // ---------------------------------------------------------------- scheduling

    /**
     * Registers {@code next} to hear when this task ends. A dependent fails if this task does not succeed; a follower
     * (same serial key) only waits for it. Returns {@code false} if this task has already ended.
     */
    synchronized boolean addNext(Task<?> next, boolean dependent) {
        if (state.isDone()) {
            return false;
        }
        (dependent ? dependents : followers).add(next);
        return true;
    }

    synchronized void addPrerequisite() {
        waiting++;
    }

    /** One prerequisite has ended, or with {@code null} submission is complete. */
    void prerequisiteEnded(Task<?> prerequisite, boolean dependent) {
        Ended ended;
        synchronized (this) {
            if (state != TaskState.WAITING) {
                return; // already failed or cancelled
            }
            if (prerequisite == null || !dependent || prerequisite.state() == TaskState.SUCCEEDED) {
                if (--waiting == 0) {
                    state = TaskState.QUEUED;
                    lane.enqueue(this);
                }
                return;
            }
            ended = end(TaskState.FAILED, null,
                new TaskFailedException(prerequisite + " did not succeed", prerequisite.failure()));
        }
        announce(ended);
    }

    /** Runs the work on the lane thread it was handed to. */
    void run() {
        boolean skip;
        synchronized (this) {
            skip = cancelRequested; // cancelled after the lane handed it over
            state = TaskState.RUNNING;
            runner = Thread.currentThread();
        }
        if (timeout != null) {
            lane.scheduler.cancelAfter(this, timeout);
        }
        T value = null;
        Throwable thrown = null;
        if (!skip) {
            try {
                value = work.call();
            } catch (Throwable t) {
                thrown = t;
            }
        }
        Ended ended;
        synchronized (this) {
            runner = null;
            Thread.interrupted(); // a cancel may have interrupted this pooled thread
            ended = cancelRequested ? end(TaskState.CANCELLED, null, cancellation())
                : end(thrown == null ? TaskState.SUCCEEDED : TaskState.FAILED, value, thrown);
        }
        lane.finished();
        announce(ended);
    }

    /** The tasks to tell after a task ended. */
    private record Ended(List<Task<?>> dependents, List<Task<?>> followers) {
    }

    /** Ends the task; the caller holds the lock and has checked the task has not ended yet. */
    private Ended end(TaskState end, T value, Throwable cause) {
        state = end;
        result = value;
        failure = cause;
        Ended ended = new Ended(List.copyOf(dependents), List.copyOf(followers));
        dependents.clear();
        followers.clear();
        notifyAll();
        return ended;
    }

    /** Tells waiting tasks and the scheduler that the task ended; called without the lock. */
    private void announce(Ended ended) {
        if (serialKey != null) {
            lane.forget(serialKey, this); // unless a later task with the key took over
        }
        for (Task<?> task : ended.dependents()) {
            task.prerequisiteEnded(this, true);
        }
        for (Task<?> task : ended.followers()) {
            task.prerequisiteEnded(this, false);
        }
        lane.scheduler.ended(this, handler != null);
    }

    /** Runs the hand-over handler; called by {@link TaskScheduler#drainCompleted()} on its thread. */
    void deliver() {
        handler.accept(this);
    }

    @Override
    public String toString() {
        return "Task " + sequence + " on " + lane.name();
    }
}
