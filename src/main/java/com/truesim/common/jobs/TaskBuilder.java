package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * Describes a task before submitting it: {@code lane.task(work).after(a, b).priority(5).submit()}.
 *
 * @param <T> the result type
 */
@AllowAllocation("tasks are for I/O and background work, not for hot loops")
public final class TaskBuilder<T> {

    private final Lane lane;
    private final Callable<T> work;
    private final List<Task<?>> after = new ArrayList<>();
    private int priority;
    private Object serialKey;
    private Duration timeout;
    private Consumer<? super Task<T>> handler;

    TaskBuilder(Lane lane, Callable<T> work) {
        this.lane = lane;
        this.work = Objects.requireNonNull(work, "work");
    }

    /** Runs only after these tasks have succeeded; if one fails or is cancelled, this task fails. */
    public TaskBuilder<T> after(Task<?>... tasks) {
        after.addAll(List.of(tasks));
        return this;
    }

    /** Among ready tasks of the lane, higher priorities start first; default 0. */
    public TaskBuilder<T> priority(int priority) {
        this.priority = priority;
        return this;
    }

    /**
     * Runs after the previous task submitted to this lane with an equal key has ended, whatever its outcome: tasks
     * with one key run one at a time in submission order, e.g. writes to one file.
     *
     * <p>Keys are compared with {@code equals}. Prefer typed keys over strings, so a typo is a compile error and keys
     * of different kinds cannot collide: a record such as {@code record SaveSlot(int index)}, an enum constant, or a
     * {@link java.nio.file.Path}. Keys must not change while in use.
     */
    public TaskBuilder<T> serialKey(Object key) {
        this.serialKey = Objects.requireNonNull(key, "key");
        return this;
    }

    /** Cancels the task if it runs longer than {@code timeout}. */
    public TaskBuilder<T> timeout(Duration timeout) {
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /**
     * Hands the finished task (whatever its outcome) to {@code handler} on the thread that calls
     * {@link TaskScheduler#drainCompleted()}, e.g. the main thread once per frame.
     */
    public TaskBuilder<T> deliver(Consumer<? super Task<T>> handler) {
        this.handler = Objects.requireNonNull(handler, "handler");
        return this;
    }

    public Task<T> submit() {
        return lane.scheduler.submit(lane, work, after, priority, serialKey, timeout, handler);
    }
}
