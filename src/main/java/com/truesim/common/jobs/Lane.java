package com.truesim.common.jobs;

import com.truesim.annotations.AllowAllocation;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * A queue of tasks with its own threads and rules: at most {@link #maxConcurrent()} of its tasks run at once, the
 * ready ones in order of priority, then submission. Created by a {@link TaskScheduler}: virtual threads for I/O,
 * platform threads for CPU-bound background work, or a serial lane that runs one task at a time in order.
 *
 * <p>Lock-free by default ({@link LaneQueue#LOCK_FREE}): a task that becomes ready joins an inbox with one atomic swap,
 * so submitting never waits for threads that finish tasks. Whichever thread then finds a free slot becomes the lane's
 * only dispatcher for a moment: it moves the inbox into the priority queue and hands out the best tasks while slots are
 * free. Other threads that want to dispatch meanwhile only leave a note, and the dispatcher goes round once more before
 * it stops, so no ready task is left behind with a free slot. The order holds among the tasks that are ready when a
 * slot is filled. {@link LaneQueue#LOCKED} takes one lock instead.
 */
@AllowAllocation("tasks are for I/O and background work, not for hot loops")
public final class Lane {

    /** How long a lane thread that ended a task waits for another thread's dispatch, in spin-wait hints. */
    private static final int FINISH_SPINS = 200;

    /** Higher priorities first, then earlier submissions. */
    private static final Comparator<Task<?>> ORDER = (a, b) -> a.priority != b.priority
        ? Integer.compare(b.priority, a.priority) : Long.compare(a.sequence, b.sequence);

    final TaskScheduler scheduler;
    private final String name;
    private final int maxConcurrent;
    private final ExecutorService executor;
    /** {@link LaneQueue#LOCKED}: submitting, finishing and cancelling take the lane's lock instead. */
    private final boolean locked;
    private final ConcurrentHashMap<Object, Task<?>> lastByKey = new ConcurrentHashMap<>();
    /**
     * Tasks that became ready since the last dispatch, oldest first: a queue of many producers and one consumer (the
     * dispatcher). {@code head} is the node consumed last, {@code tail} the node added last.
     */
    private Node head = new Node(null);
    private final AtomicReference<Node> tail = new AtomicReference<>(head);
    /** Requests to dispatch; the thread that raises it from 0 dispatches until it drops back to 0. */
    private final AtomicInteger dispatches = new AtomicInteger();
    private final AtomicInteger running = new AtomicInteger();
    private final LongAdder queued = new LongAdder();
    /** Set when a queued task was cancelled, so the dispatcher drops cancelled tasks from {@link #ready}. */
    private final AtomicBoolean purge = new AtomicBoolean();
    /** Ready tasks by priority, then submission; only touched by the dispatching thread (or under the lock). */
    private final PriorityQueue<Task<?>> ready = new PriorityQueue<>(ORDER);

    Lane(TaskScheduler scheduler, String name, int maxConcurrent, ExecutorService executor, LaneQueue queue) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("A lane needs at least one slot: " + maxConcurrent);
        }
        this.scheduler = scheduler;
        this.name = name;
        this.maxConcurrent = maxConcurrent;
        this.executor = executor;
        this.locked = queue == LaneQueue.LOCKED;
    }

    public String name() {
        return name;
    }

    /** How the lane's ready queue handles threads that submit and finish tasks at the same time. */
    public LaneQueue queue() {
        return locked ? LaneQueue.LOCKED : LaneQueue.LOCK_FREE;
    }

    /** How many of this lane's tasks may run at once. */
    public int maxConcurrent() {
        return maxConcurrent;
    }

    /** Tasks running now (or handed to a thread). */
    public int running() {
        return running.get();
    }

    /** Tasks ready to run, waiting for a free slot. */
    public int queued() {
        return queued.intValue();
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
    Task<?> exchangeLast(Object key, Task<?> next) {
        return lastByKey.put(key, next);
    }

    void forget(Object key, Task<?> task) {
        lastByKey.remove(key, task);
    }

    /** A task became ready: onto the inbox, and handed out at once if a slot is free. */
    void enqueue(Task<?> task) {
        if (locked) {
            enqueueLocked(task);
            return;
        }
        queued.increment();
        Node node = new Node(task);
        // Wait-free: claim the tail, then link the old tail to the node. Until it is linked, the dispatcher sees the
        // inbox end before the node; linking comes before reading running below, so either a slot freed afterwards
        // dispatches and finds the node, or this thread sees the free slot and dispatches itself.
        tail.getAndSet(node).next = node;
        if (running.get() < maxConcurrent) {
            dispatch();
        }
    }

    /** Takes a task out of the ready queue; {@code false} if it was already handed to a thread. */
    boolean remove(Task<?> task) {
        if (locked) {
            return removeLocked(task);
        }
        if (!task.claim()) {
            return false;
        }
        queued.decrement();
        purge.set(true); // let go of it now, not when its turn would have come
        dispatch();
        return true;
    }

    /**
     * A task ended and its slot is free. If another thread is dispatching, the lane thread waits a moment for it: the
     * dispatcher hands out a task for this slot, and a lane thread that returns to its executor before that parks and
     * has to be woken (measured: a backlog drains about 1.7 times faster with the wait, see LaneBenchmark).
     */
    void finished() {
        if (locked) {
            finishedLocked();
            return;
        }
        running.decrementAndGet();
        dispatch();
        for (int spins = 0; spins < FINISH_SPINS && dispatches.get() != 0; spins++) {
            Thread.onSpinWait();
        }
    }

    /** Becomes the dispatcher, or leaves a note for the current one to go round once more. */
    private void dispatch() {
        if (dispatches.getAndIncrement() != 0) {
            return;
        }
        int missed = 1;
        do {
            boolean purging = purge.get(); // before the inbox: a cancel from here on leaves another note
            if (purging) {
                purge.set(false);
            }
            for (Node next = head.next; next != null; next = head.next) {
                ready.add(next.task);
                next.task = null; // the node stays as the new head; the task goes when it ends
                head = next;
            }
            if (purging) {
                ready.removeIf(Task::claimed);
            }
            // Only the dispatcher adds to running and others only take away, so a free slot stays free here.
            while (running.get() < maxConcurrent && !ready.isEmpty()) {
                Task<?> task = ready.poll();
                if (task.claim()) { // not cancelled meanwhile
                    queued.decrement();
                    running.incrementAndGet();
                    executor.execute(task::run);
                }
            }
            missed = dispatches.addAndGet(-missed);
        } while (missed != 0);
    }

    // ---------------------------------------------------------------- LaneQueue.LOCKED: one lock, no inbox

    private synchronized void enqueueLocked(Task<?> task) {
        queued.increment();
        ready.add(task);
        dispatchLocked();
    }

    private synchronized boolean removeLocked(Task<?> task) {
        if (!ready.remove(task)) {
            return false;
        }
        queued.decrement();
        return true;
    }

    private synchronized void finishedLocked() {
        running.decrementAndGet();
        dispatchLocked();
    }

    private void dispatchLocked() {
        while (running.get() < maxConcurrent && !ready.isEmpty()) {
            Task<?> task = ready.poll();
            queued.decrement();
            running.incrementAndGet();
            executor.execute(task::run);
        }
    }

    /** An inbox entry. */
    private static final class Node {

        Task<?> task;
        volatile Node next;

        Node(Task<?> task) {
            this.task = task;
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
