package com.truesim.common.jobs;

/** Where a {@link Task} is in its life. */
public enum TaskState {
    /** Waiting for the tasks it depends on. */
    WAITING,
    /** Ready, waiting for a free slot on its lane. */
    QUEUED,
    RUNNING,
    SUCCEEDED,
    /** The task threw, or a task it depends on did not succeed. */
    FAILED,
    CANCELLED;

    /** Whether the task has finished, one way or another. */
    public boolean isDone() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
