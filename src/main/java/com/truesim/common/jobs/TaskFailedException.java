package com.truesim.common.jobs;

/** Thrown by {@link Task#result()} when the task failed or was cancelled; the cause says why. */
public final class TaskFailedException extends RuntimeException {

    private static final long serialVersionUID = 1;

    TaskFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
