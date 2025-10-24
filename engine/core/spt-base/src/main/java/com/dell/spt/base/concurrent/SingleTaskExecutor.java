package com.dell.spt.base.concurrent;

public interface SingleTaskExecutor extends TaskExecutor {

	/** Returns the active task instance, or null when no active task is running. */
	Runnable task();

	/**
	 * Atomically stop the active task.
	 *
	 * @param task the task to check if it is still active
	 * @return true if the task was active and stopped; false otherwise
	 */
	boolean stop(final Runnable task);
}
