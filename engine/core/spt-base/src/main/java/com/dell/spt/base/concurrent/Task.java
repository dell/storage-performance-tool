package com.dell.spt.base.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * Lifecycle contract for a recurring background task.
 * Implementations run on Virtual Threads via TaskExecutor.
 */
public interface Task extends AutoCloseable {
	void start();

	void stop();

	void close();

	boolean isStarted();

	boolean isStopped();

	/**
	 * Wait for the task to finish after stop() has been called (or after it self-stops).
	 *
	 * @return true if the task stopped within the timeout, false otherwise
	 */
	boolean await(long timeout, TimeUnit unit) throws InterruptedException;
}
