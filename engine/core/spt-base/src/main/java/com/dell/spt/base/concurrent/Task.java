package com.dell.spt.base.concurrent;

/**
 * Lifecycle contract for a recurring background task. Replaces fiber4j's Fiber interface.
 * Implementations run on Virtual Threads via TaskExecutor.
 */
public interface Task extends AutoCloseable {
	void start();

	void stop();

	void close();

	boolean isStarted();

	boolean isStopped();
}
