package com.dell.spt.base.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs Task instances on Virtual Threads. Replaces fiber4j's FibersExecutor.
 */
public class VirtualThreadExecutor implements AutoCloseable {

	private final ExecutorService executor;

	public VirtualThreadExecutor() {
		this.executor = Executors.newVirtualThreadPerTaskExecutor();
	}

	public void submit(final Runnable task) {
		executor.submit(task);
	}

	@Override
	public void close() {
		executor.shutdown();
		try {
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (final InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	public boolean isShutdown() {
		return executor.isShutdown();
	}
}
