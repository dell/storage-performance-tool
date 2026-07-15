package com.dell.spt.base.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Shared lifecycle for thread-per-task executors used by SPT recurring tasks.
 */
public abstract class ThreadTaskExecutor implements AutoCloseable {

	private final ExecutorService executor;

	protected ThreadTaskExecutor(final ThreadFactory threadFactory) {
		this.executor = Executors.newThreadPerTaskExecutor(threadFactory);
	}

	@SuppressWarnings("FutureReturnValueIgnored")
	public final void submit(final Runnable task) {
		executor.submit(task);
	}

	@Override
	public final void close() {
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

	public final boolean isShutdown() {
		return executor.isShutdown();
	}
}
