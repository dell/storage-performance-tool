package com.dell.spt.base.concurrent;

/**
 * Runs Task instances on Virtual Threads.
 * <p>
 * Tunes the VT carrier ForkJoinPool parallelism to avoid excessive work-stealing
 * overhead. SPT runs only a handful of active VTs (LoadGenerator,
 * OperationDispatchTask, MetricsManager, etc.) so the JVM default of
 * availableProcessors() carrier threads is wasteful on large machines.
 * Override with {@code -Djdk.virtualThreadScheduler.parallelism=N} or
 * set {@code load.service.threads} in the SPT config.
 */
public class VirtualThreadExecutor extends ThreadTaskExecutor {

	private static final String VT_PARALLELISM_PROP = "jdk.virtualThreadScheduler.parallelism";

	static {
		if (System.getProperty(VT_PARALLELISM_PROP) == null) {
			final int parallelism = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
			System.setProperty(VT_PARALLELISM_PROP, String.valueOf(parallelism));
		}
	}

	public VirtualThreadExecutor() {
		super(Thread.ofVirtual().factory());
	}
}
