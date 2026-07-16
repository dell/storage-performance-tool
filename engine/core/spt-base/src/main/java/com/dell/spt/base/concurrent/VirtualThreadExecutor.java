package com.dell.spt.base.concurrent;

/**
 * Runs Task instances on Virtual Threads.
 * <p>
 * Tunes the VT carrier ForkJoinPool parallelism used by the remaining short-lived
 * and blocking virtual-thread paths. Long-lived recurring service tasks use
 * {@link PlatformThreadExecutor} instead.
 * Override with {@code -Djdk.virtualThreadScheduler.parallelism=N} or
 * set the legacy-named {@code load.service.threads} option in the SPT config.
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
