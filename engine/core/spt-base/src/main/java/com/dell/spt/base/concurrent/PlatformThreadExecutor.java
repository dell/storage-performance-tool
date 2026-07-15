package com.dell.spt.base.concurrent;

/**
 * Runs each submitted long-lived service task on its own platform thread.
 */
public final class PlatformThreadExecutor extends ThreadTaskExecutor {

	private static final String THREAD_NAME_PREFIX = "spt-service-";

	public PlatformThreadExecutor() {
		super(Thread.ofPlatform().daemon(true).name(THREAD_NAME_PREFIX, 0).factory());
	}
}
