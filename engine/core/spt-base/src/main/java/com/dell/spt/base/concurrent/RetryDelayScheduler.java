package com.dell.spt.base.concurrent;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.ThreadContext;

/**
 * The JVM's one timer for delayed operation retries. Its single daemon thread starts with the
 * first scheduled retry, so a run without one never has it, and runs each short task itself.
 * Retries therefore do not depend on the ForkJoin common pool, which a distributed step's await
 * can hold for the whole step.
 *
 * <p>The executor is deliberately never shut down: the JVM owns it, and a daemon thread holds
 * neither an entry's exit nor a node's shutdown. A per-step executor would reject a retry
 * scheduled after its step was cancelled, and that retry must still end as a counted failure.
 * Each step cancels its own pending tasks, and a cancelled task leaves the queue at once.
 *
 * <p>Internal to the engine; not an extension API.
 */
public final class RetryDelayScheduler {

	public static final String THREAD_NAME = "spt-retry-delay";

	private static final ScheduledThreadPoolExecutor EXECUTOR = newExecutor();

	private RetryDelayScheduler() {}

	/**
	 * Runs {@code task} after the delay, under the log context of the calling thread, so what it
	 * logs reaches the step that scheduled it. As with {@code CompletableFuture.runAsync}, an
	 * exception from the task goes to the returned future, and the thread carries on.
	 */
	public static Future<?> schedule(final long delayMillis, final Runnable task) {
		final Map<String, String> logContext = ThreadContext.getImmutableContext();
		return EXECUTOR.schedule(
						() -> {
							applyLogContext(logContext);
							task.run();
						},
						delayMillis,
						TimeUnit.MILLISECONDS);
	}

	/**
	 * Every task replaces the thread's whole context, even when the previous task came from the same
	 * producer: a task may have changed the context, and a skipped equal snapshot would then leave
	 * the next task logging to another step.
	 */
	private static void applyLogContext(final Map<String, String> logContext) {
		ThreadContext.clearMap();
		ThreadContext.putAll(logContext);
	}

	/** Whether {@code future} is still waiting in the timer's queue. */
	static boolean isQueued(final Future<?> future) {
		return future instanceof RunnableScheduledFuture<?> queued && EXECUTOR.getQueue().contains(queued);
	}

	private static ScheduledThreadPoolExecutor newExecutor() {
		final var executor = new ScheduledThreadPoolExecutor(1, RetryDelayScheduler::newThread);
		executor.setRemoveOnCancelPolicy(true);
		return executor;
	}

	/**
	 * The thread outlives every step, so it takes neither the log context nor the class loader of
	 * the thread that happens to start it.
	 */
	static Thread newThread(final Runnable runnable) {
		final var thread = new Thread(null, runnable, THREAD_NAME, 0, false);
		thread.setDaemon(true);
		thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
		return thread;
	}
}
