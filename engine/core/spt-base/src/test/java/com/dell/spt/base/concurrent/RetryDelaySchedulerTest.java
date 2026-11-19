package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.Test;

class RetryDelaySchedulerTest {

	private static final String LOG_KEY = "retry_delay_test";
	private static final long FAR_FUTURE_MILLIS = TimeUnit.HOURS.toMillis(1);

	@Test
	void runsOnTheNamedDaemonThreadWithNothingInherited() throws Exception {
		final var observed = new CompletableFuture<Thread>();
		final var loader = new ClassLoader(ClassLoader.getSystemClassLoader()) {
		};
		final var starter = Thread.currentThread();
		final var previousLoader = starter.getContextClassLoader();
		starter.setContextClassLoader(loader);
		try {
			RetryDelayScheduler.schedule(1, () -> observed.complete(Thread.currentThread()));
		} finally {
			starter.setContextClassLoader(previousLoader);
		}
		final var thread = observed.get(10, TimeUnit.SECONDS);
		assertEquals(RetryDelayScheduler.THREAD_NAME, thread.getName());
		assertTrue(thread.isDaemon());
		assertEquals(Thread.NORM_PRIORITY, thread.getPriority());
		assertEquals(ClassLoader.getSystemClassLoader(), thread.getContextClassLoader());
	}

	@Test
	void eachTaskSeesOnlyTheLogContextOfItsOwnScheduler() throws Exception {
		final var first = new CompletableFuture<String>();
		final var second = new CompletableFuture<String>();
		final var after = new CompletableFuture<String>();
		ThreadContext.put(LOG_KEY, "step-a");
		try {
			RetryDelayScheduler.schedule(1, () -> first.complete(ThreadContext.get(LOG_KEY)));
			ThreadContext.put(LOG_KEY, "step-b");
			RetryDelayScheduler.schedule(1, () -> second.complete(ThreadContext.get(LOG_KEY)));
			assertEquals("step-a", first.get(10, TimeUnit.SECONDS));
			assertEquals("step-b", second.get(10, TimeUnit.SECONDS));
		} finally {
			ThreadContext.remove(LOG_KEY);
		}
		RetryDelayScheduler.schedule(1, () -> after.complete(ThreadContext.get(LOG_KEY)));
		assertNull(after.get(10, TimeUnit.SECONDS), "a finished task must leave no log context on the shared thread");
	}

	@Test
	void cancelledTaskNeverRunsAndLeavesTheQueue() throws Exception {
		final var ran = new AtomicBoolean();
		final List<Future<?>> futures = new ArrayList<>();
		for (var i = 0; i < 100; i++) {
			futures.add(RetryDelayScheduler.schedule(FAR_FUTURE_MILLIS, () -> ran.set(true)));
		}
		futures.forEach(future -> assertTrue(RetryDelayScheduler.isQueued(future)));
		futures.forEach(future -> assertTrue(future.cancel(false)));
		futures.forEach(future -> assertFalse(RetryDelayScheduler.isQueued(future)));
		final var drained = new CountDownLatch(1);
		RetryDelayScheduler.schedule(1, drained::countDown);
		assertTrue(drained.await(10, TimeUnit.SECONDS));
		assertFalse(ran.get());
	}

	@Test
	void failedTaskGoesToItsFutureAndTheThreadCarriesOn() throws Exception {
		final var failure = new IllegalStateException("task failure");
		final var failed = RetryDelayScheduler.schedule(1, () -> {
			throw failure;
		});
		final var thrown = assertThrows(ExecutionException.class, () -> failed.get(10, TimeUnit.SECONDS));
		assertInstanceOf(IllegalStateException.class, thrown.getCause());
		final var next = new CompletableFuture<String>();
		RetryDelayScheduler.schedule(1, () -> next.complete(Thread.currentThread().getName()));
		assertEquals(RetryDelayScheduler.THREAD_NAME, next.get(10, TimeUnit.SECONDS));
	}
}
