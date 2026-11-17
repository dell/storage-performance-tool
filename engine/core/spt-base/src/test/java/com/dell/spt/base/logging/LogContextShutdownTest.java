package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.jupiter.api.Test;

final class LogContextShutdownTest {

	/**
	 * A context whose stop takes {@code stopMillis} whatever timeout it is given, as Log4j's own drain wait
	 * does, or until {@code release} counts down.
	 */
	private static final class SlowContext extends LoggerContext {

		private final long stopMillis;
		private final CountDownLatch release;
		volatile long givenNanos = -1;
		volatile boolean stopCalled;

		SlowContext(final String name, final long stopMillis, final CountDownLatch release) {
			super(name);
			this.stopMillis = stopMillis;
			this.release = release;
		}

		@Override
		public boolean stop(final long timeout, final TimeUnit timeUnit) {
			givenNanos = timeUnit.toNanos(timeout);
			stopCalled = true;
			try {
				release.await(stopMillis, TimeUnit.MILLISECONDS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return true;
		}
	}

	@Test
	void stopsEachContextAtOnceWhenNothingIsQueued() {
		final var release = new CountDownLatch(0);
		final var first = new SlowContext("first", 0, release);
		final var second = new SlowContext("second", 0, release);
		final var firstRun = new CountDownLatch(1);
		final var lastRun = new CountDownLatch(1);
		final long start = System.nanoTime();
		assertTrue(LogContextShutdown.stopAll(
						List.of(first, second), 10_000, () -> firstRun.countDown(), () -> lastRun.countDown(), Thread::new));
		final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertTrue(first.stopCalled && second.stopCalled);
		assertEquals(0, firstRun.getCount());
		assertEquals(0, lastRun.getCount());
		assertTrue(first.givenNanos > 0 && second.givenNanos > 0);
		assertTrue(elapsedMillis < 2_000, "an empty queue took " + elapsedMillis + " ms");
	}

	@Test
	void waitsForAllContextsWithinOneDeadline() {
		final var release = new CountDownLatch(1);
		try {
			// The second stop alone outlasts the deadline; a timeout of its own would wait 5 s more
			final var first = new SlowContext("first", 400, release);
			final var second = new SlowContext("second", 5_000, release);
			final long start = System.nanoTime();
			assertFalse(LogContextShutdown.stopAll(List.of(first, second), 1_000, () -> {}, () -> {}, Thread::new));
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			assertTrue(elapsedMillis >= 900 && elapsedMillis < 3_000, "the stops took " + elapsedMillis + " ms");
			// The second stop gets only the time the first left
			assertTrue(second.stopCalled);
			assertTrue(second.givenNanos < first.givenNanos, second.givenNanos + " >= " + first.givenNanos);
			assertTrue(first.givenNanos <= TimeUnit.MILLISECONDS.toNanos(1_000));
		} finally {
			release.countDown();
		}
	}

	@Test
	void skipsTheContextsLeftAfterTheDeadline() throws InterruptedException {
		final var release = new CountDownLatch(1);
		final var stopper = new AtomicReference<Thread>();
		try {
			final var first = new SlowContext("first", 5_000, release);
			final var second = new SlowContext("second", 0, release);
			final var lastRun = new CountDownLatch(1);
			final ThreadFactory threads = task -> {
				final var thread = new Thread(task);
				stopper.set(thread);
				return thread;
			};
			assertFalse(LogContextShutdown.stopAll(List.of(first, second), 300, () -> {}, lastRun::countDown, threads));
			release.countDown();
			// The stopper finds no time left for the second context, and ends without its last step
			stopper.get().join(5_000);
			assertFalse(stopper.get().isAlive());
			assertFalse(second.stopCalled);
			assertEquals(1, lastRun.getCount());
		} finally {
			release.countDown();
		}
	}

	@Test
	void writesNothingFromTheCallerWhenTheStopperIsBlocked() {
		final var release = new CountDownLatch(1);
		try {
			// A stopper blocked on a full pipe: the caller returns at the deadline
			final long start = System.nanoTime();
			assertFalse(LogContextShutdown.stopAll(List.of(), 300, () -> awaitQuietly(release), () -> {}, Thread::new));
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			assertTrue(elapsedMillis >= 250 && elapsedMillis < 2_000, "the caller took " + elapsedMillis + " ms");
		} finally {
			release.countDown();
		}
	}

	@Test
	void quietsTheStatusLoggerBelowError() {
		final var statusLogger = StatusLogger.getLogger();
		final var level = statusLogger.getFallbackListener().getStatusLevel();
		try {
			statusLogger.getFallbackListener().setLevel(Level.INFO);
			LogContextShutdown.quietStatusLogger();
			assertEquals(Level.ERROR, statusLogger.getFallbackListener().getStatusLevel());
			assertFalse(statusLogger.isEnabled(Level.WARN, null));
			assertTrue(statusLogger.isEnabled(Level.ERROR, null));
		} finally {
			statusLogger.getFallbackListener().setLevel(level);
		}
	}

	private static void awaitQuietly(final CountDownLatch latch) {
		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Test
	void listsTheEngineLoggersContextFirstAndEachContextOnce() {
		final var contexts = LogContextShutdown.engineContexts();
		assertSame(((org.apache.logging.log4j.core.Logger) Loggers.MSG).getContext(), contexts.get(0));
		final var distinct = Collections.newSetFromMap(new IdentityHashMap<LoggerContext, Boolean>());
		distinct.addAll(contexts);
		assertEquals(contexts.size(), distinct.size());
	}

	@Test
	void defaultsToTenSecondsWithoutAContext() {
		assertEquals(10_000, LogContextShutdown.timeoutMillis(List.of()));
	}
}
