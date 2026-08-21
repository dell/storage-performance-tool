package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TaskBaseTest {

	@Test
	void doWorkIsCalledRepeatedly() throws Exception {
		final var callCount = new AtomicInteger(0);
		final var reachedTarget = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() {
					if (callCount.incrementAndGet() >= 5) {
						reachedTarget.countDown();
						stop();
					}
				}
			};
			task.start();
			assertTrue(reachedTarget.await(5, TimeUnit.SECONDS), "doWork should be called repeatedly");
			assertTrue(callCount.get() >= 5);
		}
	}

	@Test
	void runsOnVirtualThread() throws Exception {
		final var isVirtual = new AtomicBoolean(false);
		final var done = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() {
					isVirtual.set(Thread.currentThread().isVirtual());
					done.countDown();
					stop();
				}
			};
			task.start();
			assertTrue(done.await(5, TimeUnit.SECONDS));
			assertTrue(isVirtual.get(), "Task should run on a virtual thread");
		}
	}

	@Test
	void stopInterruptsBlockingWork() throws Exception {
		final var entered = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() throws Exception {
					entered.countDown();
					Thread.sleep(60_000); // block until interrupted
				}
			};
			task.start();
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			task.stop();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS), "Task should stop after interrupt");
			assertTrue(task.isStopped());
		}
	}

	@Test
	void doubleStartThrows() {
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() {
					stop();
				}
			};
			task.start();
			assertThrows(IllegalStateException.class, task::start);
		}
	}

	@Test
	void isStartedReflectsState() throws Exception {
		final var entered = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() throws Exception {
					entered.countDown();
					Thread.sleep(60_000);
				}
			};
			assertFalse(task.isStarted());
			assertFalse(task.isStopped());
			task.start();
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			assertTrue(task.isStarted());
			assertFalse(task.isStopped());
			task.stop();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
			assertFalse(task.isStarted());
			assertTrue(task.isStopped());
		}
	}

	@Test
	void closeStopsAndCallsDoClose() throws Exception {
		final var doCloseCalled = new AtomicBoolean(false);
		final var entered = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() throws Exception {
					entered.countDown();
					Thread.sleep(60_000);
				}

				@Override
				protected void doClose() {
					doCloseCalled.set(true);
				}
			};
			task.start();
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			task.close();
			assertTrue(task.isStopped());
			assertTrue(doCloseCalled.get(), "doClose should be called by close()");
		}
	}

	@Test
	void doStopCalledOnNormalExit() throws Exception {
		final var doStopCalled = new AtomicBoolean(false);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() {
					stop();
				}

				@Override
				protected void doStop() {
					doStopCalled.set(true);
				}
			};
			task.start();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
			assertTrue(doStopCalled.get(), "doStop should be called when task exits");
		}
	}

	@Test
	void handleErrorCalledOnException() throws Exception {
		final var caughtError = new AtomicReference<Exception>();
		final var done = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() throws Exception {
					throw new RuntimeException("test error");
				}

				@Override
				protected void handleError(final Exception e) {
					caughtError.set(e);
					done.countDown();
				}
			};
			task.start();
			assertTrue(done.await(5, TimeUnit.SECONDS));
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
			assertNotNull(caughtError.get());
			assertEquals("test error", caughtError.get().getMessage());
			assertTrue(task.isStopped());
		}
	}

	@Test
	void stopBeforeStartIsSafe() throws Exception {
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() {
					fail("should never be called");
				}
			};
			// stop before start should not throw
			assertDoesNotThrow(task::stop);
			assertTrue(task.isStopped());
			assertTrue(task.awaitStop(10, TimeUnit.MILLISECONDS));
		}
	}

	@Test
	void restartRejectsOverlapUntilPriorTaskThreadTerminates() throws Exception {
		final var entered = new CountDownLatch(1);
		final var stopping = new CountDownLatch(1);
		final var allowTermination = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doWork() throws Exception {
					entered.countDown();
					Thread.sleep(60_000);
				}

				@Override
				protected void doStop() {
					stopping.countDown();
					try {
						allowTermination.await();
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			};
			task.start();
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			task.stop();
			assertTrue(stopping.await(5, TimeUnit.SECONDS));
			assertThrows(IllegalStateException.class, task::restart);

			allowTermination.countDown();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
			assertDoesNotThrow(task::restart);
			task.stop();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
		}
	}
}
