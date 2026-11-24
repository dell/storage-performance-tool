package com.dell.spt.base.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import static org.junit.jupiter.api.Assertions.*;

class SingleTaskExecutorImplTest {

	@BeforeAll
	static void initLog4j() {
		ThreadContext.put("test", "init");
		ThreadContext.remove("test");
		LogManager.getLogger(SingleTaskExecutorImplTest.class).info("Initializing Log4j");
	}

	private SingleTaskExecutorImpl executor;

	@BeforeEach
	void setUp() {
		executor = new SingleTaskExecutorImpl();
	}

	@AfterEach
	void tearDown() {
		executor.close();
	}

	@Test
	void testExecuteTask() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicBoolean executed = new AtomicBoolean(false);

		executor.execute(() -> {
			executed.set(true);
			latch.countDown();
		});

		assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should be executed within timeout");
		assertTrue(executed.get(), "Task flag should be set to true");
	}

	@Test
	void testRejectExecutionWhenBusy() throws InterruptedException {
		final CountDownLatch startLatch = new CountDownLatch(1);

		executor.execute(() -> {
			startLatch.countDown();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});

		// Wait for the first task to start
		assertTrue(startLatch.await(5, TimeUnit.SECONDS));

		// Submitting a second task should throw RejectedExecutionException
		assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {}));
	}

	@Test
	void testTaskClearedAfterExecution() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		final Runnable task = () -> {
			latch.countDown();
		};

		executor.execute(task);
		assertTrue(latch.await(5, TimeUnit.SECONDS));

		// Wait for the worker to clean up the taskRef
		long deadline = System.currentTimeMillis() + 5000;
		while (executor.task() != null && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}

		assertNull(executor.task(), "Task should be null after completion");
	}

	@Test
	void testStopRunningTask() throws InterruptedException {
		final CountDownLatch startLatch = new CountDownLatch(1);

		final Runnable task = () -> {
			startLatch.countDown();
			try {
				// Block for a long time
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt(); // normal case for interruption
			}
		};

		executor.execute(task);

		// Wait for it to start
		assertTrue(startLatch.await(5, TimeUnit.SECONDS));

		// Now stop it
		assertTrue(executor.stop(task), "Should successfully stop the task");

		// Wait for the worker thread to be fully recreated
		Thread.sleep(100);

		// After stopping, we should be able to execute a new task
		final CountDownLatch newLatch = new CountDownLatch(1);
		executor.execute(newLatch::countDown);
		assertTrue(newLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	void testStopNonRunningTask() {
		final Runnable dummyTask = () -> {};
		assertFalse(executor.stop(dummyTask), "Stopping a task that is not running should return false");
	}

	@Test
	void testExceptionHandlingSurvives() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		// Execute a task that throws an exception
		executor.execute(() -> {
			latch.countDown();
			throw new RuntimeException("Test exception");
		});

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		// Wait for the worker to clean up the taskRef
		long deadline = System.currentTimeMillis() + 5000;
		while (executor.task() != null && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}

		assertNull(executor.task(), "Task should be cleared even if it threw an exception");

		// The worker thread should survive the exception and accept new tasks
		final CountDownLatch nextLatch = new CountDownLatch(1);
		executor.execute(nextLatch::countDown);
		assertTrue(nextLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	void anInterruptThatWasNotAStopLeavesTheWorkerToRunTheNextTask() throws InterruptedException {
		final var firstThread = new AtomicReference<Thread>();
		final Runnable interrupted = () -> {
			firstThread.set(Thread.currentThread());
			throwUnchecked(new InterruptedException("not a stop"));
		};
		executor.execute(interrupted);
		awaitSlotCleared();

		final var next = nextTask();

		assertTrue(next.ran.await(5, TimeUnit.SECONDS), "the worker ended with the interrupt");
		assertSame(firstThread.get(), next.thread.get(), "no stop was requested, so the worker is the same");
		assertFalse(next.interrupted.get(), "the next task started interrupted");
	}

	@Test
	void aTaskThatEndsWithItsInterruptFlagSetDoesNotInterruptTheNextTask() throws InterruptedException {
		// The next task is submitted as soon as the slot frees, before the worker can park, which would
		// clear the flag on its own
		for (int i = 0; i < 1000; i++) {
			final var finished = new AtomicBoolean();
			final var next = new NextTask();
			final Runnable nextTask = () -> {
				next.thread.set(Thread.currentThread());
				next.interrupted.set(Thread.currentThread().isInterrupted());
				next.ran.countDown();
			};
			final var submitter = new Thread(() -> {
				while (!finished.get()) {
					Thread.onSpinWait();
				}
				while (true) {
					try {
						executor.execute(nextTask);
						return;
					} catch (final RejectedExecutionException busy) {
						Thread.onSpinWait();
					}
				}
			});
			submitter.start();
			executor.execute(() -> {
				Thread.currentThread().interrupt();
				finished.set(true);
			});

			assertTrue(next.ran.await(5, TimeUnit.SECONDS));
			assertFalse(next.interrupted.get(), "the next task started interrupted, round " + i);
			submitter.join(5000);
			awaitSlotCleared();
		}
	}

	@Test
	void aStoppedTaskEndsQuietlyWithExactlyOneReplacementWorker() throws InterruptedException {
		final var started = new CountDownLatch(1);
		final var stoppedThread = new AtomicReference<Thread>();
		final Runnable task = () -> {
			stoppedThread.set(Thread.currentThread());
			started.countDown();
			try {
				new CountDownLatch(1).await();
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		};
		executor.execute(task);
		assertTrue(started.await(5, TimeUnit.SECONDS));
		assertTrue(executor.stop(task));
		stoppedThread.get().join(5000);
		assertFalse(stoppedThread.get().isAlive(), "the stopped worker did not end");

		final var second = nextTask();
		assertTrue(second.ran.await(5, TimeUnit.SECONDS));
		awaitSlotCleared();
		final var third = nextTask();
		assertTrue(third.ran.await(5, TimeUnit.SECONDS));

		assertNotSame(stoppedThread.get(), second.thread.get());
		assertSame(second.thread.get(), third.thread.get(), "a stop starts exactly one replacement worker");
		assertFalse(second.interrupted.get());
	}

	private NextTask nextTask() {
		final var next = new NextTask();
		executor.execute(() -> {
			next.thread.set(Thread.currentThread());
			next.interrupted.set(Thread.currentThread().isInterrupted());
			next.ran.countDown();
		});
		return next;
	}

	private void awaitSlotCleared() throws InterruptedException {
		final long deadline = System.currentTimeMillis() + 5000;
		while (executor.task() != null && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}
		assertNull(executor.task(), "the task did not end");
	}

	private static final class NextTask {
		private final CountDownLatch ran = new CountDownLatch(1);
		private final AtomicReference<Thread> thread = new AtomicReference<>();
		private final AtomicBoolean interrupted = new AtomicBoolean();
	}
}
