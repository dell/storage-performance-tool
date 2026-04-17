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
}
