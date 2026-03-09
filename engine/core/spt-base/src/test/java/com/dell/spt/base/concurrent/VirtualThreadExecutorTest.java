package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class VirtualThreadExecutorTest {

	@Test
	void submitRunsOnVirtualThread() throws Exception {
		final var isVirtual = new AtomicBoolean(false);
		final var done = new CountDownLatch(1);
		try (final var executor = new VirtualThreadExecutor()) {
			executor.submit(() -> {
				isVirtual.set(Thread.currentThread().isVirtual());
				done.countDown();
			});
			assertTrue(done.await(5, TimeUnit.SECONDS));
			assertTrue(isVirtual.get(), "Submitted task should run on a virtual thread");
		}
	}

	@Test
	void closeShutdownsGracefully() throws Exception {
		final var done = new CountDownLatch(1);
		final var executor = new VirtualThreadExecutor();
		executor.submit(() -> {
			done.countDown();
		});
		assertTrue(done.await(5, TimeUnit.SECONDS));
		assertDoesNotThrow(executor::close);
		assertTrue(executor.isShutdown());
	}

	@Test
	void multipleConcurrentTasks() throws Exception {
		final int taskCount = 100;
		final var latch = new CountDownLatch(taskCount);
		try (final var executor = new VirtualThreadExecutor()) {
			for (int i = 0; i < taskCount; i++) {
				executor.submit(latch::countDown);
			}
			assertTrue(latch.await(10, TimeUnit.SECONDS),
							"All " + taskCount + " tasks should complete");
		}
	}
}
