package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PlatformThreadExecutorTest {

	@Test
	void serviceTasksUsePlatformThreadExecutor() {
		assertInstanceOf(PlatformThreadExecutor.class, ServiceTaskExecutor.TASK_EXECUTOR);
	}

	@Test
	void taskBaseRunsAndRestartsOnPlatformThreads() throws Exception {
		final var initCount = new AtomicInteger();
		final var runCount = new AtomicInteger();
		final var firstThread = new AtomicReference<Thread>();
		final var secondThread = new AtomicReference<Thread>();
		try (final var executor = new PlatformThreadExecutor()) {
			final var task = new TaskBase(executor) {
				@Override
				protected void doInit() {
					initCount.incrementAndGet();
				}

				@Override
				protected void doWork() {
					final int invocation = runCount.incrementAndGet();
					if (invocation == 1) {
						firstThread.set(Thread.currentThread());
					} else {
						secondThread.set(Thread.currentThread());
					}
					stop();
				}
			};

			task.start();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
			task.restart();
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS));

			assertEquals(2, initCount.get());
			assertEquals(2, runCount.get());
			assertFalse(firstThread.get().isVirtual());
			assertFalse(secondThread.get().isVirtual());
			assertTrue(firstThread.get().isDaemon(),
							"platform service tasks should preserve virtual-thread exit behavior");
			assertTrue(secondThread.get().isDaemon(),
							"platform service tasks should preserve virtual-thread exit behavior");
			assertNotSame(firstThread.get(), secondThread.get(),
							"restart should submit a fresh platform thread");
		}
	}

	@Test
	void stopInterruptsBlockingPlatformTaskAndExecutorCloses() throws Exception {
		final var entered = new CountDownLatch(1);
		final var interrupted = new AtomicBoolean();
		final var executor = new PlatformThreadExecutor();
		final var task = new TaskBase(executor) {
			@Override
			protected void doWork() throws Exception {
				entered.countDown();
				try {
					Thread.sleep(60_000);
				} catch (final InterruptedException e) {
					interrupted.set(true);
					throw e;
				}
			}
		};

		task.start();
		assertTrue(entered.await(5, TimeUnit.SECONDS));
		task.stop();
		assertTrue(task.awaitStop(5, TimeUnit.SECONDS));
		assertTrue(interrupted.get());
		executor.close();
		assertTrue(executor.isShutdown());
	}
}
