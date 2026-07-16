package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PlatformThreadExecutorTest {

	private static final int SCALE_TASK_COUNT = 48;
	private static final long MAX_SCALE_RSS_GROWTH_BYTES = 128L * 1024 * 1024;

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

	@Test
	void scaledServiceTasksRestartWithoutThreadLeaksOrUnboundedRssGrowth() throws Exception {
		final long rssBefore = linuxRssBytes();
		final var firstEntered = new CountDownLatch(SCALE_TASK_COUNT);
		final var secondEntered = new CountDownLatch(SCALE_TASK_COUNT);
		final var firstRelease = new CountDownLatch(1);
		final var secondRelease = new CountDownLatch(1);
		final var release = new AtomicReference<>(firstRelease);
		final List<Thread> taskThreads = new java.util.concurrent.CopyOnWriteArrayList<>();
		final List<TaskBase> tasks = new ArrayList<>(SCALE_TASK_COUNT);

		try (final var executor = new PlatformThreadExecutor()) {
			for (int i = 0; i < SCALE_TASK_COUNT; i++) {
				final var generation = new AtomicInteger();
				tasks.add(new TaskBase(executor) {
					@Override
					protected void doInit() {
						taskThreads.add(Thread.currentThread());
						if (generation.incrementAndGet() == 1) {
							firstEntered.countDown();
						} else {
							secondEntered.countDown();
						}
					}

					@Override
					protected void doWork() throws Exception {
						release.get().await();
						stop();
					}
				});
			}

			tasks.forEach(TaskBase::start);
			assertTrue(firstEntered.await(10, TimeUnit.SECONDS));
			assertServiceThreads(taskThreads, SCALE_TASK_COUNT);
			assertRssGrowthIsBounded(rssBefore, linuxRssBytes());

			firstRelease.countDown();
			awaitAll(tasks);
			release.set(secondRelease);
			tasks.forEach(TaskBase::restart);
			assertTrue(secondEntered.await(10, TimeUnit.SECONDS));
			assertServiceThreads(taskThreads, SCALE_TASK_COUNT * 2);

			secondRelease.countDown();
			awaitAll(tasks);
		}

		assertTrue(taskThreads.stream().noneMatch(Thread::isAlive),
						"all scaled service-task threads should terminate after close");
	}

	private static void awaitAll(final List<TaskBase> tasks) throws InterruptedException {
		for (final var task : tasks) {
			assertTrue(task.awaitStop(10, TimeUnit.SECONDS));
		}
	}

	private static void assertServiceThreads(final List<Thread> threads, final int expectedCount) {
		assertEquals(expectedCount, threads.size());
		assertEquals(expectedCount, new HashSet<>(threads).size(),
						"each task start should use a fresh thread");
		assertTrue(threads.stream().allMatch(thread -> thread.getName().startsWith("spt-service-")));
		assertTrue(threads.stream().allMatch(Thread::isDaemon));
		assertTrue(threads.stream().noneMatch(Thread::isVirtual));
	}

	private static long linuxRssBytes() throws Exception {
		final Path statusPath = Path.of("/proc/self/status");
		if (!Files.isReadable(statusPath)) {
			return -1;
		}
		for (final String line : Files.readAllLines(statusPath)) {
			if (line.startsWith("VmRSS:")) {
				return Long.parseLong(line.replaceAll("[^0-9]", "")) * 1024;
			}
		}
		return -1;
	}

	private static void assertRssGrowthIsBounded(final long before, final long active) {
		if (before >= 0 && active >= 0) {
			assertTrue(active - before < MAX_SCALE_RSS_GROWTH_BYTES,
							"scaled service tasks grew RSS by " + (active - before) + " bytes");
		}
	}
}
