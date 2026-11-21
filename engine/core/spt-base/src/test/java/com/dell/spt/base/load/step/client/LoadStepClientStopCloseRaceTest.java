package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A signal runs the step's stop and close on the exit hook while the scenario thread is still in
 * {@code await()}'s trailing stop. The two must not run at once, and an await on a step the exit hook
 * has closed ends as a stop.
 */
@Timeout(60)
class LoadStepClientStopCloseRaceTest {

	private static final long WAIT_SECONDS = 20;

	private final Config config = TestConfigBuilder.config();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);

	@BeforeEach
	void configure() {
		config.val("load-op-type", "noop");
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
	}

	@AfterEach
	void resetJvmExit() {
		JvmExitTestSupport.reset();
	}

	@Test
	void closeWaitsForTheAwaitsStopInsideTheSliceLoop() throws Exception {
		final LoadStep slice = completedSlice();
		final CountDownLatch inScenarioStop = new CountDownLatch(1);
		final CountDownLatch releaseScenarioStop = new CountDownLatch(1);
		final AtomicInteger stopsRunning = new AtomicInteger();
		final AtomicInteger overlaps = new AtomicInteger();
		doAnswer(invocation -> {
			if (stopsRunning.incrementAndGet() > 1) {
				overlaps.incrementAndGet();
			}
			try {
				if ("scenario".equals(Thread.currentThread().getName())) {
					inScenarioStop.countDown();
					releaseScenarioStop.await();
				}
			} finally {
				stopsRunning.decrementAndGet();
			}
			return null;
		}).when(slice).stop();
		doAnswer(invocation -> {
			if (stopsRunning.get() > 0) {
				overlaps.incrementAndGet();
			}
			return null;
		}).when(slice).close();
		final TestLoadStepClient client = clientWithLocalSlice(slice);
		client.start();

		final CompletableFuture<Boolean> awaited = runAsync("scenario", () -> client.await(1, TimeUnit.MINUTES));
		assertTrue(inScenarioStop.await(WAIT_SECONDS, TimeUnit.SECONDS), "the await's stop reaches the slice loop");
		final Thread[] hook = new Thread[1];
		final CompletableFuture<Boolean> closed = runAsync("exit-hook", () -> {
			hook[0] = Thread.currentThread();
			client.close();
			return true;
		});
		awaitQueuedOrDone(client, () -> hook[0], closed);
		assertFalse(closed.isDone(), "the close must wait for the await's stop: " + outcome(closed));
		releaseScenarioStop.countDown();

		assertEquals(true, awaited.get(WAIT_SECONDS, TimeUnit.SECONDS));
		assertEquals(true, closed.get(WAIT_SECONDS, TimeUnit.SECONDS));
		assertTrue(client.isClosed());
		assertEquals(0, overlaps.get(), "a stop or close ran while another stop ran");
		// One stop for each caller of doStop(), as an unsignalled await then close does.
		verify(slice, times(2)).stop();
		verify(slice, times(1)).close();
	}

	@Test
	void twoStopsAtOnceCloseTheTimingAggregatorOnce() throws Exception {
		final LoadStep slice = completedSlice();
		final TestLoadStepClient client = clientWithLocalSlice(slice);
		client.start();
		final CountDownLatch inScenarioClose = new CountDownLatch(1);
		final CountDownLatch releaseScenarioClose = new CountDownLatch(1);
		final AtomicInteger aggregatorCloses = new AtomicInteger();
		final AutoCloseable aggregator = () -> {
			aggregatorCloses.incrementAndGet();
			if ("scenario".equals(Thread.currentThread().getName())) {
				inScenarioClose.countDown();
				releaseScenarioClose.await();
			}
		};
		timingAggregators(client).add(aggregator);

		final CompletableFuture<Boolean> awaited = runAsync("scenario", () -> client.await(1, TimeUnit.MINUTES));
		assertTrue(inScenarioClose.await(WAIT_SECONDS, TimeUnit.SECONDS), "the await's stop closes the aggregator");
		final Thread[] hook = new Thread[1];
		final CompletableFuture<Boolean> closed = runAsync("exit-hook", () -> {
			hook[0] = Thread.currentThread();
			client.close();
			return true;
		});
		awaitQueuedOrDone(client, () -> hook[0], closed);
		releaseScenarioClose.countDown();

		assertDoesNotThrow(() -> awaited.get(WAIT_SECONDS, TimeUnit.SECONDS));
		assertDoesNotThrow(() -> closed.get(WAIT_SECONDS, TimeUnit.SECONDS));
		assertTrue(client.isClosed());
		assertEquals(1, aggregatorCloses.get(), "the timing aggregator is closed exactly once");
		assertTrue(timingAggregators(client).isEmpty());
	}

	@Test
	void awaitOnAStepTheExitHookClosedEndsAsAStop() throws Exception {
		final TestLoadStepClient client = clientWithLocalSlice(completedSlice());
		client.start();
		JvmExit.markBegun();
		final CompletableFuture<Boolean> closed = runAsync("exit-hook", () -> {
			client.close();
			return true;
		});
		assertEquals(true, closed.get(WAIT_SECONDS, TimeUnit.SECONDS));

		final InterruptedException stop = assertThrows(
						InterruptedException.class, () -> client.await(1, TimeUnit.MINUTES));

		assertEquals(client.loadStepId() + ": stopped, the engine is exiting", stop.getMessage());
	}

	@Test
	void awaitOnAStepTheExitHookClosesWhileItWaitsEndsAsAStop() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		final CountDownLatch polled = new CountDownLatch(1);
		final CountDownLatch releasePoll = new CountDownLatch(1);
		when(slice.await(anyLong(), any())).thenAnswer(invocation -> {
			polled.countDown();
			releasePoll.await();
			return true;
		});
		final TestLoadStepClient client = clientWithLocalSlice(slice);
		client.start();
		final CompletableFuture<Boolean> awaited = runAsync("scenario", () -> client.await(1, TimeUnit.MINUTES));
		assertTrue(polled.await(WAIT_SECONDS, TimeUnit.SECONDS));
		JvmExit.markBegun();
		client.close();
		releasePoll.countDown();

		final Throwable failure = failure(awaited);

		assertInstanceOf(InterruptedException.class, failure, String.valueOf(failure));
		verify(slice, times(1)).close();
	}

	@Test
	void awaitOnANeverStartedStepStillThrowsIllegalState() {
		final TestLoadStepClient client = clientWithLocalSlice(completedSlice());

		assertThrows(IllegalStateException.class, () -> client.await(1, TimeUnit.SECONDS));
		JvmExit.markBegun();
		assertThrows(IllegalStateException.class, () -> client.await(1, TimeUnit.SECONDS));
	}

	@Test
	void awaitOnAClosedStepWithoutAnExitStillThrowsIllegalState() throws Exception {
		final TestLoadStepClient client = clientWithLocalSlice(completedSlice());
		client.start();
		client.close();

		assertThrows(IllegalStateException.class, () -> client.await(1, TimeUnit.SECONDS));
	}

	@Test
	void aTrailingStopAfterTheCloseChangesNothing() throws Exception {
		final LoadStep slice = completedSlice();
		final TestLoadStepClient client = clientWithLocalSlice(slice);
		final MetricsContext<?> context = mock(MetricsContext.class);
		client.addMetricsContextForTest(context);
		client.start();
		final AtomicInteger aggregatorCloses = new AtomicInteger();
		timingAggregators(client).add(aggregatorCloses::incrementAndGet);
		client.close();
		verify(metricsMgr, times(1)).unregister(context);
		assertEquals(1, aggregatorCloses.get());
		timingAggregators(client).add(aggregatorCloses::incrementAndGet);

		client.doStop();

		verify(metricsMgr, times(1)).unregister(context);
		verify(slice, times(1)).stop();
		assertEquals(1, aggregatorCloses.get(), "the trailing stop closes nothing");
		assertTrue(client.isClosed());
	}

	@SuppressWarnings("unchecked")
	private TestLoadStepClient clientWithLocalSlice(final LoadStep slice) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		return new TestLoadStepClient(config, List.of(factory), List.of(), metricsMgr);
	}

	private static LoadStep completedSlice() {
		final LoadStep slice = mock(LoadStep.class);
		try {
			when(slice.await(anyLong(), any())).thenReturn(true);
		} catch (final Exception e) {
			throw new LinkageError(e.getMessage(), e);
		}
		return slice;
	}

	@SuppressWarnings("unchecked")
	private static List<AutoCloseable> timingAggregators(final LoadStepClientBase<?> client) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField("itemTimingMetricsOutputFileAggregators");
		field.setAccessible(true);
		return (List<AutoCloseable>) field.get(client);
	}

	/** Waits until the close is queued on the client's stop and close lock, or has ended. */
	private static void awaitQueuedOrDone(
					final LoadStepClientBase<?> client,
					final Supplier<Thread> hook,
					final CompletableFuture<?> closed) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField("stopCloseLock");
		field.setAccessible(true);
		final ReentrantLock lock = (ReentrantLock) field.get(client);
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
		while (!closed.isDone() && (hook.get() == null || !lock.hasQueuedThread(hook.get()))) {
			assertTrue(System.nanoTime() < deadline, "the close neither queued nor ended");
			Thread.onSpinWait();
		}
	}

	private static String outcome(final CompletableFuture<?> future) {
		return future.isDone() ? String.valueOf(failure(future)) : "running";
	}

	private static Throwable failure(final CompletableFuture<?> future) {
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		try {
			future.handle((value, error) -> {
				failure.set(error);
				return null;
			}).get(WAIT_SECONDS, TimeUnit.SECONDS);
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
		return failure.get();
	}

	private static <T> CompletableFuture<T> runAsync(final String name, final Callable<T> task) {
		final CompletableFuture<T> future = new CompletableFuture<>();
		Thread.ofPlatform().name(name).daemon(true).start(() -> {
			try {
				future.complete(task.call());
			} catch (final Throwable e) {
				future.completeExceptionally(e);
			}
		});
		return future;
	}
}
