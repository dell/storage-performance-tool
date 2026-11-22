package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregator;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A step slice whose shutdown fails, as a local slice whose context's drain or storage driver shutdown
 * throws, does not cost the step its close; and a stop stage that fails does not cost the stop its
 * metrics output. The slices are mocks: slice 0 stands for the entry's own, slices 1 and 2 for two
 * workers'.
 */
@Timeout(60)
class StopFailureStillClosesTest {

	private final Config config = TestConfigBuilder.config();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private final List<LoadStep> slices = new ArrayList<>();
	private final List<TestLoadStepClient> clients = new ArrayList<>();

	@BeforeEach
	void configure() {
		config.val("load-op-type", "noop");
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
	}

	/**
	 * Disarms the failures a test injected and closes each client it made. A client left registered
	 * whose close throws an InterruptedException fails the {@code DaemonBase.closeAll()} of a later test
	 * class in the same JVM.
	 */
	@AfterEach
	void closeClients() throws Exception {
		Thread.interrupted();
		try {
			for (var i = 0; i < slices.size(); i++) {
				reset(slices.get(i));
				stubName(slices.get(i), i);
			}
			for (final TestLoadStepClient client : clients) {
				list(client, "itemTimingMetricsOutputFileAggregators").clear();
				list(client, "operationLifecycleAggregators").clear();
				list(client, "itemOutputFileAggregators").clear();
				client.close();
				assertTrue(client.isClosed(), "the test's client is closed after it");
			}
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void aSliceWhoseShutdownThrowsStillLetsTheStepStopAndCloseEverySlice() throws Exception {
		final TestLoadStepClient client = clientWithThreeSlices();
		doThrow(new IllegalStateException("injected: the context's driver shutdown failed"))
						.when(slices.get(1)).shutdown();
		final AtomicInteger timingCloses = new AtomicInteger();
		final AtomicInteger lifecycleCloses = new AtomicInteger();
		final AtomicInteger itemOutputCloses = new AtomicInteger();
		list(client, "itemTimingMetricsOutputFileAggregators").add((AutoCloseable) timingCloses::incrementAndGet);
		list(client, "operationLifecycleAggregators").add((AutoCloseable) lifecycleCloses::incrementAndGet);
		list(client, "itemOutputFileAggregators").add((AutoCloseable) itemOutputCloses::incrementAndGet);

		client.close();

		for (final LoadStep slice : slices) {
			verify(slice, times(1)).shutdown();
			verify(slice, times(1)).close();
		}
		verify(slices.get(0), times(1)).stop();
		verify(slices.get(2), times(1)).stop();
		assertEquals(1, timingCloses.get(), "the timing aggregator is closed");
		assertEquals(1, lifecycleCloses.get(), "the operation lifecycle aggregator is closed");
		assertEquals(1, itemOutputCloses.get(), "the item output aggregator is closed");
		assertTrue(client.isClosed(), "the step ends closed");
	}

	@Test
	void anErrorFromASlicesShutdownStillPropagates() throws Exception {
		final TestLoadStepClient client = clientWithThreeSlices();
		final AssertionError error = new AssertionError("injected");
		doThrow(error).when(slices.get(1)).shutdown();

		assertSame(error, assertThrows(AssertionError.class, client::doShutdown));
	}

	@Test
	void anInterruptFromASlicesShutdownStillPropagates() throws Exception {
		final TestLoadStepClient client = clientWithThreeSlices();
		doAnswer(invocation -> {
			throw new InterruptedException("injected");
		}).when(slices.get(1)).shutdown();

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::doShutdown));
	}

	@Test
	void anInterruptedSliceStopEndsTheStopBeforeItsOutputAndTheClosesStopWritesIt() throws Exception {
		final TestLoadStepClient client = clientWithThreeSlices();
		final MetricsContext<?> context = mock(MetricsContext.class);
		client.addMetricsContextForTest(context);
		final AtomicInteger sliceStops = new AtomicInteger();
		doAnswer(invocation -> {
			if (sliceStops.incrementAndGet() == 1) {
				throw new InterruptedException("injected: stopped");
			}
			return invocation.getMock();
		}).when(slices.get(1)).stop();
		final AtomicInteger timingCloses = new AtomicInteger();
		list(client, "itemTimingMetricsOutputFileAggregators").add((AutoCloseable) timingCloses::incrementAndGet);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::close));

		assertFalse(Thread.interrupted(), "the thrown InterruptedException carries the stop, as before");
		assertEquals(0, timingCloses.get(), "no output stage runs before the slices have stopped");
		verify(metricsMgr, never()).unregister(context);
		assertFalse(client.isClosed());

		client.close();

		assertEquals(1, timingCloses.get(), "the close's stop closes the timing aggregator");
		verify(metricsMgr, times(1)).unregister(context);
		assertTrue(client.isClosed(), "the step ends closed");
	}

	@Test
	void aFailedStopStageStillWritesTheMetricsOutputAndThrowsTheFirstFailure() throws Exception {
		final TestLoadStepClient client = clientWithThreeSlices();
		final MetricsContext<?> context = mock(MetricsContext.class);
		client.addMetricsContextForTest(context);
		final IllegalStateException first = new IllegalStateException("injected: metrics aggregator stop");
		final MetricsAggregator aggregator = mock(MetricsAggregator.class);
		doThrow(first).when(aggregator).stop();
		final Field field = LoadStepClientBase.class.getDeclaredField("metricsAggregator");
		field.setAccessible(true);
		field.set(client, aggregator);
		final InterruptedException later = new InterruptedException("injected: interrupted");
		final AtomicInteger timingCloses = new AtomicInteger();
		list(client, "itemTimingMetricsOutputFileAggregators").add((AutoCloseable) () -> {
			timingCloses.incrementAndGet();
			throw later;
		});

		final RuntimeException stop = assertThrows(RuntimeException.class, client::doStop);
		field.set(client, null);

		assertSame(first, stop);
		assertEquals(List.of(later), List.of(first.getSuppressed()));
		assertEquals(1, timingCloses.get(), "the stage after the failed one still runs");
		verify(metricsMgr, times(1)).unregister(context);
		assertTrue(Thread.interrupted(), "the stop request under the first failure sets the interrupt flag");
	}

	@Test
	void anInterruptedTimingAggregatorCloseStillWritesTheMetricsOutput() throws Exception {
		// Unlike an interrupted slice stop, the slices have stopped here.
		final TestLoadStepClient client = clientWithThreeSlices();
		final MetricsContext<?> context = mock(MetricsContext.class);
		client.addMetricsContextForTest(context);
		list(client, "itemTimingMetricsOutputFileAggregators").add((AutoCloseable) () -> {
			throw new InterruptedException("injected: interrupted");
		});

		final Throwable stop = assertThrows(Throwable.class, client::doStop);

		assertTrue(stop instanceof InterruptedException, String.valueOf(stop));
		verify(metricsMgr, times(1)).unregister(context);
		assertFalse(Thread.interrupted(), "the thrown InterruptedException carries the stop itself");
	}

	@Test
	void aLaterStageFailureIsSuppressedUnderTheFirstAndNeverUnderItself() {
		final IllegalStateException first = new IllegalStateException("first");
		final IllegalStateException second = new IllegalStateException("second");

		final var failures = new LoadStepClientBase.StopFailures();
		failures.run(() -> {
			throw first;
		});
		failures.run(() -> {});
		failures.run(() -> {
			throw first;
		});
		failures.run(() -> {
			throw second;
		});

		assertSame(first, failures.first());
		assertEquals(List.of(second), List.of(first.getSuppressed()));
		assertSame(first, assertThrows(IllegalStateException.class, failures::throwFirst));
		assertFalse(Thread.interrupted(), "no stop request was collected");
	}

	private TestLoadStepClient clientWithThreeSlices() throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(), List.of(), metricsMgr);
		clients.add(client);
		for (var i = 0; i < 3; i++) {
			final LoadStep slice = mock(LoadStep.class);
			stubName(slice, slices.size());
			slices.add(slice);
		}
		list(client, "stepSlices").addAll(slices);
		final List<Object> addrs = list(client, "stepSliceNodeAddrs");
		addrs.add(null);
		addrs.add("127.0.0.1:1");
		addrs.add("127.0.0.1:2");
		final Field started = LoadStepClientBase.class.getDeclaredField("startedSliceCount");
		started.setAccessible(true);
		started.setInt(client, 3);
		return client;
	}

	private static void stubName(final LoadStep slice, final int index) throws Exception {
		final String name = "slice-" + index;
		when(slice.loadStepId()).thenReturn(name);
		when(slice.toString()).thenReturn(name);
	}

	@SuppressWarnings("unchecked")
	private static List<Object> list(final LoadStepClientBase<?> client, final String name) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField(name);
		field.setAccessible(true);
		return (List<Object>) field.get(client);
	}
}
