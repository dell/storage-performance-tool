package com.dell.spt.base.load.step.client.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class MetricsSnapshotsSupplierTaskImplTest {

	private static final String FETCH_FAILED = "Failed to fetch the metrics snapshots from";
	/** How long a count waits for the asynchronous loggers to deliver what was logged before it. */
	private static final long LOG_DRAIN_MILLIS = 10_000;

	/** Names this test's slices, so that a count leaves out the lines of another test's. */
	private final String marker = "fetch-test-" + System.nanoTime();
	private final AtomicInteger steps = new AtomicInteger();

	@Test
	void fetchFailureInvalidatesPreviouslyCachedSnapshots() throws Exception {
		final LoadStep step = step();
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		doReturn(fresh)
						.doThrow(new IllegalStateException("worker unavailable"))
						.when(step).metricsSnapshots();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(
						ServiceTaskExecutor.TASK_EXECUTOR, step);

		try {
			assertSame(fresh, supplier.get());
			assertNull(supplier.get(), "a failed refresh must not publish stale snapshots");
		} finally {
			supplier.close();
		}
	}

	@Test
	void aSliceTheRecorderLosesGetsNoFurtherFetch() throws Exception {
		final LoadStep step = step();
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		final RemoteException refused = new ConnectException("injected: connection refused");
		doReturn(fresh).doThrow(refused).when(step).metricsSnapshots();
		final List<RemoteException> told = Collections.synchronizedList(new ArrayList<>());
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), recorder(failure -> {
			told.add(failure);
			return true;
		}));

		try (var fetchFailures = new FetchFailureCapture(marker)) {
			assertSame(fresh, supplier.get());
			assertNull(supplier.get());
			for (var i = 0; i < 100; i++) {
				assertNull(supplier.get(), "a lost slice publishes no snapshots");
			}
			verify(step, times(2)).metricsSnapshots();
			assertEquals(List.of(refused), told, "the recorder hears of the one failure");
			assertEquals(0, fetchFailures.count(), "the recorder logs the loss, not the fetch");
		} finally {
			supplier.close();
		}
	}

	@Test
	void aSliceTheRecorderKeepsIsFetchedAgainAndItsFailuresLogOnceAtInfo() throws Exception {
		final LoadStep step = step();
		doThrow(new ConnectException("injected: connection refused")).when(step).metricsSnapshots();
		final AtomicInteger told = new AtomicInteger();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), recorder(failure -> {
			told.incrementAndGet();
			return false;
		}));

		try (var fetchFailures = new FetchFailureCapture(marker)) {
			for (var i = 0; i < 50; i++) {
				assertNull(supplier.get());
			}
			verify(step, times(50)).metricsSnapshots();
			assertEquals(50, told.get());
			assertEquals(1, fetchFailures.count(), "one INFO line per slice, not one per fetch");
		} finally {
			supplier.close();
		}
	}

	@Test
	void failedFetchesLogOnceAtInfo() throws Exception {
		final LoadStep step = step();
		doThrow(new ConnectException("injected: connection refused")).when(step).metricsSnapshots();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(ServiceTaskExecutor.TASK_EXECUTOR, step);

		try (var fetchFailures = new FetchFailureCapture(marker)) {
			for (var i = 0; i < 50; i++) {
				assertNull(supplier.get());
			}
			verify(step, times(50)).metricsSnapshots();
			assertEquals(1, fetchFailures.count(), "one INFO line per slice, not one per fetch");
		} finally {
			supplier.close();
		}
	}

	@Test
	void withoutARecorderNoFailureLosesTheSlice() throws Exception {
		final LoadStep step = step();
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		doThrow(new ConnectException("injected: connection refused")).doReturn(fresh).when(step).metricsSnapshots();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig());

		try {
			assertNull(supplier.get());
			assertSame(fresh, supplier.get(), "the next fetch is made, as before");
		} finally {
			supplier.close();
		}
	}

	@Test
	void aSliceLostByAnotherOfTheEntrysCallsGetsNoFurtherFetch() throws Exception {
		final LoadStep step = step();
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		doReturn(fresh).when(step).metricsSnapshots();
		final AtomicBoolean lostAtAwait = new AtomicBoolean();
		final AtomicInteger told = new AtomicInteger();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), new MetricsAggregatorImpl.SliceLoss() {
			@Override
			public boolean isLost(final LoadStep slice) {
				assertSame(step, slice);
				return lostAtAwait.get();
			}

			@Override
			public boolean fetchFailed(final LoadStep slice, final RemoteException failure) {
				told.incrementAndGet();
				return true;
			}
		});

		try {
			assertSame(fresh, supplier.get());
			// The entry loses the slice at its await, stop or close, which answered or failed elsewhere.
			lostAtAwait.set(true);
			assertNull(supplier.get(), "a lost slice publishes no snapshots");
			supplier.start();
			Thread.sleep(300);
			assertNull(supplier.get());
			verify(step, times(1)).metricsSnapshots();
			assertEquals(0, told.get(), "no fetch, so no failure to tell of");
		} finally {
			supplier.close();
		}
	}

	@Test
	void fetchesFailingTogetherLogOnceAtInfo() throws Exception {
		final int rounds = 200;
		try (var fetchFailures = new FetchFailureCapture(marker)) {
			for (var round = 0; round < rounds; round++) {
				final LoadStep step = step();
				final CyclicBarrier together = new CyclicBarrier(2);
				doAnswer(invocation -> {
					together.await(5, TimeUnit.SECONDS);
					throw new ConnectException("injected: connection refused");
				}).when(step).metricsSnapshots();
				final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), recorder(failure -> false));
				try {
					// The background fetch and the step's metrics output fail at the same time.
					final Thread other = Thread.ofPlatform().start(supplier::get);
					assertNull(supplier.get());
					other.join();
				} finally {
					supplier.close();
				}
			}
			assertEquals(rounds, fetchFailures.count(), "one INFO line per slice, also for failures at the same time");
		}
	}

	/** A slice mock that this test's {@link FetchFailureCapture} counts the lines of. */
	private LoadStep step() {
		return mock(LoadStep.class, withSettings().name(marker + "-" + steps.incrementAndGet()));
	}

	/** A record of lost slices that loses none by another route and tells each fetch failure to the predicate. */
	private static MetricsAggregatorImpl.SliceLoss recorder(final Predicate<RemoteException> fetchFailed) {
		return new MetricsAggregatorImpl.SliceLoss() {
			@Override
			public boolean isLost(final LoadStep slice) {
				return false;
			}

			@Override
			public boolean fetchFailed(final LoadStep slice, final RemoteException failure) {
				return fetchFailed.test(failure);
			}
		};
	}

	private static com.github.akurilov.confuse.Config metricsConfig() {
		return TestConfigBuilder.config().configVal("output-metrics");
	}

	/**
	 * Counts the fetch-failure lines at INFO or above on the error logger, for this test's slices
	 * only, while open. The engine's loggers are asynchronous, so a count first logs a line of its own
	 * and waits for it: the loggers deliver in order, so every line logged before it has arrived.
	 */
	private static final class FetchFailureCapture extends AbstractAppender implements AutoCloseable {
		private final Map<String, Integer> linesByStep = new ConcurrentHashMap<>();
		private final Logger logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		private final Level level = logger.getLevel();
		private final String marker;
		private final String drainLine;
		private final AtomicInteger drains = new AtomicInteger();
		private volatile int drained;

		FetchFailureCapture(final String marker) {
			super("fetchFailureCapture", null, null, true, Property.EMPTY_ARRAY);
			this.marker = marker;
			this.drainLine = marker + ": log drained ";
			start();
			logger.addAppender(this);
			logger.setLevel(Level.INFO);
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.startsWith(drainLine)) {
				drained = Math.max(drained, Integer.parseInt(message.substring(drainLine.length())));
			} else if (event.getLevel().isMoreSpecificThan(Level.INFO) && message.startsWith(FETCH_FAILED)) {
				final int at = message.indexOf("\"" + marker + "-");
				if (at >= 0) {
					linesByStep.merge(message.substring(at + 1, message.indexOf('"', at + 1)), 1, Integer::sum);
				}
			}
		}

		/** The lines logged so far, after the loggers have delivered them; fails if they never do. */
		int count() {
			final int drain = drains.incrementAndGet();
			Loggers.ERR.info("{}{}", drainLine, drain);
			final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LOG_DRAIN_MILLIS);
			while (drained < drain) {
				if (System.nanoTime() > deadline) {
					throw new AssertionError("the loggers did not deliver the line logged to drain them");
				}
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			}
			final int lines = linesByStep.values().stream().mapToInt(Integer::intValue).sum();
			assertEquals(linesByStep.size(), lines, "at most one line per slice: " + linesByStep);
			return lines;
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			logger.setLevel(level);
			stop();
		}
	}
}
