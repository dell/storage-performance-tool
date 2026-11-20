package com.dell.spt.base.load.step.client.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class MetricsSnapshotsSupplierTaskImplTest {

	private static final String FETCH_FAILED = "Failed to fetch the metrics snapshots from";

	@Test
	void fetchFailureInvalidatesPreviouslyCachedSnapshots() throws Exception {
		final LoadStep step = mock(LoadStep.class);
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
		final LoadStep step = mock(LoadStep.class);
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		final RemoteException refused = new ConnectException("injected: connection refused");
		doReturn(fresh).doThrow(refused).when(step).metricsSnapshots();
		final List<RemoteException> told = Collections.synchronizedList(new ArrayList<>());
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), failure -> {
			told.add(failure);
			return true;
		});

		try (var fetchFailures = new FetchFailureCapture()) {
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
		final LoadStep step = mock(LoadStep.class);
		doThrow(new ConnectException("injected: connection refused")).when(step).metricsSnapshots();
		final AtomicInteger told = new AtomicInteger();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(step, metricsConfig(), failure -> {
			told.incrementAndGet();
			return false;
		});

		try (var fetchFailures = new FetchFailureCapture()) {
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
		final LoadStep step = mock(LoadStep.class);
		doThrow(new ConnectException("injected: connection refused")).when(step).metricsSnapshots();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(ServiceTaskExecutor.TASK_EXECUTOR, step);

		try (var fetchFailures = new FetchFailureCapture()) {
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
		final LoadStep step = mock(LoadStep.class);
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

	private static com.github.akurilov.confuse.Config metricsConfig() {
		return TestConfigBuilder.config().configVal("output-metrics");
	}

	/** Counts the fetch-failure lines at INFO or above on the error logger while open. */
	private static final class FetchFailureCapture extends AbstractAppender implements AutoCloseable {
		private final AtomicInteger count = new AtomicInteger();
		private final Logger logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		private final Level level = logger.getLevel();

		FetchFailureCapture() {
			super("fetchFailureCapture", null, null, true, Property.EMPTY_ARRAY);
			start();
			logger.addAppender(this);
			logger.setLevel(Level.INFO);
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getLevel().isMoreSpecificThan(Level.INFO)
							&& event.getMessage().getFormattedMessage().startsWith(FETCH_FAILED)) {
				count.incrementAndGet();
			}
		}

		int count() {
			LogUtil.flushAll();
			return count.get();
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			logger.setLevel(level);
			stop();
		}
	}
}
