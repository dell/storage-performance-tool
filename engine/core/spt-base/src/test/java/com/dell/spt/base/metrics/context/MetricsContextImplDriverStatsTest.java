package com.dell.spt.base.metrics.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** How a metrics context collects the statistics of its storage driver. */
class MetricsContextImplDriverStatsTest {

	private static final String STEP_ID = "driver-stats-step";
	private static final String UNAVAILABLE = "storage driver statistics are unavailable";
	/** Far enough ahead that the snapshot throttle stays closed for the whole test. */
	private static final long THROTTLE_CLOSED_MILLIS = TimeUnit.HOURS.toMillis(1);

	private MetricsContext<AllMetricsSnapshotImpl> ctx;
	private CapturingAppender appender;
	private final List<Level> previousLevels = new ArrayList<>();

	@BeforeEach
	void setUp() {
		ctx = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(1L)
						.opType(OpType.CREATE)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(4096))
						.stdOutColorFlag(false)
						.outputPeriodSec(1)
						.actualConcurrencyGauge(() -> 0)
						.build();
		appender = new CapturingAppender();
		appender.start();
		for (final var name : loggerNames()) {
			final var logger = LoggerContext.getContext(false).getLogger(name);
			previousLevels.add(logger.getLevel());
			logger.addAppender(appender);
			logger.setLevel(Level.INFO);
		}
	}

	@AfterEach
	void tearDown() {
		final List<String> names = loggerNames();
		for (int i = 0; i < names.size(); i++) {
			final var logger = LoggerContext.getContext(false).getLogger(names.get(i));
			logger.setLevel(previousLevels.get(i));
			logger.removeAppender(appender);
		}
		appender.stop();
		ctx.close();
	}

	@Test
	void snapshotCarriesWhatTheDriverReports() {
		final DriverStatsSnapshot reported = StatsReportingDriverMock.stats(5, 2, 9);
		register(() -> reported);
		ctx.start();

		ctx.refreshLastSnapshot(true);

		assertSame(reported, ctx.lastSnapshot().driverStats());
	}

	@Test
	void contextWithoutADriverSupplierReportsNothing() {
		ctx.start();

		ctx.refreshLastSnapshot(true);

		assertNull(ctx.lastSnapshot().driverStats());
	}

	@Test
	void driverThatKeepsNoStatisticsReportsNothing() {
		register(() -> null);
		ctx.start();

		ctx.refreshLastSnapshot(true);

		assertNull(ctx.lastSnapshot().driverStats());
	}

	@Test
	void valueOfAnotherTypeUnderTheKeyIsIgnored() {
		ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, (Supplier<Object>) () -> "not statistics");
		ctx.start();
		ctx.refreshLastSnapshot(true);
		assertNull(ctx.lastSnapshot().driverStats());

		ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, Boolean.TRUE);
		ctx.refreshLastSnapshot(true);
		assertNull(ctx.lastSnapshot().driverStats());
	}

	@Test
	void driverIsAskedOnlyWhenTheThrottleOpensOrTheRefreshIsForced() throws Exception {
		final AtomicInteger asked = new AtomicInteger();
		register(() -> StatsReportingDriverMock.stats(asked.incrementAndGet(), 0, 0));
		ctx.start();
		ctx.refreshLastSnapshot(true);
		final int afterStart = asked.get();

		// Throttle closed: any number of refreshes reuses the last value.
		closeThrottle();
		for (int i = 0; i < 1_000; i++) {
			ctx.refreshLastSnapshot();
		}
		assertEquals(afterStart, asked.get(), "asked while the throttle was closed");
		assertEquals(afterStart, widgets(), "a throttled refresh must keep the last statistics");

		// A forced refresh asks once.
		ctx.refreshLastSnapshot(true);
		assertEquals(afterStart + 1, asked.get());
		assertEquals(afterStart + 1, widgets());

		// The throttle opening asks once, and closes again.
		openThrottle();
		ctx.refreshLastSnapshot();
		closeThrottle();
		ctx.refreshLastSnapshot();
		assertEquals(afterStart + 2, asked.get());
		assertEquals(afterStart + 2, widgets());
	}

	@Test
	void failingDriverLeavesTheSnapshotWithoutStatisticsAndWarnsOnce() throws Exception {
		final AtomicInteger asked = new AtomicInteger();
		final boolean[] failing = {true
		};
		register(() -> {
			asked.incrementAndGet();
			if (failing[0]) {
				throw new IllegalStateException("driver cannot answer");
			}
			return StatsReportingDriverMock.stats(1, 0, 0);
		});
		ctx.start();
		ctx.markSucc(1024L, 2_000L, 500L);

		for (int i = 0; i < 5; i++) {
			ctx.refreshLastSnapshot(true);
		}

		assertTrue(asked.get() >= 5);
		assertNull(ctx.lastSnapshot().driverStats());
		assertEquals(1, ctx.lastSnapshot().successSnapshot().count(), "the rest of the snapshot must still refresh");
		final List<String> warnings = appender.messagesContaining(UNAVAILABLE);
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).contains(STEP_ID), warnings.get(0));
		assertTrue(warnings.get(0).contains("driver cannot answer"), warnings.get(0));

		// The driver answers again: statistics return without another record.
		failing[0] = false;
		ctx.refreshLastSnapshot(true);
		assertEquals(1L, widgets());
		assertEquals(1, appender.messagesContaining(UNAVAILABLE).size());
	}

	@Test
	void collectingStatisticsLogsNothing() throws Exception {
		register(() -> StatsReportingDriverMock.stats(1, 2, 3));
		ctx.start();

		for (int i = 0; i < 200; i++) {
			ctx.refreshLastSnapshot(true);
		}

		assertEquals(List.of(), appender.messagesContaining(""));
	}

	@SuppressWarnings("unchecked")
	private void register(final Supplier<DriverStatsSnapshot> supplier) {
		ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, supplier);
	}

	private long widgets() {
		return ctx.lastSnapshot().driverStats().counters().get(StatsReportingDriverMock.COUNTER_WIDGETS);
	}

	private void closeThrottle() throws ReflectiveOperationException {
		setLastSnapshotsUpdateTs(System.currentTimeMillis() + THROTTLE_CLOSED_MILLIS);
	}

	private void openThrottle() throws ReflectiveOperationException {
		setLastSnapshotsUpdateTs(0);
	}

	private void setLastSnapshotsUpdateTs(final long timestamp) throws ReflectiveOperationException {
		final var field = MetricsContextImpl.class.getDeclaredField("lastSnapshotsUpdateTs");
		field.setAccessible(true);
		field.setLong(ctx, timestamp);
	}

	private static List<String> loggerNames() {
		return List.of(Loggers.ERR.getName(), Loggers.MSG.getName());
	}

	/**
	 * Collects the records of the engine's message and error loggers. Those loggers are asynchronous,
	 * so a read first sends a marker through each and waits for it.
	 */
	private static final class CapturingAppender extends AbstractAppender {
		private static final long MARKER_WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final AtomicInteger markers = new AtomicInteger();

		CapturingAppender() {
			super("testDriverStatsCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			captured.add(event.getMessage().getFormattedMessage());
		}

		List<String> messagesContaining(final String text) throws InterruptedException {
			final String marker = "driver-stats-test-marker-" + markers.incrementAndGet() + "-";
			Loggers.ERR.warn(marker + "err");
			Loggers.MSG.warn(marker + "msg");
			final long deadline = System.nanoTime() + MARKER_WAIT_NANOS;
			while (!(captured.contains(marker + "err") && captured.contains(marker + "msg"))) {
				assertFalse(System.nanoTime() > deadline, "log markers did not arrive: " + captured);
				Thread.sleep(5);
			}
			final List<String> matching = new ArrayList<>();
			synchronized (captured) {
				for (final String message : captured) {
					if (!message.startsWith("driver-stats-test-marker-") && message.contains(text)) {
						matching.add(message);
					}
				}
			}
			return matching;
		}
	}
}
