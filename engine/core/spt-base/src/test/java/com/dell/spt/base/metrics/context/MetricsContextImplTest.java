package com.dell.spt.base.metrics.context;

import com.github.akurilov.commons.system.SizeInBytes;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.dell.spt.base.metrics.MetricsConstants.*;
import static org.junit.jupiter.api.Assertions.*;

class MetricsContextImplTest {

	private MetricsContext<AllMetricsSnapshotImpl> ctx;
	private AtomicInteger concGauge;

	private static final String STEP_ID = "step-xyz";
	private static final long RUN_ID = 987654321L;
	private static final int CONC_LIMIT = 16;
	private static final int CONC_THRESHOLD = 8;
	private static final boolean STDOUT_COLOR = true;
	private static final int PERIOD_SEC = 1;
	private static final SizeInBytes ITEM_SIZE = new SizeInBytes(8192);
	private static final String COMMENT = "unit test";

	@BeforeEach
	void setUp() {
		concGauge = new AtomicInteger(0);
		ctx = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(RUN_ID)
						.opType(OpType.CREATE)
						.concurrencyLimit(CONC_LIMIT)
						.concurrencyThreshold(CONC_THRESHOLD)
						.itemDataSize(ITEM_SIZE)
						.stdOutColorFlag(STDOUT_COLOR)
						.outputPeriodSec(PERIOD_SEC)
						.comment(COMMENT)
						.actualConcurrencyGauge(concGauge::get)
						.build();
	}

	@Test
	void lifecycleStartAndClose() {
		assertFalse(ctx.isStarted(), "should not be started before start()");
		assertEquals(-1, ctx.startTimeStamp(), "startTimeStamp() should be -1 before start()");

		ctx.start();
		assertTrue(ctx.isStarted(), "should be started after start()");
		long ts = ctx.startTimeStamp();
		assertTrue(ts > 0, "startTimeStamp should be set");
		assertEquals(ts, (long) ctx.metadata().get(MetricsConstants.METADATA_START_TIME));

		long elapsed1 = ctx.elapsedTimeMillis();
		assertTrue(elapsed1 >= 0);
		sleepQuiet(5);
		long elapsed2 = ctx.elapsedTimeMillis();
		assertTrue(elapsed2 >= elapsed1, "elapsed time should increase");

		ctx.close();
		assertFalse(ctx.isStarted(), "after close(), isStarted() should be false");
		assertEquals(-1, ctx.startTimeStamp(), "after close(), startTimeStamp() should be -1");
	}

	@Test
	void metadataAndConfigAreExposedViaGetters() {
		ctx.start();

		assertEquals(STEP_ID, ctx.loadStepId());
		assertEquals(RUN_ID, ctx.runId());
		assertEquals(OpType.CREATE, ctx.opType());
		assertEquals(CONC_LIMIT, ctx.concurrencyLimit());
		assertEquals(CONC_THRESHOLD, ctx.concurrencyThreshold());
		assertEquals(ITEM_SIZE, ctx.itemDataSize());
		assertEquals(STDOUT_COLOR, ctx.stdOutColorEnabled());
		assertEquals(PERIOD_SEC * 1000L, ctx.outputPeriodMillis());
		assertEquals(COMMENT, ctx.comment());

		assertEquals(0L, ctx.lastOutputTs());
		ctx.lastOutputTs(1234L);
		assertEquals(1234L, ctx.lastOutputTs());
	}

	/**
	 * Verifies that the last snapshot is created and refreshed after starting the context.
	 */
	@Test
	void lastSnapshotCreatedAndRefreshes() {
		ctx.start();

		AllMetricsSnapshotImpl s1 = ctx.lastSnapshot();
		assertNotNull(s1, "lastSnapshot() should not be null after start()");

		sleepQuiet(2);
		ctx.refreshLastSnapshot();
		AllMetricsSnapshotImpl s2 = ctx.lastSnapshot();
		assertNotNull(s2);

	}

	/**
	 * Verifies that markSucc and markPartSucc calls do not throw exceptions and supports array overloads.
	 */
	@Test
	void markSuccAndFailCallsDoNotThrowAndSupportArrayOverloads() {
		ctx.start();

		ctx.markSucc(1024L, 2_000L, 1_000L);

		ctx.markPartSucc(2048L, 3_000L, 1_500L);

		ctx.markSucc(3L, 4096L, new long[]{1000, 2000, 3000
		}, new long[]{500, 1000, 1500
		});

		ctx.markPartSucc(
						8192L,
						new long[]{500, 700
						},
						new long[]{200, 400
						});

		ctx.markFail();
		ctx.markFail(5);
		ctx.markCorrupt();

		ctx.refreshLastSnapshot();
		assertNotNull(ctx.lastSnapshot());
		assertEquals(7, ctx.lastSnapshot().failsSnapshot().count());
		assertEquals(1, ctx.lastSnapshot().corruptSnapshot().count());
	}

	@Test
	void ttfbSamplesAreRecordedSeparatelyFromLatencyAndDuration() {
		ctx.start();

		ctx.markSucc(1024L, 2_000L, 500L, 800L);
		ctx.markSucc(1024L, 3_000L, 600L, 0L);

		ctx.refreshLastSnapshot();

		assertEquals(2, ctx.lastSnapshot().durationSnapshot().count());
		assertEquals(2, ctx.lastSnapshot().latencySnapshot().count());
		assertEquals(1, ctx.lastSnapshot().ttfbSnapshot().count());
		assertEquals(800L, ctx.lastSnapshot().ttfbSnapshot().percentile(0.5));
	}

	@Test
	void forcedRefreshWithinSnapshotThrottleWindowPublishesTimingSamples() throws Exception {
		ctx.start();
		ctx.lastSnapshot();
		setLastSnapshotsUpdateTs(ctx, System.currentTimeMillis());

		ctx.markSucc(1024L, 2_000L, 500L, 800L);
		ctx.refreshLastSnapshot(true);

		assertEquals(1, ctx.lastSnapshot().successSnapshot().count());
		assertEquals(1024L, ctx.lastSnapshot().byteSnapshot().count());
		assertEquals(1, ctx.lastSnapshot().durationSnapshot().count());
		assertEquals(1, ctx.lastSnapshot().latencySnapshot().count());
		assertEquals(1, ctx.lastSnapshot().ttfbSnapshot().count());
	}

	@Test
	void ttfbSamplesOutsideDurationAreIgnoredAtMetricsBoundary() {
		ctx.start();

		ctx.markSucc(1024L, 1_000L, 500L, 1_000L);
		ctx.markSucc(1024L, 1_000L, 500L, 1_001L);
		ctx.markSucc(1024L, 1_000L, 500L, 0L);
		ctx.markSucc(1024L, 1_000L, 500L, -1L);

		ctx.refreshLastSnapshot();

		assertEquals(4, ctx.lastSnapshot().durationSnapshot().count());
		assertEquals(4, ctx.lastSnapshot().latencySnapshot().count());
		assertEquals(1, ctx.lastSnapshot().ttfbSnapshot().count());
		assertEquals(1_000L, ctx.lastSnapshot().ttfbSnapshot().percentile(0.5));
	}

	@Test
	void arrayTimingOverloadRecordsOnlyProvidedTtfbSamples() {
		ctx.start();

		ctx.markSucc(
						3L,
						1024L,
						new long[]{1_000L, 2_000L, 3_000L
						},
						new long[]{100L, 200L, 300L
						},
						new long[]{150L, 250L
						});

		ctx.refreshLastSnapshot();

		assertEquals(3, ctx.lastSnapshot().durationSnapshot().count());
		assertEquals(3, ctx.lastSnapshot().latencySnapshot().count());
		assertEquals(2, ctx.lastSnapshot().ttfbSnapshot().count());
	}

	/**
	 * Verifies that non-positive latency is ignored while valid duration samples are still retained.
	 */
	@Test
	void nonPositiveLatencyIsIgnoredWhileDurationIsRecorded() {
		ctx.start();

		ctx.markSucc(1, 1_000L, 0L);
		ctx.markSucc(1, 500L, 600L);

		ctx.refreshLastSnapshot();
		assertEquals(2, ctx.lastSnapshot().durationSnapshot().count());
		assertEquals(1, ctx.lastSnapshot().latencySnapshot().count());
		assertEquals(600L, ctx.lastSnapshot().latencySnapshot().percentile(0.5));
	}

	/**
	 * Verifies that a nested context is created when entering a threshold state with expected metadata.
	 */
	@Test
	void thresholdStateCreatesNestedContextWithExpectedMetadata() {
		ctx.start();
		assertFalse(ctx.thresholdStateEntered());

		ctx.enterThresholdState();
		assertTrue(ctx.thresholdStateEntered(), "enterThresholdState() should mark state entered");

		MetricsContext<AllMetricsSnapshotImpl> nested = ctx.thresholdMetrics();
		assertNotNull(nested);
		assertTrue(nested.isStarted(), "nested context should be started");

		assertEquals(ctx.loadStepId(), nested.loadStepId());
		assertEquals(ctx.runId(), nested.runId());
		assertEquals(ctx.opType(), nested.opType());
		assertEquals(ctx.concurrencyLimit(), nested.concurrencyLimit());
		assertEquals(Integer.MAX_VALUE, nested.concurrencyThreshold(), "child threshold of 0 becomes MAX_INT in base");
		assertEquals(ctx.itemDataSize(), nested.itemDataSize());
		assertEquals(ctx.stdOutColorEnabled(), nested.stdOutColorEnabled());
		assertEquals(ctx.outputPeriodMillis(), nested.outputPeriodMillis());

		ctx.markSucc(128, 2_000, 1_000);
		ctx.markPartSucc(64, 1_500, 700);
		ctx.markFail(2);

		ctx.exitThresholdState();
		assertTrue(ctx.thresholdStateExited(), "exit should flip exited flag");

		assertFalse(nested.isStarted(), "nested should be closed after exit");
	}

	/**
	 * Verifies that equals and toString methods behave as documented.
	 */
	@Test
	void equalsAndtoStringBehaveAsExpected() {
		MetricsContext<AllMetricsSnapshotImpl> a = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(RUN_ID)
						.opType(OpType.CREATE)
						.concurrencyLimit(CONC_LIMIT)
						.concurrencyThreshold(CONC_THRESHOLD)
						.itemDataSize(ITEM_SIZE)
						.stdOutColorFlag(STDOUT_COLOR)
						.outputPeriodSec(PERIOD_SEC)
						.comment(COMMENT)
						.actualConcurrencyGauge(concGauge::get)
						.build();

		MetricsContext<AllMetricsSnapshotImpl> b = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(RUN_ID)
						.opType(OpType.CREATE)
						.concurrencyLimit(CONC_LIMIT)
						.concurrencyThreshold(CONC_THRESHOLD)
						.itemDataSize(ITEM_SIZE)
						.stdOutColorFlag(STDOUT_COLOR)
						.outputPeriodSec(PERIOD_SEC)
						.comment(COMMENT)
						.actualConcurrencyGauge(concGauge::get)
						.build();

		assertNotEquals(a, b, "distinct instances are generally not equal");

		// check self equality
		assertEquals(a, a);

		String s = a.toString();
		assertTrue(s.contains("MetricsContextImpl(") || s.contains("MetricsContextImpl"),
						"toString should include the class simple name");
		assertTrue(s.contains(OpType.CREATE.name()));
		assertTrue(s.contains(String.valueOf(CONC_LIMIT)));
		assertTrue(s.contains(STEP_ID));
	}

	/**
	 * Verifies that the builder allows zero or negative threshold values but
	 * normalizes them to max int in child contexts.
	 */
	@Test
	void builderAllowsZeroOrNegativeThresholdButBaseNormalizesToMaxIntInChild() {
		MetricsContext<AllMetricsSnapshotImpl> zeroThresholdCtx = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(RUN_ID)
						.opType(OpType.READ)
						.concurrencyLimit(CONC_LIMIT)
						.concurrencyThreshold(0)
						.itemDataSize(ITEM_SIZE)
						.stdOutColorFlag(STDOUT_COLOR)
						.outputPeriodSec(PERIOD_SEC)
						.actualConcurrencyGauge(concGauge::get)
						.build();

		zeroThresholdCtx.start();
		zeroThresholdCtx.enterThresholdState();
		assertEquals(Integer.MAX_VALUE, zeroThresholdCtx.thresholdMetrics().concurrencyThreshold());
	}

	private static void sleepQuiet(long millis) {
		try {
			Thread.sleep(millis);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("Sleep interrupted in test helper", e);
		}
	}

	private static void setLastSnapshotsUpdateTs(
					final MetricsContext<AllMetricsSnapshotImpl> ctx,
					final long timestamp)
					throws ReflectiveOperationException {
		final var field = MetricsContextImpl.class.getDeclaredField("lastSnapshotsUpdateTs");
		field.setAccessible(true);
		field.setLong(ctx, timestamp);
	}
}
