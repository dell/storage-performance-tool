package com.dell.spt.base.metrics.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The Entry Node's steady aggregate travels in its snapshot and is not recomputed needlessly. */
class DistributedMetricsContextSteadyTest {

	@Test
	void aggregateAppearsOnceASliceClosesAndIsReusedWhileInputsAreUnchanged() {
		final AtomicReference<List<AllMetricsSnapshot>> supplied = new AtomicReference<>(
						List.of(slice(null), slice(null)));
		final DistributedMetricsContextImpl<?> ctx = DistributedMetricsContextImpl.builder()
						.loadStepId("steady-dist").runId(1L).opType(OpType.CREATE)
						.nodeCountSupplier(() -> 2).concurrencyLimit(4).concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1024)).outputPeriodSec(0).stdOutColorFlag(false)
						.avgPersistFlag(false).sumPersistFlag(false).timingPersistFlag(false)
						.snapshotsSupplier(supplied::get)
						.quantileValues(List.of(0.5)).nodeAddrs(List.of("a:1099", "b:1099"))
						.build();
		ctx.refreshLastSnapshot(true);
		assertNull(ctx.steadyResult(), "no slice has closed its window");

		final SteadyResult a = valid(100);
		supplied.set(List.of(slice(a), slice(null)));
		ctx.refreshLastSnapshot(true);
		assertEquals(SteadyStatus.INCOMPLETE, ctx.steadyResult().status(), "one slice is still open");

		supplied.set(List.of(slice(a), slice(valid(200))));
		ctx.refreshLastSnapshot(true);
		final SteadyResult first = ctx.steadyResult();
		assertEquals(SteadyStatus.VALID, first.status());
		assertEquals(300, first.statistics().successSnapshot().count());

		// A later RMI read deserializes equal copies: the aggregate is reused, not recomputed.
		supplied.set(List.of(slice(valid(100)), slice(valid(200))));
		ctx.refreshLastSnapshot(true);
		assertSame(first, ctx.steadyResult());

		supplied.set(List.of(slice(valid(100)), slice(valid(250))));
		ctx.refreshLastSnapshot(true);
		assertNotSame(first, ctx.steadyResult());
		assertEquals(350, ctx.steadyResult().statistics().successSnapshot().count());
	}

	private static SteadyResult valid(final long succeeded) {
		return new SteadyResult(
						SteadyStatus.VALID, "shutdown", 1_000, 30_000_000_000L, succeeded / 4.0, 0.5, 0.02, 10, 20,
						snapshot(succeeded, 30_000, null));
	}

	private static AllMetricsSnapshot slice(final SteadyResult steady) {
		return snapshot(500, 32_000, steady);
	}

	private static AllMetricsSnapshot snapshot(final long succeeded, final long elapsedMillis, final SteadyResult steady) {
		final List<Long> durations = new ArrayList<>();
		for (long i = 0; i < Math.min(succeeded, 50); i++) {
			durations.add(1_000L + i);
		}
		return new AllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, durations),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, durations),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 4, 4.0),
						new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_FAIL, 0, elapsedMillis),
						new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedMillis),
						new RateMetricSnapshotImpl(10, 10, MetricsConstants.METRIC_NAME_SUCC, succeeded, elapsedMillis),
						new RateMetricSnapshotImpl(10, 10, MetricsConstants.METRIC_NAME_BYTE, succeeded * 1024, elapsedMillis),
						elapsedMillis,
						null,
						steady);
	}
}
