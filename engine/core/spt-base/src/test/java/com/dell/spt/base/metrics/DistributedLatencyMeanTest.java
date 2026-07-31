package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Reproducer for the latency mean bug: distributed context reports cumulative sum
 * instead of per-operation average latency.
 *
 * The test simulates one worker local context feeding a distributed (entry) context,
 * records many operations with known latency values, then asserts the distributed
 * snapshot's latencySnapshot().mean() is close to the expected per-op average.
 */
public class DistributedLatencyMeanTest {

	private static final long KNOWN_LATENCY_US = 4000;
	private static final long KNOWN_DURATION_US = 4500;
	private static final long ITEM_BYTES = 1024;

	private MetricsContextImpl<?> workerCtx;
	private DistributedMetricsContextImpl<?> entryCtx;

	@BeforeEach
	void setUp() {
		final SizeInBytes size = new SizeInBytes(ITEM_BYTES);

		workerCtx = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("lat-test")
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 4)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker")
						.runId(1)
						.build();
		workerCtx.start();

		entryCtx = (DistributedMetricsContextImpl<?>) DistributedMetricsContextImpl.builder()
						.loadStepId("lat-test")
						.opType(OpType.CREATE)
						.nodeCountSupplier(() -> 1)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(workerCtx.lastSnapshot()))
						.quantileValues(Arrays.asList(0.25, 0.5, 0.75))
						.nodeAddrs(List.of("127.0.0.1:1099"))
						.comment("entry")
						.runId(1)
						.opCountLimit(0L)
						.timeLimitSec(0L)
						.build();
		entryCtx.start();
	}

	@AfterEach
	void tearDown() {
		if (entryCtx != null)
			entryCtx.close();
		if (workerCtx != null)
			workerCtx.close();
	}

	@Test
	void corruptionCountsAggregateAsFailureSubset() {
		workerCtx.markCorrupt();
		workerCtx.markCorrupt();
		workerCtx.markFail();
		workerCtx.refreshLastSnapshot();
		entryCtx.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snapshot = (DistributedAllMetricsSnapshot) entryCtx.lastSnapshot();
		assertEquals(3, snapshot.failsSnapshot().count());
		assertEquals(2, snapshot.corruptSnapshot().count());
	}

	@Test
	void latencyMeanCorrectWithSmallOpCount() {
		for (int i = 0; i < 100; i++) {
			workerCtx.markSucc(ITEM_BYTES, KNOWN_DURATION_US, KNOWN_LATENCY_US);
		}
		workerCtx.refreshLastSnapshot();
		entryCtx.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) entryCtx.lastSnapshot();
		assertNotNull(snap, "snapshot should not be null");
		assertNotNull(snap.latencySnapshot(), "latency snapshot should not be null");

		final double latMean = snap.latencySnapshot().mean();
		System.out.println("Small op count: latency mean = " + latMean + " µs");
		System.out.println("  latency sum  = " + snap.latencySnapshot().sum());
		System.out.println("  latency count= " + snap.latencySnapshot().count());
		assertEquals(KNOWN_LATENCY_US, latMean, 1.0,
						"Latency mean should equal the known per-op latency");
	}

	@Test
	void latencyMeanCorrectWithLargeOpCount() {
		// Simulate ~1.8M operations (similar to the real test that showed the bug)
		final int opCount = 1_800_000;
		for (int i = 0; i < opCount; i++) {
			workerCtx.markSucc(ITEM_BYTES, KNOWN_DURATION_US, KNOWN_LATENCY_US);
		}
		workerCtx.refreshLastSnapshot();
		entryCtx.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) entryCtx.lastSnapshot();
		assertNotNull(snap, "snapshot should not be null");

		final double latMean = snap.latencySnapshot().mean();
		final long latSum = snap.latencySnapshot().sum();
		final long latCount = snap.latencySnapshot().count();
		System.out.println("Large op count (" + opCount + "):");
		System.out.println("  latency mean  = " + latMean + " µs");
		System.out.println("  latency sum   = " + latSum);
		System.out.println("  latency count = " + latCount);
		System.out.println("  expected mean = " + KNOWN_LATENCY_US);

		assertEquals(opCount, latCount, "latency count should match op count");
		assertEquals(KNOWN_LATENCY_US, latMean, 1.0,
						"Latency mean should equal the known per-op latency, not a cumulative value");
		assertTrue(latMean < 10_000,
						"Latency mean (" + latMean + " µs) should be < 10,000 µs, not billions");
	}

	@Test
	void latencyMeanCorrectWithConcurrentRefresh() throws Exception {
		// Simulate concurrent snapshot refreshes (like MetricsManager + snapshot supplier)
		final int opCount = 500_000;
		final MetricsManager mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);

		// Register the distributed context with the metrics manager
		mgr.register(entryCtx);

		// Feed operations while the metrics manager is running
		for (int i = 0; i < opCount; i++) {
			workerCtx.markSucc(ITEM_BYTES, KNOWN_DURATION_US, KNOWN_LATENCY_US);
			if (i % 10000 == 0) {
				Thread.sleep(1); // allow metrics manager to tick
			}
		}

		workerCtx.refreshLastSnapshot();
		entryCtx.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) entryCtx.lastSnapshot();
		assertNotNull(snap, "snapshot should not be null");

		final double latMean = snap.latencySnapshot().mean();
		final long latSum = snap.latencySnapshot().sum();
		final long latCount = snap.latencySnapshot().count();
		System.out.println("Concurrent refresh (" + opCount + " ops):");
		System.out.println("  latency mean  = " + latMean + " µs");
		System.out.println("  latency sum   = " + latSum);
		System.out.println("  latency count = " + latCount);

		// Allow some slack for concurrency, but mean should be nowhere near billions
		assertTrue(latMean < 100_000,
						"Latency mean (" + latMean + " µs) should be reasonable, not " + latMean);

		mgr.close();
	}

	@Test
	void durationMeanAlsoCorrect() {
		final int opCount = 100_000;
		for (int i = 0; i < opCount; i++) {
			workerCtx.markSucc(ITEM_BYTES, KNOWN_DURATION_US, KNOWN_LATENCY_US);
		}
		workerCtx.refreshLastSnapshot();
		entryCtx.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) entryCtx.lastSnapshot();

		final double durMean = snap.durationSnapshot().mean();
		final double latMean = snap.latencySnapshot().mean();

		System.out.println("Duration mean = " + durMean + " µs (expected " + KNOWN_DURATION_US + ")");
		System.out.println("Latency  mean = " + latMean + " µs (expected " + KNOWN_LATENCY_US + ")");

		assertEquals(KNOWN_DURATION_US, durMean, 1.0, "Duration mean should match");
		assertEquals(KNOWN_LATENCY_US, latMean, 1.0, "Latency mean should match");
	}

	@Test
	void distributedContextMergesTimingHistogramsForPercentiles() {
		final SizeInBytes size = new SizeInBytes(ITEM_BYTES);
		final MetricsContextImpl<?> secondWorker = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("lat-test")
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 4)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker-2")
						.runId(1)
						.build();
		secondWorker.start();
		final DistributedMetricsContextImpl<?> twoWorkerEntry = (DistributedMetricsContextImpl<?>) DistributedMetricsContextImpl.builder()
						.loadStepId("lat-test")
						.opType(OpType.CREATE)
						.nodeCountSupplier(() -> 2)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(workerCtx.lastSnapshot(), secondWorker.lastSnapshot()))
						.quantileValues(Arrays.asList(0.5, 0.9, 0.99))
						.nodeAddrs(List.of("127.0.0.1:1099", "127.0.0.2:1099"))
						.comment("entry")
						.runId(1)
						.build();
		twoWorkerEntry.start();
		try {
			workerCtx.markSucc(ITEM_BYTES, 1000, 100, 150);
			workerCtx.markSucc(ITEM_BYTES, 1100, 200, 250);
			secondWorker.markSucc(ITEM_BYTES, 10_000, 9000, 9500);
			secondWorker.markSucc(ITEM_BYTES, 11_000, 10_000, 10_500);
			workerCtx.refreshLastSnapshot();
			secondWorker.refreshLastSnapshot();
			twoWorkerEntry.refreshLastSnapshot();

			final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) twoWorkerEntry.lastSnapshot();

			assertEquals(4, snap.latencySnapshot().count());
			assertTrue(snap.latencySnapshot().percentile(0.9) >= 9000);
			assertTrue(snap.durationSnapshot().percentile(0.9) >= 10_000);
			assertEquals(4, snap.ttfbSnapshot().count());
			assertTrue(snap.ttfbSnapshot().percentile(0.9) >= 9500);
		} finally {
			twoWorkerEntry.close();
			secondWorker.close();
		}
	}

	@Test
	void distributedContextWeightsPercentilesByHistogramSampleCounts() {
		final SizeInBytes size = new SizeInBytes(ITEM_BYTES);
		final MetricsContextImpl<?> highLatencyWorker = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("lat-weighted")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker-high")
						.runId(1)
						.build();
		final MetricsContextImpl<?> lowLatencyWorker = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("lat-weighted")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker-low")
						.runId(1)
						.build();
		highLatencyWorker.start();
		lowLatencyWorker.start();
		final DistributedMetricsContextImpl<?> entry = (DistributedMetricsContextImpl<?>) DistributedMetricsContextImpl.builder()
						.loadStepId("lat-weighted")
						.opType(OpType.READ)
						.nodeCountSupplier(() -> 2)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(lowLatencyWorker.lastSnapshot(), highLatencyWorker.lastSnapshot()))
						.quantileValues(Arrays.asList(0.5, 0.9, 0.99))
						.nodeAddrs(List.of("127.0.0.1:1099", "127.0.0.2:1099"))
						.comment("entry")
						.runId(1)
						.build();
		entry.start();
		try {
			for (int i = 0; i < 1_900; i++) {
				lowLatencyWorker.markSucc(ITEM_BYTES, 1_000, 100, 150);
			}
			for (int i = 0; i < 100; i++) {
				highLatencyWorker.markSucc(ITEM_BYTES, 20_000, 10_000, 10_500);
			}
			lowLatencyWorker.refreshLastSnapshot();
			highLatencyWorker.refreshLastSnapshot();
			entry.refreshLastSnapshot();

			final DistributedAllMetricsSnapshot snap = (DistributedAllMetricsSnapshot) entry.lastSnapshot();
			assertEquals(2_000, snap.latencySnapshot().count());
			assertTrue(snap.latencySnapshot().percentile(0.9) < 1_000);
			assertTrue(snap.latencySnapshot().percentile(0.99) >= 10_000);
		} finally {
			entry.close();
			highLatencyWorker.close();
			lowLatencyWorker.close();
		}
	}
}
