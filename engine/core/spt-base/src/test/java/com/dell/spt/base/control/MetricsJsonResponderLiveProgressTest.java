package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Live {@code /metrics/json} rows of a step with several metrics contexts (mixed, weighted and
 * pipeline steps) report each context's own progress, with the manager's real progress cache.
 */
class MetricsJsonResponderLiveProgressTest {

	private final Set<MetricsContext> contexts = new LinkedHashSet<>();
	private final Set<DistributedMetricsContext> distributed = new LinkedHashSet<>();
	private final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
		@Override
		public Set<MetricsContext> getAllContexts() {
			return contexts;
		}

		@Override
		public Set<DistributedMetricsContext> getDistributedContexts() {
			return distributed;
		}
	};

	@AfterEach
	void tearDown() throws Exception {
		mgr.close();
	}

	@Test
	void mixedStepRowsReportTheirOwnCountsBytesAndTiming() throws Exception {
		final Live read = local("mix-4", OpType.READ, Map.of());
		final Live create = local("mix-4", OpType.CREATE, Map.of());
		final Live delete = local("mix-4", OpType.DELETE, Map.of());
		final Live stat = local("mix-4", OpType.STAT, Map.of());
		read.set(snapshot(10_000, 0, 40_960_000, 5_000, List.of(190L, 200L, 210L), List.of(210L, 220L, 230L)));
		create.set(snapshot(200, 0, 819_200, 5_000, List.of(4_800L, 5_000L, 5_200L), List.of(5_000L, 5_200L, 5_400L)));
		delete.set(snapshot(180, 0, 0, 5_000, List.of(140L, 150L, 160L), List.of(150L, 160L, 170L)));
		stat.set(snapshot(0, 190, 0, 5_000, List.of(), List.of()));
		recordAll();

		final ArrayNode rows = responder().buildNodeMetrics(true);
		assertRow(row(rows, "mix-4", "READ"), 10_000, 0, 40_960_000, 200.0);
		assertRow(row(rows, "mix-4", "CREATE"), 200, 0, 819_200, 5_000.0);
		assertRow(row(rows, "mix-4", "DELETE"), 180, 0, 0, 150.0);
		assertRow(row(rows, "mix-4", "STAT"), 0, 190, 0, 0.0);
		for (final JsonNode row : rows) {
			assertFalse(row.has("diag_cached_progress"), "fresh row must not report cached progress: " + row);
			assertMeansWithinRange(row);
		}
	}

	@Test
	void weightedStepContextsOfOneOperationTypeKeepTheirOwnCounts() throws Exception {
		final Live big = local("w-same-op", OpType.READ, Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 6_000L));
		final Live small = local("w-same-op", OpType.READ, Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 100L));
		big.set(snapshot(3_000, 0, 3_072_000, 2_000, List.of(300L), List.of(320L)));
		small.set(snapshot(60, 0, 3_932_160, 2_000, List.of(900L), List.of(950L)));
		recordAll();

		final ArrayNode rows = responder().buildNodeMetrics(false);
		final JsonNode bigRow = rowWithLimit(rows, 6_000L);
		final JsonNode smallRow = rowWithLimit(rows, 100L);
		assertRow(bigRow, 3_000, 0, 3_072_000, 300.0);
		assertRow(smallRow, 60, 0, 3_932_160, 900.0);
		assertEquals(60, smallRow.get(MetricsConstants.METRIC_NAME_COMPLETION).asInt());
	}

	@Test
	void overallCompletionSumsEachContextsOwnCounts() throws Exception {
		final Map<String, Object> limit = Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 1_000L);
		final Live first = local("w-count", OpType.CREATE, limit);
		final Live second = local("w-count", OpType.READ, limit);
		first.set(snapshot(500, 0, 512_000, 3_000, List.of(100L), List.of(110L)));
		second.set(snapshot(100, 0, 102_400, 3_000, List.of(100L), List.of(110L)));
		recordAll();

		final ArrayNode rows = responder().buildNodeMetrics(false);
		// (500 + 100) / (1000 + 1000)
		assertEquals(30, row(rows, "w-count", "CREATE").get("overall_completion_percent").asInt());
		assertEquals(30, row(rows, "w-count", "READ").get("overall_completion_percent").asInt());
	}

	@Test
	void freshSnapshotKeepsItsOwnTimingMeans() throws Exception {
		final Live ctx = local("fresh-lat", OpType.READ, Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 1_000L));
		ctx.set(snapshot(100, 0, 409_600, 1_000, List.of(400L, 500L, 600L), List.of(450L, 550L, 650L)));
		recordAll();
		ctx.set(snapshot(200, 0, 819_200, 2_000, List.of(900L, 1_000L, 1_100L), List.of(1_000L, 1_100L, 1_200L)));

		final JsonNode row = responder().buildNodeMetrics(true).get(0);
		assertRow(row, 200, 0, 819_200, 1_000.0);
		assertEquals(1_100.0, row.get("timing").get("duration_mean_us").asDouble(), 1e-9);
		assertFalse(row.has("diag_cached_progress"), row.toString());
		assertMeansWithinRange(row);
	}

	@Test
	void regressedSnapshotRestoresItsOwnCachedProgressNotASiblings() throws Exception {
		final Map<String, Object> limit = Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 500L);
		final Live own = local("regress-own", OpType.CREATE, limit);
		final Live sibling = local("regress-own", OpType.READ, limit);
		own.set(snapshot(500, 2, 2_048_000, 3_100, List.of(300L, 400L, 500L), List.of(350L, 450L, 550L)));
		sibling.set(snapshot(5_000, 0, 20_480_000, 3_100, List.of(100L), List.of(110L)));
		recordAll();
		own.set(snapshot(0, 0, 0, 0, List.of(), List.of()));

		final JsonNode row = row(responder().buildNodeMetrics(true), "regress-own", "CREATE");
		assertRow(row, 500, 2, 2_048_000, 400.0);
		assertEquals(450.0, row.get("timing").get("duration_mean_us").asDouble(), 1e-9);
		assertEquals(3.1, row.get("elapsed_time_seconds").asDouble(), 1e-9);
		assertTrue(row.get("diag_cached_progress").asBoolean());
		assertEquals(3L, row.get("timing").get("latency").get("count").asLong());
		assertMeansWithinRange(row);
	}

	@Test
	void sameCountsWithoutTimingFallBackToTheCachedTiming() throws Exception {
		final Live ctx = local("zero-timing", OpType.READ, Map.of());
		ctx.set(snapshot(100, 0, 409_600, 1_000, List.of(200L, 250L, 300L), List.of(220L, 270L, 320L)));
		recordAll();
		ctx.set(snapshot(100, 0, 409_600, 1_000, List.of(), List.of()));

		final JsonNode row = responder().buildNodeMetrics(true).get(0);
		assertRow(row, 100, 0, 409_600, 250.0);
		assertTrue(row.get("diag_cached_progress").asBoolean());
		assertMeansWithinRange(row);
	}

	@Test
	void fleetRowsOfAMixedStepReportTheirOwnProgressAndAPartialRegressionKeepsItsOwnRange() throws Exception {
		final Map<String, Object> limit = Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 20_000L);
		final Live read = fleet("fleet-mix", OpType.READ, limit);
		final Live create = fleet("fleet-mix", OpType.CREATE, limit);
		read.set(fleetSnapshot(10_000, 0, 40_960_000, 5_000, List.of(190L, 200L, 210L), List.of(210L, 220L, 230L)));
		create.set(fleetSnapshot(200, 0, 819_200, 5_000, List.of(4_800L, 5_000L, 5_200L), List.of(4_900L, 5_200L, 5_500L)));
		recordAll();
		// One contributor missing: fewer operations and a narrower range than the cached read.
		create.set(fleetSnapshot(150, 0, 614_400, 5_000, List.of(4_800L, 4_900L), List.of(4_900L, 4_950L)));

		final ArrayNode rows = responder().buildClusterMetrics(true);
		final JsonNode readRow = row(rows, "fleet-mix", "READ");
		final JsonNode createRow = row(rows, "fleet-mix", "CREATE");
		assertRow(readRow, 10_000, 0, 40_960_000, 200.0);
		assertFalse(readRow.has("diag_cached_progress"), readRow.toString());
		assertRow(createRow, 200, 0, 819_200, 5_000.0);
		assertTrue(createRow.get("diag_cached_progress").asBoolean());
		assertMeansWithinRange(readRow);
		assertMeansWithinRange(createRow);
		// (10000 + 200) / (20000 + 20000)
		assertEquals(26, createRow.get("overall_completion_percent").asInt());
	}

	private Live local(final String stepId, final OpType opType, final Map<String, Object> metadata) {
		final Live live = new Live(mock(MetricsContext.class));
		stub(live, stepId, opType, metadata);
		contexts.add(live.ctx);
		return live;
	}

	private Live fleet(final String stepId, final OpType opType, final Map<String, Object> metadata) {
		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		final Live live = new Live(ctx);
		stub(live, stepId, opType, metadata);
		when(ctx.nodeCount()).thenReturn(2);
		when(ctx.nodeAddrs()).thenReturn(List.of("entry", "worker-1"));
		when(ctx.nodesPresent()).thenReturn(List.of("entry", "worker-1"));
		contexts.add(ctx);
		distributed.add(ctx);
		return live;
	}

	@SuppressWarnings("unchecked")
	private static void stub(final Live live, final String stepId, final OpType opType, final Map<String, Object> metadata) {
		when(live.ctx.loadStepId()).thenReturn(stepId);
		when(live.ctx.opType()).thenReturn(opType);
		when(live.ctx.metadata()).thenReturn(metadata);
		when(live.ctx.runId()).thenReturn(777L);
		when(live.ctx.lastSnapshot()).thenAnswer(invocation -> live.snapshot.get());
	}

	/** One sweep of the metrics manager over every context, as its task would do. */
	private void recordAll() throws Exception {
		final Method record = MetricsManagerImpl.class.getDeclaredMethod(
						"recordProgressSnapshot", MetricsContext.class, AllMetricsSnapshot.class);
		record.setAccessible(true);
		for (final MetricsContext ctx : contexts) {
			record.invoke(mgr, ctx, ctx.lastSnapshot());
		}
	}

	private MetricsJsonResponder responder() {
		final Config config = TestConfigBuilder.config();
		config.val("run-comment", "probe-node");
		config.val("run-port", 18080);
		config.val("run-id", 777L);
		config.val("run-node", true);
		config.val("server-metrics-expose_fleet", true);
		return new MetricsJsonResponder(mgr, config);
	}

	private static AllMetricsSnapshot snapshot(
					final long success,
					final long fails,
					final long bytes,
					final long elapsedMillis,
					final List<Long> latencies,
					final List<Long> durations) {
		return fill(mock(AllMetricsSnapshot.class), success, fails, bytes, elapsedMillis, latencies, durations);
	}

	private static DistributedAllMetricsSnapshot fleetSnapshot(
					final long success,
					final long fails,
					final long bytes,
					final long elapsedMillis,
					final List<Long> latencies,
					final List<Long> durations) {
		return fill(mock(DistributedAllMetricsSnapshot.class), success, fails, bytes, elapsedMillis, latencies, durations);
	}

	private static <S extends AllMetricsSnapshot> S fill(
					final S snapshot,
					final long success,
					final long fails,
					final long bytes,
					final long elapsedMillis,
					final List<Long> latencies,
					final List<Long> durations) {
		final RateMetricSnapshot successSnapshot = rate(success);
		final RateMetricSnapshot failsSnapshot = rate(fails);
		final RateMetricSnapshot bytesSnapshot = rate(bytes);
		final TimingMetricSnapshot latency = TimingMetricSnapshotImpl.fromSamples("latency", latencies);
		final TimingMetricSnapshot duration = TimingMetricSnapshotImpl.fromSamples("duration", durations);
		final ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(snapshot.successSnapshot()).thenReturn(successSnapshot);
		when(snapshot.failsSnapshot()).thenReturn(failsSnapshot);
		when(snapshot.byteSnapshot()).thenReturn(bytesSnapshot);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.concurrencySnapshot()).thenReturn(conc);
		when(snapshot.elapsedTimeMillis()).thenReturn(elapsedMillis);
		return snapshot;
	}

	private static RateMetricSnapshot rate(final long count) {
		final RateMetricSnapshot rate = mock(RateMetricSnapshot.class);
		when(rate.count()).thenReturn(count);
		return rate;
	}

	private static JsonNode row(final ArrayNode rows, final String stepId, final String opType) {
		for (final JsonNode row : rows) {
			if (stepId.equals(row.get("step_id").asText()) && opType.equals(row.get("op_type").asText())) {
				return row;
			}
		}
		throw new AssertionError("no " + opType + " row of " + stepId + " in " + rows);
	}

	private static JsonNode rowWithLimit(final ArrayNode rows, final long opCount) {
		for (final JsonNode row : rows) {
			if (row.path("limit").path("op_count").asLong() == opCount) {
				return row;
			}
		}
		throw new AssertionError("no row with limit " + opCount + " in " + rows);
	}

	private static void assertRow(
					final JsonNode row, final long success, final long fails, final long bytes, final double latencyMean) {
		assertEquals(success, row.get("operations").get("success_count").asLong(), row.toString());
		assertEquals(fails, row.get("operations").get("failed_count").asLong(), row.toString());
		assertEquals(bytes, row.get("bandwidth").get("bytes_total").asLong(), row.toString());
		assertEquals(latencyMean, row.get("timing").get("latency_mean_us").asDouble(), 1e-9, row.toString());
	}

	private static void assertMeansWithinRange(final JsonNode row) {
		for (final String kind : List.of("latency", "duration")) {
			final JsonNode timing = row.get("timing").get(kind);
			if (timing.get("count").asLong() == 0L) {
				assertEquals(0.0, timing.get("mean_us").asDouble(), 1e-9, kind + " of " + row);
				continue;
			}
			final double mean = timing.get("mean_us").asDouble();
			assertTrue(
							mean >= timing.get("min_us").asDouble() && mean <= timing.get("max_us").asDouble(),
							kind + " mean outside [min, max]: " + row);
			assertEquals(mean, row.get("timing").get(kind + "_mean_us").asDouble(), 1e-9, row.toString());
		}
	}

	/** A metrics context whose snapshot a test replaces between sweeps. */
	private static final class Live {
		final MetricsContext ctx;
		final AtomicReference<AllMetricsSnapshot> snapshot = new AtomicReference<>();

		Live(final MetricsContext ctx) {
			this.ctx = ctx;
		}

		void set(final AllMetricsSnapshot next) {
			snapshot.set(next);
		}
	}
}
