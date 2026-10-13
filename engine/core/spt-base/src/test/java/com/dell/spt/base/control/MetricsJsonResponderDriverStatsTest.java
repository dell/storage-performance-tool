package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The {@code driver_stats} object on metrics JSON rows. */
class MetricsJsonResponderDriverStatsTest {

	private static final long RUN_ID = 123L;

	private final List<MetricsContext<?>> contexts = new ArrayList<>();

	@AfterEach
	void closeContexts() {
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void nodeRowCarriesStatisticsOfADriverTheCoreDoesNotKnow() {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = context("stats-step", () -> StatsReportingDriverMock.stats(41, 0, 7));

		final JsonNode row = nodeRow(ctx);

		final JsonNode stats = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(List.of("driver", "counters", "gauges"), fieldNames(stats));
		assertEquals(StatsReportingDriverMock.DRIVER, stats.get("driver").asText());
		assertEquals(
						List.of(StatsReportingDriverMock.COUNTER_SPROCKETS, StatsReportingDriverMock.COUNTER_WIDGETS),
						fieldNames(stats.get("counters")));
		assertEquals(41, stats.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(0, stats.get("counters").get(StatsReportingDriverMock.COUNTER_SPROCKETS).asLong());
		assertEquals(List.of(StatsReportingDriverMock.GAUGE_SHELF), fieldNames(stats.get("gauges")));
		assertEquals(7, stats.get("gauges").get(StatsReportingDriverMock.GAUGE_SHELF).asLong());
	}

	@Test
	void nodeRowKeepsValuesBeyondTheIntegerRange() {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = context(
						"stats-step", () -> StatsReportingDriverMock.stats(Long.MAX_VALUE, 0, 1L << 40));

		final JsonNode stats = nodeRow(ctx).get(MetricsConstants.METRIC_NAME_DRIVER_STATS);

		assertEquals(Long.MAX_VALUE, stats.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(1L << 40, stats.get("gauges").get(StatsReportingDriverMock.GAUGE_SHELF).asLong());
	}

	@Test
	void nodeRowOfADriverWithoutStatisticsIsUnchanged() {
		final JsonNode withoutSupplier = nodeRow(context("plain-step", null));
		final JsonNode reportingNothing = nodeRow(context("plain-step", () -> null));

		assertFalse(withoutSupplier.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
		assertFalse(reportingNothing.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
		assertEquals(fieldNames(withoutSupplier), fieldNames(reportingNothing));
	}

	@SuppressWarnings("unchecked")
	private MetricsContext<AllMetricsSnapshotImpl> context(final String stepId, final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = MetricsContextImpl.builder()
						.loadStepId(stepId)
						.runId(RUN_ID)
						.opType(OpType.CREATE)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(4096))
						.stdOutColorFlag(false)
						.outputPeriodSec(1)
						.actualConcurrencyGauge(() -> 0)
						.build();
		if (stats != null) {
			ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, stats);
		}
		ctx.start();
		contexts.add(ctx);
		return ctx;
	}

	private static JsonNode nodeRow(final MetricsContext<?> ctx) {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of(ctx));
		when(mgr.getTerminalSteps()).thenReturn(List.of());
		final var rows = new MetricsJsonResponder(mgr, config()).buildNodeMetrics(false);
		assertEquals(1, rows.size(), rows.toString());
		return rows.get(0);
	}

	private static List<String> fieldNames(final JsonNode node) {
		final List<String> names = new ArrayList<>();
		node.fieldNames().forEachRemaining(names::add);
		return names;
	}

	private static Config config() {
		final Config config = TestConfigBuilder.config();
		config.val("run-comment", "driver-stats-test-node");
		config.val("run-port", 18080);
		config.val("run-id", RUN_ID);
		config.val("run-node", true);
		config.val("server-metrics-expose_fleet", true);
		return config;
	}
}
