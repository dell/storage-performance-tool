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
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DriverStatsFleet;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The {@code driver_stats} object on metrics JSON rows. */
class MetricsJsonResponderDriverStatsTest {

	private static final long RUN_ID = 123L;
	private static final String OTHER_DRIVER = "other-mock";

	private final List<MetricsContext<?>> contexts = new ArrayList<>();

	@AfterEach
	void closeContexts() {
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void nodeRowCarriesStatisticsOfADriverTheCoreDoesNotKnow() throws Exception {
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
		assertSchema5Accepts(row);
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

	@Test
	void terminalNodeRowCarriesTheStatisticsTheStepKept() {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = context("stats-step", () -> StatsReportingDriverMock.stats(41, 0, 7));
		ctx.markSucc(4096, 1_000, 100);
		final JsonNode live = nodeRow(ctx).get(MetricsConstants.METRIC_NAME_DRIVER_STATS);

		final JsonNode row = onlyRow(finished(ctx));

		assertTrue(row.get("terminal").asBoolean());
		assertEquals(live, row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS), "the same object as on the live row");
		assertEquals(List.of("driver", "counters", "gauges"), fieldNames(row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS)));
	}

	@Test
	void terminalNodeRowOfAStepInWhichNothingCompletedCarriesTheStatistics() {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = context("stats-step", () -> StatsReportingDriverMock.stats(2, 5, 7));

		final JsonNode row = onlyRow(finished(ctx));

		assertTrue(row.get("terminal").asBoolean());
		assertEquals(0, row.get("operations").get("success_count").asLong());
		final JsonNode counters = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS).get("counters");
		assertEquals(2, counters.get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(5, counters.get(StatsReportingDriverMock.COUNTER_SPROCKETS).asLong());
	}

	@Test
	void terminalNodeRowOfADriverWithoutStatisticsIsUnchanged() {
		final MetricsContext<AllMetricsSnapshotImpl> withoutSupplier = context("plain-step", null);
		final MetricsContext<AllMetricsSnapshotImpl> reportingNothing = context("plain-step", () -> null);
		withoutSupplier.markSucc(4096, 1_000, 100);
		reportingNothing.markSucc(4096, 1_000, 100);

		final JsonNode first = onlyRow(finished(withoutSupplier));
		final JsonNode second = onlyRow(finished(reportingNothing));

		assertTrue(first.get("terminal").asBoolean());
		assertFalse(first.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
		assertFalse(second.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
		assertEquals(fieldNames(first), fieldNames(second));
	}

	@Test
	void terminalStatisticsStayReadableForTheLingerTimeAndGoWithTheRow() throws InterruptedException {
		final MetricsContext<AllMetricsSnapshotImpl> ctx = context("stats-step", () -> StatsReportingDriverMock.stats(41, 0, 7));
		final MetricsManagerImpl mgr = finished(ctx);
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, config());

		final JsonNode first = onlyRow(responder.buildNodeMetrics(false)).get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		final JsonNode again = onlyRow(responder.buildNodeMetrics(false)).get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(41, first.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(first, again);

		// The linger time (api.linger.sec) has passed.
		mgr.setTerminalRetentionMillis(0);
		Thread.sleep(5);

		final JsonNode idle = onlyRow(responder.buildNodeMetrics(false));
		assertFalse(idle.has("terminal"));
		assertFalse(idle.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
	}

	@Test
	void fleetRowCarriesTheTotalOfItsContributors() throws Exception {
		final DistributedMetricsContext<?> fleet = fleet(
						StatsReportingDriverMock.stats(8, 3, 5), StatsReportingDriverMock.stats(6, 2, 4));

		final JsonNode row = fleetRow(fleet);

		final JsonNode stats = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(List.of("driver", "counters", "gauges", "reporting", "partial"), fieldNames(stats));
		assertEquals(StatsReportingDriverMock.DRIVER, stats.get("driver").asText());
		assertEquals(
						List.of(StatsReportingDriverMock.COUNTER_SPROCKETS, StatsReportingDriverMock.COUNTER_WIDGETS),
						fieldNames(stats.get("counters")));
		assertEquals(14, stats.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(5, stats.get("counters").get(StatsReportingDriverMock.COUNTER_SPROCKETS).asLong());
		assertEquals(9, stats.get("gauges").get(StatsReportingDriverMock.GAUGE_SHELF).asLong());
		assertEquals(2, stats.get("reporting").asInt());
		assertFalse(stats.get("partial").asBoolean());
		assertFalse(row.get("partial").asBoolean());
		assertEquals(2, row.get("operations").get("success_count").asLong());
		assertSchema5Accepts(row);
	}

	@Test
	void fleetRowSaysHowManyContributorsItsTotalHolds() throws Exception {
		final DistributedMetricsContext<?> fleet = fleet(StatsReportingDriverMock.stats(8, 3, 5), null);

		final JsonNode row = fleetRow(fleet);

		final JsonNode stats = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(8, stats.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(1, stats.get("reporting").asInt());
		assertTrue(stats.get("partial").asBoolean());
		assertFalse(row.get("partial").asBoolean(), "the row itself is complete: both contributors are present");
		assertSchema5Accepts(row);
	}

	@Test
	void fleetRowOfContributorsWithDifferentDriversNamesThemAndCarriesNoTotal() throws Exception {
		final DistributedMetricsContext<?> fleet = fleet(StatsReportingDriverMock.stats(8, 3, 5), otherDriverStats());

		final JsonNode row = fleetRow(fleet);

		final JsonNode stats = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(List.of("partial", "drivers"), fieldNames(stats));
		assertTrue(stats.get("partial").asBoolean());
		assertEquals(List.of(OTHER_DRIVER, StatsReportingDriverMock.DRIVER), texts(stats.get("drivers")));
		assertFalse(row.get("partial").asBoolean());
		assertSchema5Accepts(row);
	}

	@Test
	void fleetRowHasNoStatisticsWhenNoContributorReports() {
		final JsonNode row = fleetRow(fleet(null, null));

		assertFalse(row.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
		assertEquals(2, row.get("operations").get("success_count").asLong());
	}

	@Test
	void terminalFleetRowCarriesTheTotalTheStepKept() {
		final DistributedMetricsContext<?> fleet = fleet(
						StatsReportingDriverMock.stats(8, 3, 5), StatsReportingDriverMock.stats(6, 2, 4));
		final JsonNode live = fleetRow(fleet).get(MetricsConstants.METRIC_NAME_DRIVER_STATS);

		final JsonNode row = onlyRow(new MetricsJsonResponder(finished(fleet), config()).buildClusterMetrics(false));

		assertTrue(row.get("terminal").asBoolean());
		assertEquals(live, row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS), "the same object as on the live row");
		assertEquals(14, live.get("counters").get(StatsReportingDriverMock.COUNTER_WIDGETS).asLong());
		assertEquals(2, live.get("reporting").asInt());
	}

	@Test
	void terminalFleetRowOfContributorsWithDifferentDriversNamesThem() {
		final DistributedMetricsContext<?> fleet = fleet(StatsReportingDriverMock.stats(8, 3, 5), otherDriverStats());

		final JsonNode row = onlyRow(new MetricsJsonResponder(finished(fleet), config()).buildClusterMetrics(false));

		assertTrue(row.get("terminal").asBoolean());
		final JsonNode stats = row.get(MetricsConstants.METRIC_NAME_DRIVER_STATS);
		assertEquals(List.of("partial", "drivers"), fieldNames(stats));
		assertEquals(List.of(OTHER_DRIVER, StatsReportingDriverMock.DRIVER), texts(stats.get("drivers")));
	}

	@Test
	void terminalFleetRowHasNoStatisticsWhenNoContributorReported() {
		final JsonNode row = onlyRow(
						new MetricsJsonResponder(finished(fleet(null, null)), config()).buildClusterMetrics(false));

		assertTrue(row.get("terminal").asBoolean());
		assertFalse(row.has(MetricsConstants.METRIC_NAME_DRIVER_STATS));
	}

	/** A metrics manager in which the context's step has ended. */
	private static MetricsManagerImpl finished(final MetricsContext<?> ctx) {
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);
		mgr.register(ctx);
		mgr.unregister(ctx);
		return mgr;
	}

	private static JsonNode onlyRow(final MetricsManagerImpl mgr) {
		return onlyRow(new MetricsJsonResponder(mgr, config()).buildNodeMetrics(false));
	}

	private static JsonNode onlyRow(final ArrayNode rows) {
		assertEquals(1, rows.size(), rows.toString());
		return rows.get(0);
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

	/**
	 * A distributed step on each contributor of which one operation succeeded.
	 *
	 * @param reported what the driver of each contributor reports; null for a driver that reports none
	 */
	private DistributedMetricsContext<?> fleet(final DriverStatsSnapshot... reported) {
		final List<MetricsContext<?>> nodes = new ArrayList<>();
		for (final DriverStatsSnapshot stats : reported) {
			final MetricsContext<?> node = DriverStatsFleet.node("fleet-step", () -> stats);
			node.markSucc(DriverStatsFleet.ITEM_SIZE, 1_000, 100);
			nodes.add(node);
		}
		contexts.addAll(nodes);
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(
						"fleet-step",
						OpType.CREATE,
						reported.length,
						() -> nodes.stream().map(DriverStatsFleet::snapshotOf).toList());
		contexts.add(fleet);
		return fleet;
	}

	private static DriverStatsSnapshot otherDriverStats() {
		return DriverStatsSnapshot.builder(OTHER_DRIVER).counter("gears_cut", 6).build();
	}

	private static JsonNode fleetRow(final DistributedMetricsContext<?> fleet) {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(fleet));
		when(mgr.getAllContexts()).thenReturn(Set.of(fleet));
		when(mgr.getTerminalSteps()).thenReturn(List.of());
		return onlyRow(new MetricsJsonResponder(mgr, config()).buildClusterMetrics(false));
	}

	/** The published metrics schema 5 accepts the row as the responder wrote it. */
	private static void assertSchema5Accepts(final JsonNode row) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		final JsonSchema schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(
						mapper.readTree(Files.readString(Path.of("doc", "usage", "api", "remote", "metrics-schema-v5.json"))));
		schema.initializeValidators();
		final Set<ValidationMessage> errors = schema.validate(mapper.createArrayNode().add(row));
		assertTrue(errors.isEmpty(), errors::toString);
	}

	private static List<String> texts(final JsonNode array) {
		final List<String> texts = new ArrayList<>();
		array.forEach(element -> texts.add(element.asText()));
		return texts;
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
