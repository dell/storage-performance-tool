package com.dell.spt.base.metrics.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsTotal;
import com.github.akurilov.commons.system.SizeInBytes;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.Test;

/** How the fleet totals of storage driver statistics appear to Prometheus. */
class DriverStatsCollectorTest {

	private static final String DRIVER = "s3-rdma";

	@Test
	void completeTotalGivesACounterPerCounterAndAGaugePerGaugeWithTheStepLabelsAndTheDriver() {
		final DistributedMetricsContext<?> step = step("read-1", OpType.READ, complete(
						stats(DRIVER, Map.of("rdma_transferred", 4_000L), Map.of("pool_reserved_bytes", 8_192L)),
						stats(DRIVER, Map.of("rdma_transferred", 2_000L), Map.of("pool_reserved_bytes", 4_096L))));

		final Map<String, MetricFamilySamples> families = collect(step);

		assertEquals(List.of("spt_driver_partial", "spt_driver_pool_reserved_bytes", "spt_driver_rdma_transferred",
						"spt_driver_reporting"), List.copyOf(families.keySet()));
		final MetricFamilySamples transferred = families.get("spt_driver_rdma_transferred");
		assertEquals(Collector.Type.COUNTER, transferred.type);
		final Sample sample = only(transferred);
		assertEquals("spt_driver_rdma_transferred_total", sample.name);
		assertEquals(6_000.0, sample.value);
		final List<String> labelNames = new ArrayList<>(MetricsConstants.METRIC_LABELS);
		labelNames.add(MetricsConstants.METRIC_LABEL_DRIVER);
		assertEquals(labelNames, sample.labelNames);
		final List<String> labelValues = new ArrayList<>(List.of(PrometheusMetricsExporterImpl.labelValues(step)));
		labelValues.add(DRIVER);
		assertEquals(labelValues, sample.labelValues);
		final MetricFamilySamples reserved = families.get("spt_driver_pool_reserved_bytes");
		assertEquals(Collector.Type.GAUGE, reserved.type);
		assertEquals("spt_driver_pool_reserved_bytes", only(reserved).name);
		assertEquals(12_288.0, only(reserved).value);
		assertEquals(Collector.Type.GAUGE, families.get("spt_driver_reporting").type);
		assertEquals(2.0, only(families.get("spt_driver_reporting")).value);
		assertEquals(0.0, only(families.get("spt_driver_partial")).value);
	}

	@Test
	void partialTotalGivesOnlyHowManyReportedSoACounterNeverFalls() {
		final DriverStatsTotal partial = DriverStatsTotal.of(
						List.of(stats(DRIVER, Map.of("rdma_transferred", 4_000L), Map.of())), false);

		final Map<String, MetricFamilySamples> families = collect(step("read-1", OpType.READ, partial));

		assertEquals(List.of("spt_driver_partial", "spt_driver_reporting"), List.copyOf(families.keySet()));
		assertEquals(1.0, only(families.get("spt_driver_reporting")).value);
		assertEquals(1.0, only(families.get("spt_driver_partial")).value);
	}

	@Test
	void totalOfDifferentDriversNamesThemAllAndGivesNoSum() {
		final DriverStatsTotal mixed = DriverStatsTotal.of(List.of(
						stats(DRIVER, Map.of("rdma_transferred", 1L), Map.of()),
						stats("s3", Map.of("requests", 1L), Map.of())), true);

		final Map<String, MetricFamilySamples> families = collect(step("read-1", OpType.READ, mixed));

		assertEquals(List.of("spt_driver_partial", "spt_driver_reporting"), List.copyOf(families.keySet()));
		final Sample partial = only(families.get("spt_driver_partial"));
		assertEquals("s3,s3-rdma", partial.labelValues.get(partial.labelValues.size() - 1));
		assertEquals(1.0, partial.value);
		assertEquals(0.0, only(families.get("spt_driver_reporting")).value);
	}

	@Test
	void stepWithoutStatisticsOrWithoutASnapshotGivesNothing() {
		final DistributedMetricsContext<?> noSnapshot = step("read-1", OpType.READ, null);
		when(noSnapshot.lastSnapshot()).thenReturn(null);

		assertEquals(List.of(), new DriverStatsCollector(() -> List.of(step("read-2", OpType.READ, null), noSnapshot))
						.collect());
	}

	@Test
	void eachNameIsOneFamilyWithASamplePerStepInLabelOrder() {
		final DistributedMetricsContext<?> write = step("write-1", OpType.CREATE, complete(
						stats(DRIVER, Map.of("rdma_transferred", 5L), Map.of())));
		final DistributedMetricsContext<?> read = step("read-1", OpType.READ, complete(
						stats(DRIVER, Map.of("rdma_transferred", 3L), Map.of())));

		final Map<String, MetricFamilySamples> families = collect(write, read);

		final List<Sample> samples = families.get("spt_driver_rdma_transferred").samples;
		assertEquals(List.of("read-1", "write-1"), samples.stream().map(sample -> sample.labelValues.get(0)).toList());
		assertEquals(List.of(3.0, 5.0), samples.stream().map(sample -> sample.value).toList());
		assertEquals(2, families.get("spt_driver_reporting").samples.size());
	}

	@Test
	void namesPrometheusDoesNotAcceptAreMadeValidOnceAndLogged() throws Exception {
		final DistributedMetricsContext<?> step = step("read-1", OpType.READ, complete(
						stats(DRIVER, Map.of("rdma-transferred.bytes", 7L, "requests_total", 2L), Map.of("9lives", 1L, "queue depth", 2L))));
		final DriverStatsCollector collector = new DriverStatsCollector(() -> List.of(step));

		final List<String> warnings = warningsOf(() -> {
			collector.collect();
			collector.collect();
		});

		final Map<String, MetricFamilySamples> families = byName(collector.collect());
		assertEquals("spt_driver_rdma_transferred_bytes_total", only(families.get("spt_driver_rdma_transferred_bytes")).name);
		assertEquals("spt_driver_requests_total", only(families.get("spt_driver_requests")).name);
		// The prefix makes a leading digit valid.
		assertEquals(1.0, only(families.get("spt_driver_9lives")).value);
		assertEquals(2.0, only(families.get("spt_driver_queue_depth")).value);
		assertEquals(2, warnings.size(), warnings.toString());
		assertTrue(warnings.stream().anyMatch(w -> w.contains("\"rdma-transferred.bytes\"")
						&& w.contains("spt_driver_rdma_transferred_bytes")), warnings.toString());
		assertTrue(warnings.stream().anyMatch(w -> w.contains("\"queue depth\"")
						&& w.contains("spt_driver_queue_depth")), warnings.toString());
	}

	@Test
	void statisticThatWouldTakeTheSeriesOfAnotherIsLeftOutOnceAndLogged() throws Exception {
		final Map<String, Long> counters = new LinkedHashMap<>();
		counters.put("a.b", 1L);
		counters.put("a_b", 2L);
		counters.put("partial", 3L);
		counters.put("x", 4L);
		final DistributedMetricsContext<?> step = step("read-1", OpType.READ, complete(
						stats(DRIVER, counters, Map.of("reporting", 5L, "x_total", 6L))));
		final DriverStatsCollector collector = new DriverStatsCollector(() -> List.of(step));

		final List<String> warnings = warningsOf(() -> {
			collector.collect();
			collector.collect();
		});

		final Map<String, MetricFamilySamples> families = byName(collector.collect());
		assertEquals(List.of("spt_driver_a_b", "spt_driver_partial", "spt_driver_reporting", "spt_driver_x"),
						List.copyOf(families.keySet()));
		// Names are taken in name order: "a.b" before "a_b", counters before gauges.
		assertEquals(1.0, only(families.get("spt_driver_a_b")).value);
		assertEquals(0.0, only(families.get("spt_driver_partial")).value);
		assertEquals(1.0, only(families.get("spt_driver_reporting")).value);
		assertEquals(4.0, only(families.get("spt_driver_x")).value);
		final List<String> leftOut = warnings.stream().filter(w -> w.contains("is not exported")).toList();
		assertEquals(4, leftOut.size(), warnings.toString());
		for (final String statistic : List.of("a_b", "partial", "reporting", "x_total")) {
			assertTrue(leftOut.stream().anyMatch(w -> w.contains("\"" + statistic + "\"")), warnings.toString());
		}
	}

	@Test
	void collectionRefreshesEachStep() {
		final DistributedMetricsContext<?> step = step("read-1", OpType.READ, null);

		new DriverStatsCollector(() -> List.of(step)).collect();

		verify(step).refreshLastSnapshot();
	}

	private static Map<String, MetricFamilySamples> collect(final DistributedMetricsContext<?>... steps) {
		return byName(new DriverStatsCollector(() -> List.of(steps)).collect());
	}

	private static Map<String, MetricFamilySamples> byName(final List<MetricFamilySamples> families) {
		final Map<String, MetricFamilySamples> byName = new LinkedHashMap<>();
		families.forEach(family -> assertEquals(null, byName.put(family.name, family), "duplicate family " + family.name));
		return byName;
	}

	private static Sample only(final MetricFamilySamples family) {
		assertEquals(1, family.samples.size(), family.samples.toString());
		return family.samples.get(0);
	}

	private static DriverStatsTotal complete(final DriverStatsSnapshot... nodes) {
		return DriverStatsTotal.of(List.of(nodes), true);
	}

	private static DriverStatsSnapshot stats(
					final String driver, final Map<String, Long> counters, final Map<String, Long> gauges) {
		final DriverStatsSnapshot.Builder builder = DriverStatsSnapshot.builder(driver);
		counters.forEach(builder::counter);
		gauges.forEach(builder::gauge);
		return builder.build();
	}

	/** A distributed metrics context of a running step whose fleet total is the given one. */
	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static DistributedMetricsContext<?> step(final String stepId, final OpType opType, final DriverStatsTotal total) {
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		when(snapshot.driverStatsTotal()).thenReturn(total);
		final DistributedMetricsContext context = mock(DistributedMetricsContext.class);
		when(context.loadStepId()).thenReturn(stepId);
		when(context.opType()).thenReturn(opType);
		when(context.concurrencyLimit()).thenReturn(4);
		when(context.itemDataSize()).thenReturn(new SizeInBytes(1024));
		when(context.startTimeStamp()).thenReturn(1_000L);
		when(context.nodeAddrs()).thenReturn(List.of("worker-1"));
		when(context.comment()).thenReturn("");
		when(context.runId()).thenReturn(7L);
		when(context.lastSnapshot()).thenReturn(snapshot);
		return context;
	}

	/** Returns the warnings logged while the action runs. */
	private static List<String> warningsOf(final Runnable action) throws InterruptedException {
		final Logger logger = (Logger) Loggers.ERR;
		final Appender appender = mock(Appender.class);
		when(appender.getName()).thenReturn("driver-stats-collector-test");
		when(appender.isStarted()).thenReturn(true);
		final LinkedBlockingQueue<String> events = new LinkedBlockingQueue<>();
		doAnswer(call -> {
			final LogEvent event = call.getArgument(0);
			if (Level.WARN.equals(event.getLevel())) {
				events.add(event.getMessage().getFormattedMessage());
			}
			return null;
		}).when(appender).append(any());
		final Level previousLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.WARN);
		try {
			action.run();
			// A same-thread fence drains earlier asynchronous events.
			final String fence = "driver-stats-collector-fence";
			logger.warn(fence);
			final List<String> warnings = new ArrayList<>();
			for (String event = events.poll(2, TimeUnit.SECONDS); !fence.equals(event); event = events.poll(2, TimeUnit.SECONDS)) {
				assertTrue(event != null, "the logging fence did not arrive");
				warnings.add(event);
			}
			return warnings;
		} finally {
			logger.removeAppender(appender);
			logger.setLevel(previousLevel);
		}
	}
}
