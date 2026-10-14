package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DriverStatsFleet;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.util.PrometheusMetricsExporterImpl;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The fleet total of a running step's storage driver statistics on the Prometheus text endpoint,
 * through the real metrics manager, registry and text formats.
 */
class MetricsManagerImplDriverStatsPrometheusTest {

	private static final String STEP_ID = "driver-stats-prometheus";

	private final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<MetricsContext<?>> contexts = new ArrayList<>();

	@AfterEach
	void closeContexts() {
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void runningStepExportsItsFleetTotalAtEachScrapeAndNoLongerOnceItEnds() throws IOException {
		final AtomicLong widgets = new AtomicLong(5);
		final MetricsContext<?> local = node(() -> StatsReportingDriverMock.stats(widgets.get(), 1, 3));
		final MetricsContext<?> remote = node(() -> StatsReportingDriverMock.stats(2, 1, 4));
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(
						STEP_ID,
						OpType.CREATE,
						2,
						() -> List.of(DriverStatsFleet.snapshotOf(local), DriverStatsFleet.snapshotOf(remote)));
		contexts.add(fleet);
		mgr.register(fleet);
		final String labels = labels(fleet);

		String text = text(TextFormat.CONTENT_TYPE_004);
		assertEquals("counter", typeOf(text, "spt_driver_widgets_made_total" + labels), text);
		assertSample(text, "spt_driver_widgets_made_total" + labels, "7.0");
		assertSample(text, "spt_driver_sprockets_bent_total" + labels, "2.0");
		assertEquals("gauge", typeOf(text, "spt_driver_shelf_load" + labels), text);
		assertSample(text, "spt_driver_shelf_load" + labels, "7.0");
		assertSample(text, "spt_driver_reporting" + labels, "2.0");
		assertSample(text, "spt_driver_partial" + labels, "0.0");

		widgets.set(9);
		text = text(TextFormat.CONTENT_TYPE_OPENMETRICS_100);
		assertEquals("counter", typeOf(text, "spt_driver_widgets_made_total" + labels.replace(",}", "}")), text);
		assertSample(text, "spt_driver_widgets_made_total" + labels.replace(",}", "}"), "11.0");

		mgr.unregister(fleet);
		text = text(TextFormat.CONTENT_TYPE_004);
		assertFalse(text.contains("load_step_id=\"" + STEP_ID + "\""), text);
	}

	private MetricsContext<?> node(final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> node = DriverStatsFleet.node(STEP_ID, stats);
		contexts.add(node);
		return node;
	}

	/** The labels of the step's series in the 0.0.4 text format, the driver type last. */
	private static String labels(final DistributedMetricsContext<?> fleet) {
		final String[] values = PrometheusMetricsExporterImpl.labelValues(fleet);
		final StringBuilder labels = new StringBuilder("{");
		for (int i = 0; i < values.length; i++) {
			labels.append(MetricsConstants.METRIC_LABELS.get(i)).append("=\"").append(values[i]).append("\",");
		}
		labels.append(MetricsConstants.METRIC_LABEL_DRIVER).append("=\"").append(StatsReportingDriverMock.DRIVER).append("\",}");
		return labels.toString();
	}

	private static String text(final String contentType) throws IOException {
		final StringWriter writer = new StringWriter();
		TextFormat.writeFormat(contentType, writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
		return writer.toString();
	}

	private static void assertSample(final String text, final String series, final String value) {
		assertTrue(text.lines().anyMatch((series + " " + value)::equals), series + " " + value + " in\n" + text);
	}

	/** Returns the type the text declares for the family of the given series. */
	private static String typeOf(final String text, final String series) {
		String type = null;
		for (final String line : text.lines().toList()) {
			if (line.startsWith("# TYPE ")) {
				type = line.substring(line.lastIndexOf(' ') + 1);
			} else if (line.startsWith(series + " ")) {
				return type;
			}
		}
		return null;
	}
}
