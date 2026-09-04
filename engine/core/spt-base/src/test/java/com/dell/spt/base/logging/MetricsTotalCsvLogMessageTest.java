package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MetricsTotalCsvLogMessageTest {

	@Test
	void unavailableTtfbUsesEmptyFieldsMatchingHeader() {
		final Map<Double, Long> quantiles = new LinkedHashMap<>();
		quantiles.put(0.5, 20L);
		quantiles.put(0.9, 30L);
		quantiles.put(0.99, 40L);
		quantiles.put(0.999, 50L);
		final AllMetricsSnapshot snapshot = new DistributedAllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, List.of(100L, 200L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, List.of(10L, 20L)),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 2, 2.0),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, 1000),
						new RateMetricSnapshotImpl(1.0, 1.0, MetricsConstants.METRIC_NAME_SUCC, 2, 1000),
						new RateMetricSnapshotImpl(128.0, 128.0, MetricsConstants.METRIC_NAME_BYTE, 256, 1000),
						2,
						1000);
		final MetricsTotalCsvLogMessage message = new MetricsTotalCsvLogMessage(
						snapshot,
						OpType.READ,
						2,
						quantiles,
						quantiles,
						quantiles);
		final StringBuilder output = new StringBuilder();

		message.formatTo(output);

		final String formatted = output.toString();
		final String lineSeparator = System.lineSeparator();
		final int lineSeparatorIndex = formatted.indexOf(lineSeparator);
		assertTrue(lineSeparatorIndex > 0, formatted);
		final String header = formatted.substring(0, lineSeparatorIndex);
		final String values = formatted.substring(lineSeparatorIndex + lineSeparator.length());
		assertEquals(-1, values.indexOf(lineSeparator));
		assertTrue(header.endsWith("TtfbMax[us]"));
		assertFalse(header.contains("DeleteMetricsJSON"));
		assertTrue(values.endsWith(",20,,,,,,,"), formatted);
		assertEquals(columnCount(header), columnCount(values), formatted);
	}

	@Test
	void availableTtfbWritesValuesMatchingHeader() {
		final Map<Double, Long> quantiles = new LinkedHashMap<>();
		quantiles.put(0.5, 20L);
		quantiles.put(0.9, 30L);
		quantiles.put(0.99, 40L);
		quantiles.put(0.999, 50L);
		final AllMetricsSnapshot snapshot = new DistributedAllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, List.of(100L, 200L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, List.of(10L, 20L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_TTFB, List.of(7L, 11L, 13L)),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 2, 2.0),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 2, 1000),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 1, 1000),
						new RateMetricSnapshotImpl(1.0, 1.0, MetricsConstants.METRIC_NAME_SUCC, 3, 1000),
						new RateMetricSnapshotImpl(128.0, 128.0, MetricsConstants.METRIC_NAME_BYTE, 256, 1000),
						2,
						1000);
		final MetricsTotalCsvLogMessage message = new MetricsTotalCsvLogMessage(
						snapshot,
						OpType.READ,
						2,
						quantiles,
						quantiles,
						quantiles);
		final StringBuilder output = new StringBuilder();

		message.formatTo(output);

		final String formatted = output.toString();
		final String lineSeparator = System.lineSeparator();
		final int lineSeparatorIndex = formatted.indexOf(lineSeparator);
		assertTrue(lineSeparatorIndex > 0, formatted);
		final String header = formatted.substring(0, lineSeparatorIndex);
		final String values = formatted.substring(lineSeparatorIndex + lineSeparator.length());
		assertEquals(columnCount(header), columnCount(values), formatted);
		assertTrue(header.contains("CountFail,CountCorrupt,Size"), formatted);
		assertTrue(values.contains(",2,1,256,"), formatted);
		assertTrue(values.endsWith(",10.333333333333334,7,20,30,40,50,13"), formatted);
	}

	private static int columnCount(final String csvLine) {
		int columns = 1;
		boolean quoted = false;
		for (int i = 0; i < csvLine.length(); i++) {
			final char c = csvLine.charAt(i);
			if (c == '"') {
				quoted = !quoted;
			} else if (c == ',' && !quoted) {
				columns++;
			}
		}
		return columns;
	}
}
