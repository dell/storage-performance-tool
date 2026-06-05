package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.io.StringReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.Test;

class MetricsCsvLogMessageTest {

	private static final String INTERVAL_CSV_HEADER = "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,"
					+ "Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MB/s],"
					+ "BWLast[MB/s],DurationAvg[us],DurationMin[us],DurationLoQ[us],DurationMed[us],"
					+ "DurationHiQ[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyLoQ[us],"
					+ "LatencyMed[us],LatencyHiQ[us],LatencyMax[us]";

	@Test
	void intervalCsvRowMatchesHeaderShapeAndColumns() throws Exception {
		final AllMetricsSnapshot snapshot = new DistributedAllMetricsSnapshotImpl(
						new TimingMetricSnapshotImpl(1000, 4, 100, 400, 250.0, MetricsConstants.METRIC_NAME_DUR),
						new TimingMetricSnapshotImpl(100, 4, 10, 40, 25.0, MetricsConstants.METRIC_NAME_LAT),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 2, 1.5),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, 1000),
						new RateMetricSnapshotImpl(4.0, 3.0, MetricsConstants.METRIC_NAME_SUCC, 4, 1000),
						new RateMetricSnapshotImpl(4096.0, 2048.0, MetricsConstants.METRIC_NAME_BYTE, 4096, 1000),
						3,
						1000);
		final MetricsCsvLogMessage message = new MetricsCsvLogMessage(snapshot, OpType.READ, 2);
		final StringBuilder row = new StringBuilder();

		message.formatTo(row);

		try (final CSVParser parser = CSVFormat.DEFAULT.builder()
						.setHeader()
						.setSkipHeaderRecord(true)
						.build()
						.parse(new StringReader(INTERVAL_CSV_HEADER + System.lineSeparator() + row))) {
			final var records = parser.getRecords();
			assertEquals(1, records.size());
			final var record = records.get(0);
			assertEquals(parser.getHeaderMap().size(), record.size(), row.toString());
			assertEquals("400", record.get("DurationMax[us]"));
			assertEquals("25.0", record.get("LatencyAvg[us]"));
			assertEquals("40", record.get("LatencyMax[us]"));
		}
	}
}
