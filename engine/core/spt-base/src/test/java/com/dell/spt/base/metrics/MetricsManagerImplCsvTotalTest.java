package com.dell.spt.base.metrics;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsManagerImplCsvTotalTest {

	@Test
	void writesSingleHeaderForMultipleOperationsInSameStep(@TempDir final Path tempDir) throws Exception {
		ThreadContext.put(KEY_HOME_DIR, tempDir.toString());
		try {
			final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
			final String stepId = "mixed-step-csv-total";
			final DistributedMetricsContext readCtx = distributedContext(
					stepId,
					OpType.READ,
					snapshot(11L, 45_056L, List.of(120L, 180L), List.of(210L, 260L), List.of(30L, 40L)),
					-1);
			final DistributedMetricsContext createCtx = distributedContext(
					stepId,
					OpType.CREATE,
					snapshot(7L, 28_672L, List.of(160L, 220L), List.of(250L, 300L), List.of()),
					1);

			mgr.register(readCtx);
			mgr.register(createCtx);
			mgr.unregister(readCtx);
			mgr.unregister(createCtx);
			LogUtil.flushAll();

			final Path csvPath = tempDir.resolve("log").resolve(stepId).resolve("metrics.total.csv");
			assertTrue(Files.exists(csvPath), "expected metrics.total.csv at " + csvPath);

			final List<String> lines = Files.readAllLines(csvPath);
			assertEquals(1L, lines.stream().filter(line -> line.startsWith("DateTimeISO8601,")).count(), lines.toString());

			try (final var reader = Files.newBufferedReader(csvPath);
						 final CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
				final var records = parser.getRecords();
				final int headerSize = parser.getHeaderMap().size();
				assertEquals(2, records.size(), records.toString());
				assertEquals(List.of("READ", "CREATE"), records.stream().map(record -> record.get("OpType")).toList());
				for (final var record : records) {
					assertEquals(headerSize, record.size(), record.toString());
					assertNotEquals("DateTimeISO8601", record.get(0), record.toString());
				}
			}
		} finally {
			ThreadContext.remove(KEY_HOME_DIR);
		}
	}

	private static DistributedMetricsContext distributedContext(
					final String stepId,
					final OpType opType,
					final DistributedAllMetricsSnapshotImpl snapshot,
					final int sortOrder) {
		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.quantileValues()).thenReturn(List.of(0.5, 0.9, 0.99, 0.999));
		when(ctx.nodeCount()).thenReturn(1);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, snapshot.successSnapshot().count()));
		when(ctx.concurrencyLimit()).thenReturn(4);
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(4_096L));
		when(ctx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(ctx.nodeAddrs()).thenReturn(List.of("127.0.0.1:1099"));
		when(ctx.comment()).thenReturn("");
		when(ctx.runId()).thenReturn(1L);
		when(ctx.sumPersistEnabled()).thenReturn(true);
		when(ctx.timingPersistEnabled()).thenReturn(false);
		when(ctx.outputPeriodMillis()).thenReturn(0L);
		when(ctx.thresholdStateEntered()).thenReturn(false);
		when(ctx.thresholdStateExited()).thenReturn(false);
		when(ctx.compareTo(any())).thenAnswer(invocation -> invocation.getArgument(0) == ctx ? 0 : sortOrder);
		return ctx;
	}

	private static DistributedAllMetricsSnapshotImpl snapshot(
					final long successCount,
					final long byteCount,
					final List<Long> latencySamples,
					final List<Long> durationSamples,
					final List<Long> ttfbSamples) {
		return new DistributedAllMetricsSnapshotImpl(
					TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, durationSamples),
					TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, latencySamples),
					ttfbSamples.isEmpty()
							? new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB)
							: TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_TTFB, ttfbSamples),
					new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 4, 4.0),
					new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, 1_000),
					new RateMetricSnapshotImpl((double) successCount, (double) successCount, MetricsConstants.METRIC_NAME_SUCC, successCount, 1_000),
					new RateMetricSnapshotImpl((double) byteCount, (double) byteCount, MetricsConstants.METRIC_NAME_BYTE, byteCount, 1_000),
					1,
					1_000);
	}
}
