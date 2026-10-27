package com.dell.spt.base.metrics;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.logging.MetricsSteadyTotalCsvLogMessage;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The Entry Node writes one {@code metrics.steady.total.csv} row per context, beside its total row. */
class MetricsManagerImplSteadyCsvTest {

	@Test
	void writesOneSteadyRowPerContextWithTheTotalFilesColumns(@TempDir final Path tempDir) throws Exception {
		final String stepId = "steady-csv-" + System.nanoTime();
		final CapturingAppender steadyRows = new CapturingAppender(stepId, "steadyCapture");
		final CapturingAppender totalRows = new CapturingAppender(stepId, "totalCapture");
		final var loggerCtx = LoggerContext.getContext(false);
		final var steadyLogger = loggerCtx.getLogger(Loggers.METRICS_STEADY_FILE_TOTAL.getName());
		final var totalLogger = loggerCtx.getLogger(Loggers.METRICS_FILE_TOTAL.getName());
		steadyRows.start();
		totalRows.start();
		steadyLogger.addAppender(steadyRows);
		totalLogger.addAppender(totalRows);
		ThreadContext.put(KEY_HOME_DIR, tempDir.toString());
		try {
			final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
			final SteadyResult valid = new SteadyResult(
							SteadyStatus.VALID, "shutdown", 2_500, 30_000_000_000L, 120.0, -1.5, 0.04, 1_000, 2_000,
							snapshot(3_000L, 12_288_000L, 30_000));
			final DistributedMetricsContext readCtx = distributedContext(stepId, OpType.READ, valid);
			final DistributedMetricsContext statCtx = distributedContext(
							stepId, OpType.STAT, SteadyResult.incomplete("a slice never closed"));
			final DistributedMetricsContext createCtx = distributedContext(stepId, OpType.CREATE, null);
			final List<DistributedMetricsContext> contexts = List.of(readCtx, statCtx, createCtx);
			configureSortOrder(contexts);
			for (final DistributedMetricsContext ctx : contexts) {
				mgr.register(ctx);
			}
			for (final DistributedMetricsContext ctx : contexts) {
				mgr.unregister(ctx);
			}

			final String steadyCsv = joined(await(steadyRows, contexts.size()));
			final String totalCsv = joined(await(totalRows, contexts.size()));
			final String steadyHeader = steadyCsv.lines().findFirst().orElseThrow();
			final String totalHeader = totalCsv.lines().findFirst().orElseThrow();
			assertEquals(totalHeader + "," + MetricsSteadyTotalCsvLogMessage.STEADY_COLUMNS, steadyHeader);
			assertEquals(1L, steadyCsv.lines().filter(line -> line.startsWith("DateTimeISO8601,")).count());

			try (final CSVParser parser = CSVFormat.DEFAULT.builder()
							.setHeader()
							.setSkipHeaderRecord(true)
							.build()
							.parse(new StringReader(steadyCsv))) {
				final var records = parser.getRecords();
				assertEquals(3, records.size());
				for (final var record : records) {
					assertEquals(parser.getHeaderMap().size(), record.size(), record.toString());
				}
				final var read = records.get(0);
				assertEquals("READ", read.get("OpType"));
				assertEquals("valid", read.get("SteadyStatus"));
				assertEquals(100.0, Double.parseDouble(read.get("TPAvg[op/s]")), 1e-9);
				assertEquals(30.0, Double.parseDouble(read.get("StepDuration[s]")), 1e-9);
				assertEquals(2.5, Double.parseDouble(read.get("SteadyStart[s]")), 1e-9);
				assertEquals(120.0, Double.parseDouble(read.get("SteadyTurnovers")), 1e-9);
				assertEquals(-1.5, Double.parseDouble(read.get("SteadyTrend[%]")), 1e-9);
				assertEquals(0.04, Double.parseDouble(read.get("SteadyCV")), 1e-12);
				assertEquals("", read.get("TtfbAvg[us]"), "a meter without observations leaves its cells empty");

				final var stat = records.get(1);
				assertEquals("STAT", stat.get("OpType"));
				assertEquals("incomplete", stat.get("SteadyStatus"));
				assertEquals("1", stat.get("NodeCount"), "identity columns are always filled");
				assertEquals("", stat.get("CountSucc"), "an incomplete row has no statistics");
				assertEquals("", stat.get("SteadyStart[s]"));
				final var create = records.get(2);
				assertEquals("incomplete", create.get("SteadyStatus"), "no slice result is incomplete, not absent");
			}

			// Terminal entries keep the (stepId, distributed) identity (D4): the last context unregistered wins.
			final TerminalStepEntry terminal = mgr.getTerminalSteps().stream()
							.filter(entry -> entry.distributed && entry.stepId.equals(stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(OpType.CREATE, terminal.opType);
			assertNotNull(terminal.steady);
			assertEquals(SteadyStatus.INCOMPLETE, terminal.steady.status());
		} finally {
			ThreadContext.remove(KEY_HOME_DIR);
			steadyLogger.removeAppender(steadyRows);
			totalLogger.removeAppender(totalRows);
			steadyRows.stop();
			totalRows.stop();
		}
	}

	@Test
	void aFinalReadThatNoSliceAnsweredWritesAnIncompleteSteadyRow(@TempDir final Path tempDir) throws Exception {
		final String stepId = "steady-csv-final-read-" + System.nanoTime();
		final CapturingAppender steadyRows = new CapturingAppender(stepId, "steadyCapture");
		final CapturingAppender totalRows = new CapturingAppender(stepId, "totalCapture");
		final var loggerCtx = LoggerContext.getContext(false);
		final var steadyLogger = loggerCtx.getLogger(Loggers.METRICS_STEADY_FILE_TOTAL.getName());
		final var totalLogger = loggerCtx.getLogger(Loggers.METRICS_FILE_TOTAL.getName());
		steadyRows.start();
		totalRows.start();
		steadyLogger.addAppender(steadyRows);
		totalLogger.addAppender(totalRows);
		ThreadContext.put(KEY_HOME_DIR, tempDir.toString());
		try {
			final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
			final SteadyResult valid = new SteadyResult(
							SteadyStatus.VALID, "shutdown", 2_500, 30_000_000_000L, 120.0, -1.5, 0.04, 1_000, 2_000,
							snapshot(3_000L, 12_288_000L, 30_000));
			final DistributedMetricsContext ctx = distributedContext(stepId, OpType.READ, valid);
			// The earlier read carried every slice's window; the final read finds no slice.
			final AtomicReference<AllMetricsSnapshot> last = new AtomicReference<>(ctx.lastSnapshot());
			when(ctx.lastSnapshot()).thenAnswer(invocation -> last.get());
			doAnswer(invocation -> {
				last.set(null);
				return null;
			}).when(ctx).refreshLastSnapshot(true);
			when(ctx.partial()).thenReturn(true);
			configureSortOrder(List.of(ctx));
			mgr.register(ctx);
			mgr.unregister(ctx);

			final String steadyCsv = joined(await(steadyRows, 1));
			final String totalCsv = joined(await(totalRows, 1));
			try (final CSVParser steadyParser = CSVFormat.DEFAULT.builder()
							.setHeader()
							.setSkipHeaderRecord(true)
							.build()
							.parse(new StringReader(steadyCsv));
							final CSVParser totalParser = CSVFormat.DEFAULT.builder()
											.setHeader()
											.setSkipHeaderRecord(true)
											.build()
											.parse(new StringReader(totalCsv))) {
				final var steady = steadyParser.getRecords().get(0);
				assertEquals("incomplete", steady.get("SteadyStatus"));
				assertEquals("", steady.get("CountSucc"), "the earlier read's window is not reused");
				assertEquals("5000", totalParser.getRecords().get(0).get("CountSucc"), "the totals keep the earlier read");
			}
			final TerminalStepEntry terminal = mgr.getTerminalSteps().stream()
							.filter(entry -> entry.distributed && entry.stepId.equals(stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(SteadyStatus.INCOMPLETE, terminal.steady.status());
		} finally {
			ThreadContext.remove(KEY_HOME_DIR);
			steadyLogger.removeAppender(steadyRows);
			totalLogger.removeAppender(totalRows);
			steadyRows.stop();
			totalRows.stop();
		}
	}

	private static String joined(final List<String> messages) {
		return String.join(System.lineSeparator(), messages) + System.lineSeparator();
	}

	private static DistributedMetricsContext distributedContext(
					final String stepId, final OpType opType, final SteadyResult steady) {
		final DistributedAllMetricsSnapshotImpl whole = snapshot(5_000L, 20_480_000L, 60_000);
		final DistributedAllMetricsSnapshotImpl snapshot = new DistributedAllMetricsSnapshotImpl(
						whole.durationSnapshot(),
						whole.latencySnapshot(),
						whole.ttfbSnapshot(),
						whole.concurrencySnapshot(),
						whole.failsSnapshot(),
						whole.corruptSnapshot(),
						whole.successSnapshot(),
						whole.byteSnapshot(),
						1,
						60_000,
						null,
						steady);
		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.quantileValues()).thenReturn(List.of(0.5, 0.9, 0.99, 0.999));
		when(ctx.nodeCount()).thenReturn(1);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 0L));
		when(ctx.concurrencyLimit()).thenReturn(8);
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
		return ctx;
	}

	private static DistributedAllMetricsSnapshotImpl snapshot(
					final long successCount, final long byteCount, final long elapsedMillis) {
		final double seconds = elapsedMillis / 1000.0;
		return new DistributedAllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, List.of(1_000L, 2_000L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, List.of(500L, 900L)),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 8, 8.0),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, elapsedMillis),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedMillis),
						new RateMetricSnapshotImpl(
										successCount / seconds, successCount / seconds, MetricsConstants.METRIC_NAME_SUCC,
										successCount, elapsedMillis),
						new RateMetricSnapshotImpl(
										byteCount / seconds, byteCount / seconds, MetricsConstants.METRIC_NAME_BYTE, byteCount,
										elapsedMillis),
						1,
						elapsedMillis,
						null);
	}

	private static void configureSortOrder(final List<DistributedMetricsContext> contexts) {
		final IdentityHashMap<DistributedMetricsContext, Integer> orderByContext = new IdentityHashMap<>();
		for (int i = 0; i < contexts.size(); i++) {
			orderByContext.put(contexts.get(i), i);
		}
		for (final DistributedMetricsContext ctx : contexts) {
			when(ctx.compareTo(any())).thenAnswer(invocation -> {
				final Integer left = orderByContext.get(ctx);
				final Integer right = orderByContext.get(invocation.getArgument(0));
				return Integer.compare(left == null ? -1 : left, right == null ? -1 : right);
			});
		}
	}

	private static List<String> await(final CapturingAppender appender, final int rows) throws Exception {
		for (int attempt = 0; attempt < 100; attempt++) {
			LogUtil.flushAll();
			final List<String> messages = appender.messages();
			if (messages.size() >= rows) {
				return messages;
			}
			Thread.sleep(20);
		}
		final List<String> messages = appender.messages();
		assertTrue(messages.size() >= rows, "expected " + rows + " rows, got " + messages);
		return messages;
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final String stepId;
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		CapturingAppender(final String stepId, final String name) {
			super(name, null, null, true, Property.EMPTY_ARRAY);
			this.stepId = stepId;
		}

		@Override
		public void append(final LogEvent event) {
			if (stepId.equals(event.getContextData().getValue(KEY_STEP_ID))) {
				captured.add(event.getMessage().getFormattedMessage());
			}
		}

		List<String> messages() {
			synchronized (captured) {
				return List.copyOf(captured);
			}
		}
	}
}
