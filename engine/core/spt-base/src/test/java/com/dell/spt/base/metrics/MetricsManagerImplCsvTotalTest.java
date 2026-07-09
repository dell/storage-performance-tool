package com.dell.spt.base.metrics;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsManagerImplCsvTotalTest {

	@Test
	void writesSingleHeaderForMultipleOperationsInSameStep(@TempDir final Path tempDir) throws Exception {
		final String stepId = "mixed-step-csv-total";
		final CapturingAppender appender = new CapturingAppender(stepId);
		appender.start();
		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.METRICS_FILE_TOTAL.getName());
		logger.addAppender(appender);
		ThreadContext.put(KEY_HOME_DIR, tempDir.toString());
		try {
			final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
			final DistributedMetricsContext readCtx = distributedContext(
							stepId,
							OpType.READ,
							snapshot(11L, 45_056L, List.of(120L, 180L), List.of(210L, 260L), List.of(30L, 40L)));
			final DistributedMetricsContext statCtx = distributedContext(
							stepId,
							OpType.STAT,
							snapshot(5L, 0L, List.of(90L, 130L), List.of(120L, 180L), List.of()));
			final DistributedMetricsContext createCtx = distributedContext(
							stepId,
							OpType.CREATE,
							snapshot(7L, 28_672L, List.of(160L, 220L), List.of(250L, 300L), List.of()));
			final DistributedMetricsContext deleteCtx = distributedContext(
							stepId,
							OpType.DELETE,
							snapshot(3L, 0L, List.of(110L, 170L), List.of(140L, 210L), List.of()));
			final List<DistributedMetricsContext> contexts = List.of(readCtx, statCtx, createCtx, deleteCtx);
			configureSortOrder(contexts);

			for (final DistributedMetricsContext ctx : contexts) {
				mgr.register(ctx);
			}
			for (final DistributedMetricsContext ctx : contexts) {
				mgr.unregister(ctx);
			}

			final List<String> messages = awaitCsvMessages(appender, contexts.size());
			final String csvContent = String.join(System.lineSeparator(), messages) + System.lineSeparator();
			final List<String> lines = csvContent.lines().toList();
			assertEquals(1L, lines.stream().filter(line -> line.startsWith("DateTimeISO8601,")).count(), lines.toString());

			try (final CSVParser parser = CSVFormat.DEFAULT.builder()
							.setHeader()
							.setSkipHeaderRecord(true)
							.build()
							.parse(new StringReader(csvContent))) {
				final var records = parser.getRecords();
				final int headerSize = parser.getHeaderMap().size();
				assertNotEquals(null, parser.getHeaderMap().get("BWAvg[MiB/s]"));
				assertNotEquals(null, parser.getHeaderMap().get("BWLast[MiB/s]"));
				assertEquals(contexts.size(), records.size(), records.toString());
				assertEquals(List.of("READ", "STAT", "CREATE", "DELETE"), records.stream().map(record -> record.get("OpType")).toList());
				for (final var record : records) {
					assertEquals(headerSize, record.size(), record.toString());
					assertNotEquals("DateTimeISO8601", record.get(0), record.toString());
				}
			}
		} finally {
			ThreadContext.remove(KEY_HOME_DIR);
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	private static DistributedMetricsContext distributedContext(
					final String stepId,
					final OpType opType,
					final DistributedAllMetricsSnapshotImpl snapshot) {
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
		return ctx;
	}

	private static void configureSortOrder(final List<DistributedMetricsContext> contexts) {
		final Map<DistributedMetricsContext, Integer> orderByContext = new IdentityHashMap<>();
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

	private static List<String> awaitCsvMessages(final CapturingAppender appender, final int expectedDataRows) throws Exception {
		AssertionError lastFailure = null;
		for (int attempt = 0; attempt < 100; attempt++) {
			LogUtil.flushAll();
			final List<String> messages = appender.messages();
			if (messages.size() >= expectedDataRows) {
				return messages;
			}
			lastFailure = new AssertionError(
							"metrics total CSV events not complete yet: expected " + expectedDataRows
											+ " messages, got " + messages.size() + ": " + messages);
			Thread.sleep(50);
		}
		throw lastFailure;
	}

	private static class CapturingAppender extends AbstractAppender {
		private final String stepId;
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		CapturingAppender(final String stepId) {
			super("testMetricsTotalCsvCapture", null, null, true, Property.EMPTY_ARRAY);
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
