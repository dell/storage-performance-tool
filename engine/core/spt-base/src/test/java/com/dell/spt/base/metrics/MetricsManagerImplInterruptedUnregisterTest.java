package com.dell.spt.base.metrics;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A stopped distributed step calls {@link MetricsManagerImpl#unregister} with the interrupt flag
 * still set. The step's totals, {@code result.xml} and terminal entry must still be written, and
 * the caller must still see the interrupt afterwards.
 */
class MetricsManagerImplInterruptedUnregisterTest {

	@TempDir
	Path tempDir;

	@AfterEach
	void clearInterrupt() {
		// Never leak the flag into the next test on this thread.
		Thread.interrupted();
		ThreadContext.remove(KEY_HOME_DIR);
	}

	@Test
	void interruptedUnregisterWritesTotalsAndKeepsTheInterrupt() throws Exception {
		final Output output = unregisterOnce("interrupted-unregister-step", true);
		assertTrue(output.interruptedAfter, "the caller's interrupt flag must be restored");
		assertWritten(output);
	}

	@Test
	void uninterruptedUnregisterGivesTheSameOutput() throws Exception {
		final Output interrupted = unregisterOnce("unregister-step-a", true);
		Thread.interrupted();
		final Output control = unregisterOnce("unregister-step-b", false);
		assertFalse(control.interruptedAfter, "an uninterrupted caller must not become interrupted");
		assertWritten(control);
		assertEquals(dataColumns(control.totals.get(1)), dataColumns(interrupted.totals.get(1)));
		assertEquals(control.resultXml.size(), interrupted.resultXml.size());
		assertEquals(control.successCount, interrupted.successCount);
		assertEquals(control.bytesTotal, interrupted.bytesTotal);
	}

	private static void assertWritten(final Output output) {
		assertEquals(2, output.totals.size(), "header and totals row: " + output.totals);
		assertTrue(output.totals.get(0).startsWith("DateTimeISO8601,"), output.totals.toString());
		assertEquals(3, output.resultXml.size(), "<result>, entry, </result>: " + output.resultXml);
		assertEquals("</result>", output.resultXml.get(2));
		assertEquals(42L, output.successCount);
		assertEquals(172_032L, output.bytesTotal);
	}

	/** Drops the leading timestamp, which differs between runs. */
	private static String dataColumns(final String csvRow) {
		final int opTypeColumn = csvRow.indexOf(",CREATE,");
		assertTrue(opTypeColumn > 0, csvRow);
		return csvRow.substring(opTypeColumn);
	}

	private Output unregisterOnce(final String stepId, final boolean interrupt) throws Exception {
		final CapturingAppender totals = new CapturingAppender("interruptedTotalsCapture", stepId);
		final CapturingAppender resultXml = new CapturingAppender("interruptedResultXmlCapture", stepId);
		final Logger totalsLogger = attach(Loggers.METRICS_FILE_TOTAL.getName(), totals);
		final Logger resultXmlLogger = attach(Loggers.METRICS_EXT_RESULTS_FILE.getName(), resultXml);
		ThreadContext.put(KEY_HOME_DIR, tempDir.toString());
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);
		try {
			final DistributedMetricsContext<?> ctx = distributedContext(stepId);
			mgr.register(ctx);
			if (interrupt) {
				Thread.currentThread().interrupt();
			}
			mgr.unregister(ctx);
			final boolean interruptedAfter = Thread.interrupted();
			final List<TerminalStepEntry> entries = mgr.getTerminalSteps().stream()
							.filter(entry -> stepId.equals(entry.stepId))
							.toList();
			assertEquals(1, entries.size(), "terminal entry: " + entries);
			awaitMessages(totals, 2);
			awaitMessages(resultXml, 3);
			return new Output(
							interruptedAfter,
							totals.messages().stream().flatMap(String::lines).filter(line -> !line.isBlank()).toList(),
							resultXml.messages(),
							entries.get(0).successCount,
							entries.get(0).bytesTotal);
		} finally {
			totalsLogger.removeAppender(totals);
			resultXmlLogger.removeAppender(resultXml);
			totals.stop();
			resultXml.stop();
			mgr.close();
		}
	}

	private static Logger attach(final String loggerName, final CapturingAppender appender) {
		appender.start();
		final Logger logger = LoggerContext.getContext(false).getLogger(loggerName);
		logger.addAppender(appender);
		return logger;
	}

	private static void awaitMessages(final CapturingAppender appender, final int expected) throws Exception {
		for (int attempt = 0; attempt < 100; attempt++) {
			LogUtil.flushAll();
			if (appender.messages().stream().flatMap(String::lines).filter(line -> !line.isBlank()).count() >= expected) {
				return;
			}
			Thread.sleep(20);
		}
	}

	@SuppressWarnings("unchecked")
	private static DistributedMetricsContext<?> distributedContext(final String stepId) {
		final DistributedAllMetricsSnapshotImpl snapshot = snapshot(42L, 172_032L);
		final DistributedMetricsContext<?> ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.terminalSnapshot()).thenCallRealMethod();
		when(ctx.quantileValues()).thenReturn(List.of(0.5, 0.9, 0.99));
		when(ctx.nodeCount()).thenReturn(2);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 0L));
		when(ctx.concurrencyLimit()).thenReturn(4);
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(4_096L));
		when(ctx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(ctx.nodeAddrs()).thenReturn(List.of("127.0.0.1:1099", "127.0.0.2:1099"));
		when(ctx.comment()).thenReturn("");
		when(ctx.runId()).thenReturn(1L);
		when(ctx.sumPersistEnabled()).thenReturn(true);
		when(ctx.timingPersistEnabled()).thenReturn(false);
		when(ctx.outputPeriodMillis()).thenReturn(0L);
		when(ctx.thresholdStateEntered()).thenReturn(false);
		when(ctx.thresholdStateExited()).thenReturn(false);
		return ctx;
	}

	private static DistributedAllMetricsSnapshotImpl snapshot(final long successCount, final long byteCount) {
		return new DistributedAllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, List.of(210L, 260L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, List.of(120L, 180L)),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 4, 4.0),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, 1_000),
						new RateMetricSnapshotImpl(
										(double) successCount, (double) successCount, MetricsConstants.METRIC_NAME_SUCC, successCount, 1_000),
						new RateMetricSnapshotImpl(
										(double) byteCount, (double) byteCount, MetricsConstants.METRIC_NAME_BYTE, byteCount, 1_000),
						2,
						1_000);
	}

	private record Output(
					boolean interruptedAfter,
					List<String> totals,
					List<String> resultXml,
					long successCount,
					long bytesTotal) {}

	private static final class CapturingAppender extends AbstractAppender {
		private final String stepId;
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		CapturingAppender(final String name, final String stepId) {
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
