package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.stubbing.Stubber;

/**
 * The records a duration-mode standalone DELETE {@code run()} writes when its stop is incomplete,
 * through the real metrics manager and distributed context: one {@code # Results} block, one
 * terminal entry and one row in each metrics file, which agree with each other and with how the run
 * ends. The slice reports 5 of 10 objects accepted, unreconciled, until its drain starts, and 10 of
 * 10, reconciled, after it.
 */
@Timeout(60)
class DurationCloseRetryRecordsTest {

	private static final String OUTCOME = "Outcome:";
	private static final String ACCEPTED = "      Accepted:      ";
	private static final String RECONCILED = "Terminal Reconciled:";

	private final MetricsManagerImpl metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final Capture results = new Capture("durationRecordsResults");
	private final Capture messages = new Capture("durationRecordsMessages");
	private final Capture totalCsv = new Capture("durationRecordsTotalCsv");
	private final Capture steadyCsv = new Capture("durationRecordsSteadyCsv");
	private final Capture resultXml = new Capture("durationRecordsResultXml");
	private final List<Runnable> detach = new ArrayList<>();
	private final AtomicBoolean drained = new AtomicBoolean();
	private MetricsContextImpl<?> worker;
	private DistributedMetricsContextImpl<?> fleet;
	private TestLoadStepClient client;

	@BeforeEach
	void setUp() {
		metricsManager.setTerminalRetentionMillis(60_000L);
		attach(Loggers.METRICS_STD_OUT.getName(), results);
		attach(Loggers.MSG.getName(), messages);
		attach(Loggers.METRICS_FILE_TOTAL.getName(), totalCsv);
		attach(Loggers.METRICS_STEADY_FILE_TOTAL.getName(), steadyCsv);
		attach(Loggers.METRICS_EXT_RESULTS_FILE.getName(), resultXml);
	}

	@AfterEach
	void tearDown() throws Exception {
		Thread.interrupted();
		JvmExitTestSupport.reset();
		detach.forEach(Runnable::run);
		if (fleet != null) {
			fleet.close();
		}
		if (worker != null) {
			worker.close();
		}
		metricsManager.close();
	}

	@Test
	void anAdmissionCloseRetryThatSucceedsWritesCleanRecordsWithTheDrainedCounts() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		build(slice);

		assertDoesNotThrow(client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, "completed cleanly", 10, true);
		assertTrue(client.isClosed());
	}

	@Test
	void aSliceStopRetryThatSucceedsWritesCleanRecordsWithTheDrainedCounts() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(1).when(slice).stop();
		build(slice);

		assertDoesNotThrow(client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, "completed cleanly", 10, true);
		assertTrue(client.isClosed());
	}

	@Test
	void retriesThatRunOutWriteFailedRecordsAndTheRunFails() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(Integer.MAX_VALUE).when(slice).closeOperationAdmissionForStepStop();
		build(slice);

		assertThrows(IntegrityTerminalException.class, client::run);

		// The admission never closes, so the drain never starts: the best counts are the pre-drain ones.
		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 5, false);
		assertEquals(0, messages.count("completed cleanly"));
	}

	@Test
	void aSliceStopThatFailsOnEveryAttemptWritesFailedRecordsAndTheRunFails() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(Integer.MAX_VALUE).when(slice).stop();
		build(slice);

		final IntegrityTerminalException failure = assertThrows(IntegrityTerminalException.class, client::run);

		assertTrue(failure.getMessage().contains("failed to stop"), failure.getMessage());
		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 10, true);
		assertEquals(0, messages.count("completed cleanly"),
						"no record says completed cleanly while the run fails");
	}

	@Test
	void aSliceCloseThatFailsOnEveryAttemptWritesFailedRecordsAndTheRunFails() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(Integer.MAX_VALUE).when(slice).close();
		build(slice);

		final IntegrityTerminalException failure = assertThrows(IntegrityTerminalException.class, client::run);

		assertTrue(failure.getMessage().contains("failed to close a step slice"), failure.getMessage());
		// The stop completed and drained: the records carry the drained counts, and the outcome the close's failure.
		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 10, true);
	}

	@Test
	void anAdmissionCloseRetryWhoseSliceCloseFailsOnEveryAttemptWritesFailedRecordsAndTheRunFails()
					throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		failFirstCalls(Integer.MAX_VALUE).when(slice).close();
		build(slice);

		assertThrows(IntegrityTerminalException.class, client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 10, true);
	}

	@Test
	void aSliceCloseRetryThatSucceedsWritesCleanRecordsAndTheRunSucceeds() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(1).when(slice).close();
		build(slice);

		assertDoesNotThrow(client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, "completed cleanly", 10, true);
		assertTrue(client.isClosed());
	}

	@Test
	void anEngineExitAtAFailedSliceCloseWritesFailedRecordsAndTheRetryClosesTheStep() throws Exception {
		final LoadStep slice = slice();
		final AtomicInteger closes = new AtomicInteger();
		doAnswer(invocation -> {
			if (closes.incrementAndGet() == 1) {
				JvmExit.markBegun();
				throw new RemoteException("slice close fails as the engine exits");
			}
			return null;
		}).when(slice).close();
		build(slice);

		// The failed close had no close of the run to wait for: it wrote failed, and the run keeps it.
		assertThrows(IntegrityTerminalException.class, client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 10, true);
		assertTrue(client.isClosed(), "the retry whose cleanup succeeds closes the step");
	}

	@Test
	void anInterruptDuringACloseRetryWritesFailedRecordsAndReleasesTheSlice() throws Exception {
		final LoadStep slice = slice();
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		final CountDownLatch stopEntered = new CountDownLatch(1);
		final CountDownLatch neverReleased = new CountDownLatch(1);
		doAnswer(invocation -> {
			stopEntered.countDown();
			neverReleased.await();
			return null;
		}).when(slice).stop();
		build(slice);
		final AtomicReference<Throwable> runFailure = new AtomicReference<>();
		final Thread runner = Thread.ofPlatform().start(() -> {
			try {
				client.run();
			} catch (final Throwable failure) {
				runFailure.set(failure);
			}
		});
		try {
			assertTrue(stopEntered.await(10, TimeUnit.SECONDS), "the close retry reaches the slice's stop");
			runner.interrupt();
			runner.join(TimeUnit.SECONDS.toMillis(30));
			assertFalse(runner.isAlive());

			assertInstanceOf(InterruptedException.class, runFailure.get());
			// The retry drained before its slice stop: the records carry the drained counts.
			assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 10, true);
			verify(slice).close();
		} finally {
			neverReleased.countDown();
		}
	}

	@Test
	void anEngineExitAtTheStopWritesFailedRecordsAndTheRetryClosesTheStep() throws Exception {
		final LoadStep slice = slice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			JvmExit.markBegun();
			return false;
		});
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		build(slice);

		assertThrows(IntegrityTerminalException.class, client::run);

		assertRecords(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, "failed", 5, false);
		assertTrue(client.isClosed(), "the retry whose cleanup succeeds closes the step");
		verify(slice).close();
	}

	// ---- harness ----

	/**
	 * One {@code # Results} block, terminal entry and row in each metrics file, all carrying the
	 * outcome and the DELETE counts given, and no metrics context left registered.
	 */
	private void assertRecords(
					final String outcome, final String resultsOutcome, final long accepted, final boolean reconciled)
					throws Exception {
		awaitResults();
		assertEquals(List.of(OUTCOME + " " + resultsOutcome), lines(results, OUTCOME), "# Results outcome");
		assertEquals(List.of("Accepted: " + accepted), lines(results, ACCEPTED), "# Results accepted objects");
		assertEquals(List.of(RECONCILED + " " + reconciled), lines(results, RECONCILED), "# Results reconciled");
		final TerminalStepEntry entry = metricsManager.getTerminalSteps().stream()
						.filter(candidate -> candidate.distributed && client.loadStepId().equals(candidate.stepId))
						.reduce((first, second) -> {
							throw new AssertionError("two terminal entries");
						})
						.orElseThrow();
		assertEquals(outcome, entry.deleteMetrics.failureOutcome(), "terminal entry outcome");
		assertEquals(accepted, entry.deleteMetrics.objectAccepted(), "terminal entry accepted objects");
		assertEquals(reconciled, entry.deleteMetrics.reconciled(), "terminal entry reconciled");
		assertEquals(1, totalCsv.messages().size(), "metrics.total.csv rows");
		assertEquals(1, steadyCsv.messages().size(), "metrics.steady.total.csv rows");
		assertEquals(1, resultXml.count("<result "), "result.xml blocks");
		assertEquals(0, registeredContexts(), "no metrics context stays registered as running");
	}

	private void awaitResults() throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (lines(results, OUTCOME).isEmpty() && System.nanoTime() < deadlineNanos) {
			Thread.sleep(10);
		}
		// A block written twice would show here.
		Thread.sleep(100);
	}

	private int registeredContexts() throws Exception {
		final Field all = MetricsManagerImpl.class.getDeclaredField("allMetrics");
		all.setAccessible(true);
		return ((Collection<?>) all.get(metricsManager)).size();
	}

	private void build(final LoadStep slice) throws Exception {
		final Config config = durationConfig();
		config.val("load-step-limit-time", "1s");
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		client = new TestLoadStepClient(config, List.of(factory), Collections.emptyList(), metricsManager);
		worker = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId(client.loadStepId())
						.opType(OpType.DELETE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker")
						.runId(1)
						.build();
		worker.start();
		worker.markSucc(0, 1, 1);
		worker.refreshLastSnapshot();
		final AllMetricsSnapshot beforeDrain = withDeleteMetrics(worker.lastSnapshot(), 5, false);
		final AllMetricsSnapshot afterDrain = withDeleteMetrics(worker.lastSnapshot(), 10, true);
		fleet = (DistributedMetricsContextImpl<?>) DistributedMetricsContextImpl.builder()
						.loadStepId(client.loadStepId())
						.opType(OpType.DELETE)
						.nodeCountSupplier(() -> 1)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(true)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(drained.get() ? afterDrain : beforeDrain))
						.quantileValues(List.of(0.5))
						.nodeAddrs(List.of("local"))
						.deleteDetailsExpected(true)
						.comment("entry")
						.runId(1)
						.build();
		fleet.metadata().put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME,
						MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING);
		// The step's start registers it.
		client.addMetricsContextForTest(fleet);
	}

	private static AllMetricsSnapshot withDeleteMetrics(
					final AllMetricsSnapshot delegate, final long accepted, final boolean reconciled) {
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class);
		when(snapshot.durationSnapshot()).thenReturn(delegate.durationSnapshot());
		when(snapshot.latencySnapshot()).thenReturn(delegate.latencySnapshot());
		when(snapshot.ttfbSnapshot()).thenReturn(delegate.ttfbSnapshot());
		when(snapshot.concurrencySnapshot()).thenReturn(delegate.concurrencySnapshot());
		when(snapshot.failsSnapshot()).thenReturn(delegate.failsSnapshot());
		when(snapshot.corruptSnapshot()).thenReturn(delegate.corruptSnapshot());
		when(snapshot.successSnapshot()).thenReturn(delegate.successSnapshot());
		when(snapshot.byteSnapshot()).thenReturn(delegate.byteSnapshot());
		when(snapshot.elapsedTimeMillis()).thenReturn(delegate.elapsedTimeMillis());
		when(snapshot.deleteMetrics()).thenReturn(DeleteMetricsSnapshot.builder(1)
						.requests(accepted, accepted, 0, 0, 0, 1)
						.objects(10, accepted, accepted, 0, 10 - accepted, 0, 0)
						.failureOutcome(MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING)
						.reconciled(reconciled)
						.build());
		return snapshot;
	}

	private LoadStep slice() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("slice-0");
		// The slice runs to the deadline.
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		when(slice.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(slice.isDeleteInventoryPreValidationComplete()).thenReturn(true);
		when(slice.isDeleteInventoryVerificationCompleteForStepStop()).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(new DeleteObjectLifecycleSnapshot(10, 10, 10, 0, 0, 0, 0, 10, true));
		doAnswer(invocation -> {
			drained.set(true);
			return null;
		}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		return slice;
	}

	private static Stubber failFirstCalls(final int failures) {
		final AtomicInteger calls = new AtomicInteger();
		return doAnswer(invocation -> {
			if (calls.incrementAndGet() <= failures) {
				throw new RemoteException("transient failure " + calls.get());
			}
			return null;
		});
	}

	private static Config durationConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "duration-close-retry-records");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		return config;
	}

	/** Each captured line that contains the needle, stripped, with its runs of spaces made single. */
	private static List<String> lines(final Capture capture, final String needle) {
		final List<String> found = new ArrayList<>();
		for (final String message : capture.messages()) {
			for (final String line : message.lines().toList()) {
				if (line.contains(needle)) {
					found.add(line.strip().replaceAll("\\s+", " "));
				}
			}
		}
		return found;
	}

	private void attach(final String loggerName, final Capture capture) {
		final Logger logger = LoggerContext.getContext(false).getLogger(loggerName);
		capture.start();
		logger.addAppender(capture);
		detach.add(() -> {
			logger.removeAppender(capture);
			capture.stop();
		});
	}

	private static final class Capture extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		private Capture(final String name) {
			super(name, null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			captured.add(event.getMessage().getFormattedMessage());
		}

		private List<String> messages() {
			synchronized (captured) {
				return List.copyOf(captured);
			}
		}

		private long count(final String needle) {
			return messages().stream().filter(message -> message.contains(needle)).count();
		}
	}
}
