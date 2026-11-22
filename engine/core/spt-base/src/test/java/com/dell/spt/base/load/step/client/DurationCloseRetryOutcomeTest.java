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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.mockito.InOrder;
import org.mockito.stubbing.Stubber;

/**
 * A duration-mode standalone DELETE whose stop is incomplete writes its metrics output and
 * publishes its outcome once, and every record agrees. When {@code run()} has a close attempt
 * left, the output waits for the close that settles cleanup and carries that close's outcome, with
 * the counts captured after the drain; when no close attempt is left, or none owns a retry, the
 * stop writes it with {@code failed}, which stays final while later closes still clean up.
 */
@Timeout(60)
class DurationCloseRetryOutcomeTest {

	private static final String RESULTS_OUTCOME = "Outcome:                   ";

	private final MetricsManagerImpl metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final CapturingAppender results = new CapturingAppender("durationCloseRetryResults");
	private final CapturingAppender messages = new CapturingAppender("durationCloseRetryMessages");
	private final Logger stdOut = LoggerContext.getContext(false).getLogger(Loggers.METRICS_STD_OUT.getName());
	private final Logger msgLog = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
	private MetricsContextImpl<?> worker;
	private DistributedMetricsContextImpl<?> fleet;

	@BeforeEach
	void setUp() {
		metricsManager.setTerminalRetentionMillis(60_000L);
		results.start();
		messages.start();
		stdOut.addAppender(results);
		msgLog.addAppender(messages);
	}

	@AfterEach
	void tearDown() throws Exception {
		Thread.interrupted();
		JvmExitTestSupport.reset();
		stdOut.removeAppender(results);
		msgLog.removeAppender(messages);
		results.stop();
		messages.stop();
		if (fleet != null) {
			fleet.close();
		}
		if (worker != null) {
			worker.close();
		}
		metricsManager.close();
	}

	// ---- run(): a close attempt follows, so the output waits for the close that settles cleanup ----

	@Test
	void aRunWhoseCloseRetrySucceedsWritesOneCleanOutputAfterTheDrain() throws Exception {
		final LoadStep slice = runSlice();
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		final RunHarness run = new RunHarness(slice);

		assertDoesNotThrow(run.client::run);

		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, run.lastPublishedOutcome());
		verify(run.metricsManager, never()).updateTerminalDeleteFailureOutcome(
						anyString(), eq(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED));
		final InOrder drainThenOutput = inOrder(slice, run.metricsManager);
		drainThenOutput.verify(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		drainThenOutput.verify(run.metricsManager).unregister(run.context);
		verify(slice).close();
		assertTrue(run.client.isClosed());
	}

	@Test
	void aRunWhoseRecoveryRetrySucceedsWritesOneCleanOutputAfterTheDrain() throws Exception {
		final LoadStep slice = runSlice();
		failFirstCalls(1).when(slice).recoverQueuedOperationsForStepStop();
		final RunHarness run = new RunHarness(slice);

		assertDoesNotThrow(run.client::run);

		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, run.lastPublishedOutcome());
		final InOrder drainThenOutput = inOrder(slice, run.metricsManager);
		drainThenOutput.verify(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		drainThenOutput.verify(run.metricsManager).unregister(run.context);
		assertTrue(run.client.isClosed());
	}

	@Test
	void aRunWhoseLastCloseRetrySucceedsStillWritesOneCleanOutput() throws Exception {
		final LoadStep slice = runSlice();
		// The await's stop and the first retry fail; the last of the run's close attempts succeeds.
		failFirstCalls(2).when(slice).closeOperationAdmissionForStepStop();
		final RunHarness run = new RunHarness(slice);

		assertDoesNotThrow(run.client::run);

		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, run.lastPublishedOutcome());
		verify(slice, times(3)).closeOperationAdmissionForStepStop();
		assertTrue(run.client.isClosed());
	}

	@Test
	void aRunWhoseCloseRetriesRunOutWritesOneFailedOutputAndFails() throws Exception {
		final LoadStep slice = runSlice();
		failFirstCalls(Integer.MAX_VALUE).when(slice).closeOperationAdmissionForStepStop();
		final RunHarness run = new RunHarness(slice);

		assertThrows(IntegrityTerminalException.class, run.client::run);

		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, run.lastPublishedOutcome());
		verify(run.metricsManager, never()).updateTerminalDeleteFailureOutcome(
						anyString(),
						eq(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY));
	}

	@Test
	void aRunWhoseSliceStopIsInterruptedEndsWithTheRetrysOutcome() throws Exception {
		final LoadStep slice = runSlice();
		final CountDownLatch stopEntered = new CountDownLatch(1);
		final CountDownLatch releaseStop = new CountDownLatch(1);
		final AtomicInteger stops = new AtomicInteger();
		doAnswer(invocation -> {
			if (stops.incrementAndGet() == 1) {
				stopEntered.countDown();
				awaitIgnoringInterrupt(releaseStop);
			}
			return null;
		}).when(slice).stop();
		final RunHarness run = new RunHarness(slice);
		final AtomicReference<Throwable> runFailure = new AtomicReference<>();
		final Thread runner = Thread.ofPlatform().start(() -> {
			try {
				run.client.run();
			} catch (final Throwable failure) {
				runFailure.set(failure);
			}
		});
		try {
			assertTrue(stopEntered.await(10, TimeUnit.SECONDS), "the await's stop reaches the slice's stop");
			// A stop request while the slice is still stopping.
			runner.interrupt();
			// Let the interrupted stop end, then release the slice, so a close retry can settle cleanup.
			Thread.sleep(200);
		} finally {
			releaseStop.countDown();
		}
		runner.join(TimeUnit.SECONDS.toMillis(30));
		assertFalse(runner.isAlive());

		assertInstanceOf(InterruptedException.class, runFailure.get());
		assertEquals(1, run.outcomesAtOutput.size(), "one output: " + run.outcomesAtOutput);
		assertEquals(run.lastPublishedOutcome(), run.outcomesAtOutput.get(0),
						"the output carries the outcome the step ends with");
	}

	@Test
	void aRunInterruptedDuringACloseRetryStillWritesItsOutputFailed() throws Exception {
		final LoadStep slice = runSlice();
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		final CountDownLatch stopEntered = new CountDownLatch(1);
		final CountDownLatch releaseStop = new CountDownLatch(1);
		doAnswer(invocation -> {
			stopEntered.countDown();
			awaitIgnoringInterrupt(releaseStop);
			return null;
		}).when(slice).stop();
		final RunHarness run = new RunHarness(slice);
		final AtomicReference<Throwable> runFailure = new AtomicReference<>();
		final Thread runner = Thread.ofPlatform().start(() -> {
			try {
				run.client.run();
			} catch (final Throwable failure) {
				runFailure.set(failure);
			}
		});
		try {
			assertTrue(stopEntered.await(10, TimeUnit.SECONDS), "the close retry reaches the slice's stop");
			runner.interrupt();
			runner.join(TimeUnit.SECONDS.toMillis(30));
			assertFalse(runner.isAlive());
		} finally {
			releaseStop.countDown();
		}

		assertInstanceOf(InterruptedException.class, runFailure.get());
		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED), run.outcomesAtOutput,
						"no metrics context stays registered as running");
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, run.lastPublishedOutcome());
	}

	@Test
	void aRunWhoseEngineIsExitingWritesItsOutputAtTheStopAndEndsFailed() throws Exception {
		final LoadStep slice = cleanSlice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			// The exit hook has begun: no later close of the run can be counted on.
			JvmExit.markBegun();
			return false;
		});
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		final RunHarness run = new RunHarness(slice);

		assertThrows(IntegrityTerminalException.class, run.client::run);

		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, run.lastPublishedOutcome());
		verify(run.metricsManager, never()).updateTerminalDeleteFailureOutcome(
						anyString(),
						eq(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY));
		verify(slice, atLeastOnce()).close();
	}

	@Test
	void aCloseRetryThatFailsOnceTheEngineIsExitingWritesTheOutputItWasLeft() throws Exception {
		final LoadStep slice = runSlice();
		final AtomicInteger admissionCloses = new AtomicInteger();
		doAnswer(invocation -> {
			switch (admissionCloses.incrementAndGet()) {
			case 1 -> throw new RemoteException("transient failure at the await's stop");
			case 2 -> {
				// The exit hook begins while the run's first close retry fails: it may be the last close.
				JvmExit.markBegun();
				throw new RemoteException("transient failure at the first close retry");
			}
			default -> {
				return null;
			}
			}
		}).when(slice).closeOperationAdmissionForStepStop();
		final RunHarness run = new RunHarness(slice);
		final List<Integer> admissionClosesAtOutput = new CopyOnWriteArrayList<>();
		doAnswer(invocation -> {
			if (run.registered.getAndSet(false)) {
				admissionClosesAtOutput.add(admissionCloses.get());
				run.outcomesAtOutput.add(String.valueOf(
								run.metadata.get(MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME)));
			}
			return null;
		}).when(run.metricsManager).unregister(run.context);

		assertThrows(IntegrityTerminalException.class, run.client::run);

		assertEquals(List.of(2), admissionClosesAtOutput, "the failing close writes the output");
		assertEquals(List.of(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED), run.outcomesAtOutput);
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, run.lastPublishedOutcome());
	}

	// ---- no close attempt owned by run(): the stop writes the output, and failed stays final ----

	@Test
	void aDirectCloseRetryAfterAFailedAdmissionCloseKeepsTheFailedOutcomeItWrote() throws Exception {
		final TestLoadStepClient client = client(durationConfig());
		final LoadStep slice = cleanSlice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		failFirstCalls(1).when(slice).closeOperationAdmissionForStepStop();
		addSlice(client, slice);

		assertFalse(client.await(1, TimeUnit.MILLISECONDS));
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
		assertThrows(IntegrityTerminalException.class, client::close);

		assertDoesNotThrow(client::close);

		assertTrue(client.isClosed(), "the retry still cleans up and closes");
		verify(slice).close();
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, terminalOutcome(client),
						"the terminal entry keeps the outcome the output carries");
		assertEquals(0, messages.count("completed cleanly"));
	}

	@Test
	void aDirectCloseRetryAfterAnInterruptedSliceStopKeepsTheFailedOutcomeItWrote() throws Exception {
		final TestLoadStepClient client = client(durationConfig());
		final CountDownLatch stopEntered = new CountDownLatch(1);
		final CountDownLatch releaseStop = new CountDownLatch(1);
		final LoadStep slice = cleanSlice();
		doAnswer(invocation -> {
			stopEntered.countDown();
			awaitIgnoringInterrupt(releaseStop);
			return null;
		}).when(slice).stop();
		addSlice(client, slice);
		final Thread closer = Thread.ofPlatform().start(() -> closeIgnoringFailure(client));
		try {
			assertTrue(stopEntered.await(10, TimeUnit.SECONDS));
			closer.interrupt();
			closer.join(TimeUnit.SECONDS.toMillis(30));
			assertFalse(closer.isAlive());
		} finally {
			releaseStop.countDown();
		}

		for (int attempt = 0; attempt < 3 && !client.isClosed(); attempt++) {
			closeIgnoringFailure(client);
		}

		assertTrue(client.isClosed(), "the retries still clean up and close");
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, terminalOutcome(client));
		assertEquals(0, messages.count("completed cleanly"));
	}

	// ---- harness ----

	/**
	 * A run() of one factory-made slice, with a mocked metrics manager that records the outcome its
	 * context carries when its output is written: at the first unregister, as the real manager writes it.
	 */
	private static final class RunHarness {
		final MetricsManager metricsManager = mock(MetricsManager.class);
		final MetricsContext<?> context = mock(MetricsContext.class);
		final Map<String, Object> metadata = new HashMap<>();
		final List<String> outcomesAtOutput = new CopyOnWriteArrayList<>();
		final List<String> publishedOutcomes = new CopyOnWriteArrayList<>();
		final AtomicBoolean registered = new AtomicBoolean(true);
		final TestLoadStepClient client;

		RunHarness(final LoadStep slice) throws Exception {
			final Config config = durationConfig();
			config.val("load-step-limit-time", "1s");
			config.val("item-data-input-compressibility", 0.0);
			config.val("item-data-dedupable", false);
			config.val("item-data-verify", false);
			@SuppressWarnings("unchecked")
			final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
			when(factory.id()).thenReturn("test-load-step");
			when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
			when(context.metadata()).thenAnswer(invocation -> metadata);
			when(context.opType()).thenReturn(OpType.DELETE);
			doAnswer(invocation -> {
				if (registered.getAndSet(false)) {
					outcomesAtOutput.add(String.valueOf(metadata.get(MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME)));
				}
				return null;
			}).when(metricsManager).unregister(context);
			doAnswer(invocation -> {
				publishedOutcomes.add(invocation.getArgument(1));
				return null;
			}).when(metricsManager).updateTerminalDeleteFailureOutcome(anyString(), anyString());
			client = new TestLoadStepClient(config, List.of(factory), Collections.emptyList(), metricsManager);
			client.addMetricsContextForTest(context);
		}

		String lastPublishedOutcome() {
			return publishedOutcomes.isEmpty() ? null : publishedOutcomes.get(publishedOutcomes.size() - 1);
		}
	}

	private static LoadStep runSlice() throws Exception {
		final LoadStep slice = cleanSlice();
		// The slice runs to the deadline.
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
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

	private TestLoadStepClient client(final Config config) {
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, Collections.emptyList(), Collections.emptyList(), metricsManager);
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
		final AllMetricsSnapshot nodeSnapshot = withDeleteMetrics(worker.lastSnapshot());
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
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(nodeSnapshot))
						.quantileValues(List.of(0.5))
						.nodeAddrs(List.of("local"))
						.deleteDetailsExpected(true)
						.comment("entry")
						.runId(1)
						.build();
		fleet.metadata().put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME,
						MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING);
		fleet.start();
		client.addMetricsContextForTest(fleet);
		metricsManager.register(fleet);
		return client;
	}

	private static AllMetricsSnapshot withDeleteMetrics(final AllMetricsSnapshot delegate) {
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
						.requests(1, 1, 0, 0, 0, 1)
						.objects(1, 1, 1, 0, 0, 0, 0)
						.failureOutcome(MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING)
						.build());
		return snapshot;
	}

	private static LoadStep cleanSlice() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("slice-0");
		when(slice.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(slice.isDeleteInventoryPreValidationComplete()).thenReturn(true);
		when(slice.isDeleteInventoryVerificationCompleteForStepStop()).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		return slice;
	}

	private static DeleteObjectLifecycleSnapshot cleanDeleteLifecycle() {
		return new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, true);
	}

	private static Config durationConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "duration-close-retry");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private Optional<TerminalStepEntry> terminalEntry(final TestLoadStepClient client) {
		return metricsManager.getTerminalSteps().stream()
						.filter(entry -> entry.distributed && client.loadStepId().equals(entry.stepId))
						.findFirst();
	}

	private String terminalOutcome(final TestLoadStepClient client) {
		return terminalEntry(client).orElseThrow().deleteMetrics.failureOutcome();
	}

	/** The failure-policy outcome line of each {@code # Results} block written so far. */
	private List<String> resultsOutcomes() {
		final List<String> outcomes = new ArrayList<>();
		for (final String message : results.messages()) {
			for (final String line : message.lines().toList()) {
				final int at = line.indexOf(RESULTS_OUTCOME);
				if (at >= 0) {
					outcomes.add(line.substring(at).strip());
				}
			}
		}
		return outcomes;
	}

	/** As {@link #resultsOutcomes()}, once the console has delivered at least the expected blocks. */
	private List<String> resultsOutcomes(final int expected) throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (resultsOutcomes().size() < expected && System.nanoTime() < deadlineNanos) {
			Thread.sleep(10);
		}
		// A block written twice would show here.
		Thread.sleep(100);
		return resultsOutcomes();
	}

	private static void closeIgnoringFailure(final TestLoadStepClient client) {
		try {
			client.close();
		} catch (final Exception expected) {
			// The step's own failure: only the outputs matter here.
		}
	}

	private static void addSlice(final LoadStepClientBase<?> client, final LoadStep slice) throws Exception {
		list(client, "stepSlices").add(slice);
	}

	@SuppressWarnings("unchecked")
	private static List<Object> list(final LoadStepClientBase<?> client, final String name) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField(name);
		field.setAccessible(true);
		return (List<Object>) field.get(client);
	}

	private static void awaitIgnoringInterrupt(final CountDownLatch release) {
		boolean interrupted = false;
		while (release.getCount() > 0) {
			try {
				release.await();
			} catch (final InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		private CapturingAppender(final String name) {
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
