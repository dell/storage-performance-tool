package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregator;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

/**
 * A standalone DELETE stop publishes its failure-budget outcome and its metrics output once, from
 * the place that knows the final outcome: a stop request never leaves {@code Outcome: running} in
 * the step's {@code # Results} or its terminal entry, and the output never disagrees with the
 * outcome a later close publishes. The metrics manager and the step's fleet context are the real
 * ones; the slices are mocks.
 */
@Timeout(60)
class StopOutcomeConsistencyTest {

	private static final String RESULTS_OUTCOME = "Outcome:                   ";

	private final MetricsManagerImpl metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final CapturingAppender results = new CapturingAppender();
	private final Logger stdOut = LoggerContext.getContext(false).getLogger(Loggers.METRICS_STD_OUT.getName());
	private MetricsContextImpl<?> worker;
	private DistributedMetricsContextImpl<?> fleet;

	@BeforeEach
	void setUp() {
		metricsManager.setTerminalRetentionMillis(60_000L);
		results.start();
		stdOut.addAppender(results);
	}

	@AfterEach
	void tearDown() throws Exception {
		Thread.interrupted();
		stdOut.removeAppender(results);
		results.stop();
		if (fleet != null) {
			fleet.close();
		}
		if (worker != null) {
			worker.close();
		}
		metricsManager.close();
	}

	@Test
	void anInterruptedDurationSliceStopWritesNoOutputAndTheClosesStopWritesItFailed() throws Exception {
		final TestLoadStepClient client = client(durationConfig());
		final CountDownLatch stopEntered = new CountDownLatch(1);
		final CountDownLatch releaseStop = new CountDownLatch(1);
		final LoadStep slice = cleanSlice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		doAnswer(invocation -> {
			stopEntered.countDown();
			awaitIgnoringInterrupt(releaseStop);
			return invocation.getMock();
		}).when(slice).stop();
		addSlice(client, slice);
		final AtomicInteger timingCloses = new AtomicInteger();
		list(client, "itemTimingMetricsOutputFileAggregators").add((AutoCloseable) timingCloses::incrementAndGet);

		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread run = Thread.ofPlatform().start(() -> {
			try {
				client.await(1, TimeUnit.MILLISECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});
		try {
			assertTrue(stopEntered.await(10, TimeUnit.SECONDS), "the stop reaches the slice's stop");
			// The user stops the run while the slice is still stopping.
			run.interrupt();
			run.join(TimeUnit.SECONDS.toMillis(10));
			assertFalse(run.isAlive());

			assertInstanceOf(InterruptedException.class, awaitFailure.get());
			assertEquals(0, timingCloses.get(), "no output stage runs while the slice is still stopping");
			assertTrue(metricsManager.getAllContexts().contains(fleet), "the metrics output is not written yet");
			assertTrue(terminalEntry(client).isEmpty(), "no terminal entry yet");
			assertEquals(List.of(), resultsOutcomes());
		} finally {
			releaseStop.countDown();
		}

		closeIgnoringFailure(client);

		assertFalse(metricsManager.getAllContexts().contains(fleet), "the close's stop writes the output");
		assertEquals(1, timingCloses.get(), "the timing files are written with it");
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, terminalOutcome(client));
	}

	@Test
	void anInterruptedFinalizeAtTheAwaitsStopLeavesTheOutputToTheClose() throws Exception {
		final TestLoadStepClient client = client(countDeleteConfig());
		final LoadStep slice = cleanSlice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		addSlice(client, slice);
		// A stop request that an earlier stage only flags, as a timing file transfer does: the finalize's
		// counter capture is the first to see it.
		setMetricsAggregator(client, interruptingOnceAggregator());

		final Throwable stop = assertThrows(Throwable.class, () -> client.await(1, TimeUnit.SECONDS));

		assertInstanceOf(InterruptedException.class, stop);
		assertTrue(metricsManager.getAllContexts().contains(fleet), "the metrics output waits for the close");
		assertTrue(terminalEntry(client).isEmpty(), "no terminal entry yet");
		assertEquals(List.of(), resultsOutcomes());

		client.close();

		assertEquals(List.of(RESULTS_OUTCOME + "completed cleanly"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, terminalOutcome(client));
		assertTrue(client.isClosed());
	}

	@Test
	void anInterruptedFinalizeAtTheClosesStopWritesTheOutputFailedAndALaterCloseKeepsIt() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = client(config);
		final CountDownLatch probeEntered = new CountDownLatch(1);
		final CountDownLatch releaseProbe = new CountDownLatch(1);
		final LoadStep slice = liveProbeSlice(probeEntered, releaseProbe);
		addSlice(client, slice);
		try {
			// The await's stop defers the output to the close while the live probe runs.
			assertTrue(client.await(2, TimeUnit.SECONDS));
			assertTrue(metricsManager.getAllContexts().contains(fleet));
		} finally {
			releaseProbe.countDown();
		}
		awaitFailureBudgetSnapshotProbeCount(0);
		setMetricsAggregator(client, interruptingOnceAggregator());

		// The close's stop is interrupted in its counter capture: it skips its close, so it writes the output.
		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::close));

		assertFalse(metricsManager.getAllContexts().contains(fleet));
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, terminalOutcome(client));
		assertFalse(client.isClosed());

		// The later close fails the step with that outcome, as any failed outcome does.
		final IntegrityTerminalException closeFailure = assertThrows(IntegrityTerminalException.class, client::close);

		assertTrue(closeFailure.getMessage().contains("stop failed before its terminal failed-object counters"),
						closeFailure.getMessage());
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED, terminalOutcome(client),
						"a later close keeps the outcome the output carries");
		assertEquals(List.of(RESULTS_OUTCOME + "failed"), resultsOutcomes(1));
	}

	@Test
	void aFailedStageOfTheAwaitsStopWithALiveProbeLeavesOneOutcomeToTheClose() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = client(config);
		final CountDownLatch probeEntered = new CountDownLatch(1);
		final CountDownLatch releaseProbe = new CountDownLatch(1);
		final LoadStep slice = liveProbeSlice(probeEntered, releaseProbe);
		addSlice(client, slice);
		final IllegalStateException injected = new IllegalStateException("injected: metrics aggregator stop");
		final AtomicInteger aggregatorStops = new AtomicInteger();
		final MetricsAggregator aggregator = mock(MetricsAggregator.class);
		doAnswer(invocation -> {
			if (aggregatorStops.incrementAndGet() == 1) {
				throw injected;
			}
			return null;
		}).when(aggregator).stop();
		setMetricsAggregator(client, aggregator);
		try {
			assertSame(injected, assertThrows(IllegalStateException.class, () -> client.await(2, TimeUnit.SECONDS)));

			assertTrue(metricsManager.getAllContexts().contains(fleet), "the run's close writes the output");
			assertTrue(terminalEntry(client).isEmpty(), "no terminal entry yet");
			assertEquals(List.of(), resultsOutcomes());
		} finally {
			releaseProbe.countDown();
		}
		awaitFailureBudgetSnapshotProbeCount(0);

		client.close();

		assertEquals(List.of(RESULTS_OUTCOME + "completed cleanly"), resultsOutcomes(1));
		assertEquals(MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY, terminalOutcome(client));
		assertTrue(client.isClosed());
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

	/** A metrics aggregator whose first stop sets the interrupt flag of the stopping thread and returns. */
	private static MetricsAggregator interruptingOnceAggregator() throws Exception {
		final AtomicInteger stops = new AtomicInteger();
		final MetricsAggregator aggregator = mock(MetricsAggregator.class);
		doAnswer(invocation -> {
			if (stops.incrementAndGet() == 1) {
				// Let the monitor's first probe end, so the stop does not defer its output to that probe.
				awaitFailureBudgetSnapshotProbeCount(0);
				Thread.currentThread().interrupt();
			}
			return null;
		}).when(aggregator).stop();
		return aggregator;
	}

	private static void setMetricsAggregator(final LoadStepClientBase<?> client, final MetricsAggregator aggregator)
					throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField("metricsAggregator");
		field.setAccessible(true);
		field.set(client, aggregator);
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

	/** A slice whose await ends only once the failure-budget monitor's probe is in flight, held until released. */
	private static LoadStep liveProbeSlice(final CountDownLatch probeEntered, final CountDownLatch releaseProbe)
					throws Exception {
		final LoadStep slice = cleanSlice();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(probeEntered.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			if (releaseProbe.getCount() > 0) {
				probeEntered.countDown();
				awaitIgnoringInterrupt(releaseProbe);
			}
			return cleanDeleteLifecycle();
		});
		return slice;
	}

	private static DeleteObjectLifecycleSnapshot cleanDeleteLifecycle() {
		return new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, true);
	}

	private static Config durationConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "stop-outcome-duration");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private static Config countDeleteConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "stop-outcome-count");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
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

	private static void awaitFailureBudgetSnapshotProbeCount(final int expected) throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (System.nanoTime() < deadlineNanos) {
			if (LoadStepClientBase.activeFailureBudgetSnapshotFlightCount() == expected
							&& LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount() == expected) {
				return;
			}
			Thread.sleep(10);
		}
		assertEquals(expected, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
		assertEquals(expected, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		private CapturingAppender() {
			super("stopOutcomeConsistencyCapture", null, null, true, Property.EMPTY_ARRAY);
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
	}
}
