package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.MIN_COMPLETIONS_BEFORE_WINDOW;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.WAIT_TIMEOUT_MILLIS;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.awaitCondition;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.firstCompletionOr;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.sleepUntil;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.windowEndMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.client.LoadStepClientBase;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Lifecycle tests of the Steady-State Window through {@link LoadStepLocalBase} with real load step
 * contexts, real load generators, real metrics contexts built by {@code initMetrics} and the real
 * metrics manager loop: a step with two contexts, and a standalone DELETE duration step, which
 * is the step the worker deadline guard and the Entry Node's admission barrier and failure-budget
 * stop act on.
 *
 * <p>The Entry Node's own orchestration ({@code LoadStepClientBase}, over RMI) is not run here.
 * Its admission barrier and its failure-budget stop reach a slice through the same calls,
 * {@code enforceDispatchedOperationsDeadlineForStepStop} and then
 * {@code closeOperationAdmissionForStepStop}, which {@code LoadStepServiceImpl} passes to the
 * local step unchanged; these tests make those calls on the local step in that order.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LoadStepLocalSteadyStopPathTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;
	/**
	 * A request completed after the cut waits at least this long, and the window's timings stay below
	 * it. A loaded JVM stalls every thread for up to about 180 ms, and a stall only lengthens a
	 * recorded duration: an ordinary 25 ms request read up to about 190 ms on a pinned, loaded host,
	 * while a held request never reads less than its hold. With the hold, the window maxima are the
	 * only check here that catches a held request counted in the window, so keep the bound tight to it.
	 */
	private static final long LATE_HOLD_MILLIS = 500;
	/** Every window duration stays below this; every held request is at least this long. */
	private static final long WINDOW_TIMING_BOUND_MICROS = TimeUnit.MILLISECONDS.toMicros(LATE_HOLD_MILLIS);
	/**
	 * Steady load runs at least this long from the step's start before a stop, so a window opened
	 * late by a starved metrics loop (335 ms late measured on a cold, loaded JVM) still exceeds the
	 * steady minimum duration of {@code durationDeleteConfig} and of {@code createStepConfig}.
	 */
	private static final long STEADY_FLOOR_MILLIS = 1_500;
	/**
	 * The worker deadline guard's injected duration: after the steady floor, with room for the
	 * completion await before it, since the guard's cut stops every completion the await counts.
	 */
	private static final long WORKER_DEADLINE_MILLIS = 2_000;

	static {
		if (WORKER_DEADLINE_MILLIS - STEADY_FLOOR_MILLIS < 250) {
			throw new IllegalStateException("the worker deadline must fall well after the steady floor");
		}
	}

	/** Slow first drain: with two contexts, the second context's cutoff is not delayed. */
	@Test
	void slowFirstDrainDoesNotDelayTheSecondContextsCutoff() throws Exception {
		final Config config = createStepConfig("steady-two-contexts", C);
		final LatencyStorageDriver<?, ?> slowDrain = LatencyStorageDriver.data(C, L_MILLIS);
		final LatencyStorageDriver<?, ?> other = LatencyStorageDriver.data(C, L_MILLIS);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new TwoContextStep(config, List.of(slowDrain, other), metricsMgr);
			// The first context's requests in flight at the stop stay there until the test lets go.
			// Its first request is held from its completion, so the stop always finds one and its
			// drain lasts until the release: otherwise a stop between two waves would drain nothing,
			// and the stop would reach the second context at once.
			slowDrain.holdCompletionsWhen(firstCompletionOr(step::windowsClosed)).stopProbe(step::windowsClosed);
			other.stopProbe(step::windowsClosed);
			final Thread runner = Thread.ofPlatform().name("steady-two-contexts-run").start(step);
			final Thread stopper = Thread.ofPlatform().name("steady-two-contexts-stop").unstarted(step::stop);
			try {
				// The floor runs from the first completion on both contexts: a cold JVM can start the
				// step hundreds of milliseconds after the runner thread.
				awaitCondition(
								() -> slowDrain.completedCount() >= 1 && other.completedCount() >= 1,
								"load on both contexts");
				final long loadStartNanos = System.nanoTime();
				awaitCondition(
								() -> slowDrain.completedCount() >= 24L * C && other.completedCount() >= 24L * C,
								"steady load on both contexts");
				sleepUntil(loadStartNanos + TimeUnit.MILLISECONDS.toNanos(STEADY_FLOOR_MILLIS));
				stopper.start(); // a user stop
				assertTrue(slowDrain.awaitAdmissionClosed(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
				final long drainStartNanos = slowDrain.admissionClosedNanos();

				// The first context is draining; the second has not been reached yet, still runs
				// ordinary load, and its window is already closed.
				assertNotNull(step.metrics(1).steadyResult(), "the second window closed before the first drain");
				assertFalse(other.awaitAdmissionClosed(0, TimeUnit.MILLISECONDS));
				final long otherCompletedAtDrainStart = other.completedCount();
				awaitCondition(
								() -> other.completedCount() >= otherCompletedAtDrainStart + 4L * C,
								"the second context completes requests during the first drain");
				final long releasedNanos = System.nanoTime();
				slowDrain.releaseHeld();
				stopper.join(WAIT_TIMEOUT_MILLIS);
				runner.join(WAIT_TIMEOUT_MILLIS);
				assertFalse(stopper.isAlive());
				assertFalse(runner.isAlive());

				final SteadyResult first = step.metrics(0).steadyResult();
				final SteadyResult second = step.metrics(1).steadyResult();
				final AllMetricsSnapshot secondWindow = statistics(second);
				final AllMetricsSnapshot secondTotal = metricsMgr.total(step.metrics(1));
				assertEquals("shutdown", first.cause());
				assertEquals("shutdown", second.cause());
				assertEquals(SteadyStatus.VALID, first.status(), first.toString());
				assertEquals(SteadyStatus.VALID, second.status(), second.toString());
				assertTrue(slowDrain.stopProbeHeldAtAdmissionClose());
				assertTrue(other.stopProbeHeldAtAdmissionClose());
				assertTrue(slowDrain.heldTotal() >= 1, "the first drain had requests to wait for");
				final double drainMillis = (releasedNanos - drainStartNanos) / 1e6;
				final double cutoffSkewMillis = Math.abs(windowEndMillis(second) - windowEndMillis(first));
				assertTrue(
								cutoffSkewMillis < drainMillis / 2,
								"cutoff skew " + cutoffSkewMillis + " ms vs first drain " + drainMillis + " ms");
				assertTrue(
								secondWindow.successSnapshot().count() + MIN_COMPLETIONS_BEFORE_WINDOW + 4L * C <= secondTotal.successSnapshot().count(),
								"the second window excludes its load during the first drain");
			} finally {
				slowDrain.releaseHeld();
				if (stopper.isAlive()) {
					stopper.join(WAIT_TIMEOUT_MILLIS);
				}
				runner.join(WAIT_TIMEOUT_MILLIS);
				step.close();
			}
		}
	}

	/** Which stop-path action closes admission on a standalone DELETE duration slice. */
	enum EarlyStop {
		/** The worker's own deadline guard, at an injected short duration. */
		WORKER_DEADLINE_GUARD("close operation admission"),
		/** The Entry Node's admission barrier or failure-budget stop: deadline, then admission. */
		ENTRY_NODE_ADMISSION_STOP("dispatched operations deadline"),
		/** The failure-budget drain deadline alone, propagated while a stop is in progress. */
		FAILURE_BUDGET_DEADLINE("dispatched operations deadline");

		private final String cause;

		EarlyStop(final String cause) {
			this.cause = cause;
		}
	}

	/**
	 * Early admission close: execution is held after the stop action has closed the window but
	 * before {@code doShutdown}; a request completed there is counted in the step's total and
	 * excluded from the window.
	 */
	@ParameterizedTest
	@EnumSource(EarlyStop.class)
	void requestCompletedAfterAnEarlyAdmissionCloseIsExcluded(final EarlyStop stop) throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("steady-early-" + stop), driver, metricsMgr);
			// The first request is held from its completion, so every stop action finds one in flight,
			// even when its cut falls between two waves.
			driver.holdCompletionsWhen(firstCompletionOr(step::windowsClosed)).stopProbe(step::windowsClosed);
			try {
				step.start();
				final long stepStartNanos = System.nanoTime();
				final long durationNanos = stop == EarlyStop.WORKER_DEADLINE_GUARD
								? TimeUnit.MILLISECONDS.toNanos(WORKER_DEADLINE_MILLIS)
								: TimeUnit.SECONDS.toNanos(30);
				step.startDurationInterval(durationNanos);
				assertNull(step.metrics().steadyResult(), "arming the duration interval is not an end event");
				// Twelve waves of the C - 1 requests that cycle while the first is held, then the steady
				// floor; the worker deadline falls after both.
				awaitCondition(() -> driver.completedCount() >= 12L * (C - 1), "steady DELETE load");
				sleepUntil(stepStartNanos + TimeUnit.MILLISECONDS.toNanos(STEADY_FLOOR_MILLIS));

				switch (stop) {
				case WORKER_DEADLINE_GUARD -> {
					// The guard thread closes admission at the deadline; nothing else is called.
				}
				case ENTRY_NODE_ADMISSION_STOP -> {
					step.enforceDispatchedOperationsDeadlineForStepStop(TimeUnit.SECONDS.toNanos(5));
					step.closeOperationAdmissionForStepStop();
				}
				case FAILURE_BUDGET_DEADLINE -> step.enforceDispatchedOperationsDeadlineForStepStop(TimeUnit.SECONDS.toNanos(5));
				default -> throw new AssertionError(stop);
				}
				awaitCondition(step::windowsClosed, "the stop action closes the window");
				final SteadyResult steady = step.metrics().steadyResult();
				assertEquals(stop.cause, steady.cause());
				if (stop != EarlyStop.FAILURE_BUDGET_DEADLINE) {
					assertTrue(driver.awaitAdmissionClosed(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
					assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before admission closed");
				}
				// Every request in flight at the cut is held, the first one since its completion; then they
				// complete, before doShutdown.
				awaitCondition(
								() -> driver.heldNow() >= 1 && driver.heldNow() == driver.activeOpCount(),
								() -> "every request in flight held: held " + driver.heldNow()
												+ ", in flight " + driver.activeOpCount()
												+ ", completed " + driver.completedCount()
												+ ", window closed " + step.windowsClosed());
				sleepUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LATE_HOLD_MILLIS));
				final int late = driver.heldNow();
				final long countedBefore = step.metricsSnapshots().get(0).successSnapshot().count();
				driver.releaseHeld();
				assertEquals(
								countedBefore + late,
								step.metricsSnapshots().get(0).successSnapshot().count(),
								"the late requests count in the step's total");

				if (stop == EarlyStop.FAILURE_BUDGET_DEADLINE) {
					step.closeOperationAdmissionForStepStop();
				}
				if (stop != EarlyStop.WORKER_DEADLINE_GUARD) {
					// The rest of the Entry Node's coordinated stop, as the slice receives it.
					step.recoverQueuedOperationsForStepStop();
					step.drainDispatchedOperationsForStepStop(TimeUnit.SECONDS.toNanos(5));
					step.validateTerminalStateForStepStop();
				}
				step.close(); // doShutdown and the rest of the stop

				final AllMetricsSnapshot window = statistics(steady);
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertTrue(window.durationSnapshot().max() < WINDOW_TIMING_BOUND_MICROS, "window max");
				assertTrue(total.durationSnapshot().max() >= WINDOW_TIMING_BOUND_MICROS, "total max");
				assertTrue(
								window.successSnapshot().count() + MIN_COMPLETIONS_BEFORE_WINDOW + late <= total.successSnapshot().count(),
								"window " + window.successSnapshot().count() + ", total " + total.successSnapshot().count());
			} finally {
				driver.releaseHeld();
				step.close();
			}
		}
	}

	/**
	 * Duration path: a stop of a duration slice before its deadline ends the window in
	 * {@code doShutdown}, before {@code prepareDurationStop()} closes admission, and the requests
	 * drained after it are excluded.
	 */
	@Test
	void durationStopEndsTheWindowBeforePrepareDurationStop() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("steady-duration-stop"), driver, metricsMgr);
			// The first request is held from its completion, so the stop always finds one in flight.
			driver.holdCompletionsWhen(firstCompletionOr(step::windowsClosed))
							.releaseHeldAfterAdmissionClose(LATE_HOLD_MILLIS)
							.stopProbe(step::windowsClosed);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.SECONDS.toNanos(30));
				awaitCondition(() -> driver.completedCount() >= 16L * C, "steady DELETE load");

				step.stop(); // doShutdown: the window, then prepareDurationStop()

				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				// prepareDurationStop() would have closed it with "close operation admission".
				assertEquals("shutdown", steady.cause());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before admission closed");
				final int drained = driver.heldTotal();
				assertTrue(drained >= 1, "requests were in flight at the stop");
				assertTrue(window.durationSnapshot().max() < WINDOW_TIMING_BOUND_MICROS, "window max");
				assertTrue(total.durationSnapshot().max() >= WINDOW_TIMING_BOUND_MICROS, "total max");
				assertTrue(window.successSnapshot().count() + MIN_COMPLETIONS_BEFORE_WINDOW + drained <= total.successSnapshot().count());
			} finally {
				driver.releaseHeld();
				step.close();
			}
		}
	}

	private static Config durationDeleteConfig(final String stepId) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId + "-" + System.nanoTime());
		config.val("item-type", "data");
		config.val("item-output-file", "");
		config.val("load-batch-size", 4);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", 1);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("load-op-limit-count", 0L);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 5);
		config.val("output-metrics-trace-persist", false);
		steadySettings(config, 1.0, 5.0, 0.2);
		return config;
	}

	/**
	 * The generator hands a whole finite inventory to the driver's input queue within moments of the
	 * start. That is not running out: the slice still has work for far longer than its duration.
	 */
	@Test
	void anInventoryWaitingInTheInputQueueIsNotExhaustedAndReachesTheDeadline() throws Exception {
		final int inventory = 5_000;
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(100_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-queued"), driver, metricsMgr, inventory);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(1_000));
				awaitCondition(() -> driver.scheduledOpCount() == inventory, "the whole inventory handed to the queue");
				assertTrue(driver.queuedNow() > 0, "the rest is still queued");
				assertEquals(DurationAwaitStatus.RUNNING, step.durationAwaitStatus(), "a handoff is not exhaustion");

				step.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
				assertEquals(DurationAwaitStatus.REACHED_DEADLINE, step.durationAwaitStatus());
			} finally {
				step.close();
			}
		}
	}

	/** An inventory that really is sent before the deadline still ends the run as exhausted. */
	@Test
	void anInventoryAllDispatchedBeforeTheDeadlineIsExhausted() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(100_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-drained"), driver, metricsMgr, 2 * C);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.SECONDS.toNanos(30));
				final var failure = assertThrows(
								IntegrityTerminalException.class,
								() -> step.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
				assertTrue(failure.getMessage().contains("exhausted before the requested duration"), failure.getMessage());
				assertEquals(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE, step.durationAwaitStatus());
			} finally {
				step.close();
			}
		}
	}

	/**
	 * No poll runs at all here, as if every one missed the last dispatch. The final counts still show
	 * the inventory ran out, so the run must not pass.
	 */
	@Test
	void anInventoryThatRanOutUnseenStillFailsOnItsFinalCounts() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(2 * C);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-unseen"), driver, metricsMgr, 2 * C);
			step.start();
			step.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(700));
			awaitCondition(() -> driver.completedCount() == 2L * C, "the whole inventory completed");
			sleepUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(900));

			final var failure = assertThrows(IntegrityTerminalException.class, step::close);
			assertTrue(failure.getMessage().contains("exhausted before the requested duration"), failure.getMessage());
		}
	}

	/** A controller reads the verdict before the drain, so a worker's final counts must still reach it. */
	@Test
	void aDelegatedValidationReportsAnInventoryThatRanOutUnseen() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(2 * C);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-delegated"), driver, metricsMgr, 2 * C);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(700));
				awaitCondition(() -> driver.completedCount() == 2L * C, "the whole inventory completed");
				sleepUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(900));
				step.closeOperationAdmissionForStepStop();
				step.recoverQueuedOperationsForStepStop();
				step.drainDispatchedOperationsForStepStop(TimeUnit.SECONDS.toNanos(5));

				final var failure = assertThrows(IntegrityTerminalException.class, step::validateTerminalStateForStepStop);
				assertTrue(failure.getMessage().contains("exhausted before the requested duration"), failure.getMessage());
				assertEquals(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE, step.durationAwaitStatus());
			} finally {
				step.close();
			}
		}
	}

	/** One request still queued at the deadline means an object was left, so the run reached it. */
	@Test
	void oneRequestLeftInTheQueueIsNotExhausted() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, 1_500).inputQueue(100);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-one-left"), driver, metricsMgr, C + 1);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(500));
				awaitCondition(() -> driver.dispatchedCount() == C, "every slot holding a request");

				step.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
				assertEquals(DurationAwaitStatus.REACHED_DEADLINE, step.durationAwaitStatus());
			} finally {
				step.close();
			}
		}
	}

	/** An await that starts after the deadline still sees that every request was dispatched. */
	@Test
	void aLateAwaitReportsAnInventoryAllDispatched() throws Exception {
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(2 * C);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-late-await"), driver, metricsMgr, 2 * C);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(700));
				awaitCondition(() -> driver.completedCount() == 2L * C, "the whole inventory completed");
				sleepUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(900));

				final var failure = assertThrows(
								IntegrityTerminalException.class,
								() -> step.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
				assertTrue(failure.getMessage().contains("exhausted before the requested duration"), failure.getMessage());
				assertEquals(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE, step.durationAwaitStatus());
			} finally {
				step.close();
			}
		}
	}

	/** Closing admission empties the queue. A slice stopped that way did not run out of inventory. */
	@Test
	void aSliceStoppedBeforeTheDeadlineIsNotExhausted() throws Exception {
		final int inventory = 5_000;
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(100_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(durationDeleteConfig("duration-stopped"), driver, metricsMgr, inventory);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.SECONDS.toNanos(30));
				awaitCondition(() -> driver.scheduledOpCount() == inventory, "the whole inventory handed to the queue");
				step.closeOperationAdmissionForStepStop();
				step.recoverQueuedOperationsForStepStop();
				step.drainDispatchedOperationsForStepStop(TimeUnit.SECONDS.toNanos(5));

				assertEquals(DurationAwaitStatus.RUNNING, step.durationAwaitStatus());
				step.validateTerminalStateForStepStop();
			} finally {
				step.close();
			}
		}
	}

	/** How a slice is stopped while an await is live. */
	enum LiveAwaitStop {
		/** The Entry Node's stop phases, as {@code LoadStepClientBase} calls them over RMI. */
		ENTRY_NODE_PHASES,
		/** The slice's own stop. */
		STEP_STOP
	}

	/**
	 * A slice stopped before its deadline while an await is live did not run out: its await ends
	 * as promptly as an exhausted one would, without the exhaustion failure or status.
	 */
	@ParameterizedTest
	@EnumSource(LiveAwaitStop.class)
	void aLiveAwaitOnASliceStoppedBeforeTheDeadlineEndsPromptlyAndIsNotExhausted(final LiveAwaitStop how)
					throws Exception {
		final int inventory = 5_000;
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(100_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(
							durationDeleteConfig("duration-stopped-live-await-" + how), driver, metricsMgr, inventory);
			try {
				step.start();
				step.startDurationInterval(TimeUnit.SECONDS.toNanos(30));
				awaitCondition(() -> driver.scheduledOpCount() == inventory, "the whole inventory handed to the queue");
				final var outcome = new java.util.concurrent.atomic.AtomicReference<Object>();
				final var returnedNanos = new java.util.concurrent.atomic.AtomicLong();
				final Thread awaiter = new Thread(() -> {
					try {
						outcome.set(step.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
					} catch (final Throwable failure) {
						outcome.set(failure);
					}
					returnedNanos.set(System.nanoTime());
				}, "duration-live-await");
				awaiter.start();
				awaitDurationPollLoop(awaiter);
				final long stopNanos = System.nanoTime();
				if (how == LiveAwaitStop.ENTRY_NODE_PHASES) {
					step.closeOperationAdmissionForStepStop();
					step.recoverQueuedOperationsForStepStop();
					step.drainDispatchedOperationsForStepStop(TimeUnit.SECONDS.toNanos(5));
				} else {
					step.stop();
				}
				awaiter.join(2 * WAIT_TIMEOUT_MILLIS);
				assertFalse(awaiter.isAlive(), "the await did not return");

				final long afterStopMillis = TimeUnit.NANOSECONDS.toMillis(returnedNanos.get() - stopNanos);
				assertTrue(afterStopMillis < 1_000, "the await returned " + afterStopMillis + " ms after the stop");
				final var failure = assertInstanceOf(IntegrityTerminalException.class, outcome.get());
				assertTrue(failure.getMessage().contains("stopped before the requested duration"), failure.getMessage());
				assertFalse(failure.getMessage().contains("exhausted"), failure.getMessage());
				assertEquals(DurationAwaitStatus.RUNNING, step.durationAwaitStatus());
				if (how == LiveAwaitStop.ENTRY_NODE_PHASES) {
					step.validateTerminalStateForStepStop();
				}
			} finally {
				step.close();
			}
		}
	}

	/**
	 * Blocks until {@code awaiter} sleeps between two iterations of the duration await's poll loop,
	 * so a stop that follows lands on a live await and not before it.
	 */
	private static void awaitDurationPollLoop(final Thread awaiter) throws InterruptedException {
		awaitCondition(
						() -> awaiter.getState() == Thread.State.TIMED_WAITING
										&& java.util.Arrays.stream(awaiter.getStackTrace())
														.anyMatch(frame -> frame.getMethodName().equals("awaitDurationContexts")),
						"the duration await's poll loop");
	}

	/**
	 * A real slice joined to a real controller, stopped through the controller while both awaits
	 * are live: the controller's await ends promptly with the stop as its cause, not exhaustion.
	 */
	@Test
	void aControllerStopDuringTheAwaitOfARealSliceReportsTheStop() throws Exception {
		final int inventory = 5_000;
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.standaloneDelete(C, L_MILLIS).inputQueue(100_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new DurationDeleteStep(
							durationDeleteConfig("duration-controller-stop"), driver, metricsMgr, inventory);
			final var sliceAwaitEntered = new java.util.concurrent.CountDownLatch(1);
			final var admissionClosedNanos = new java.util.concurrent.atomic.AtomicLong();
			final LoadStep slice = observedSlice(step, sliceAwaitEntered, admissionClosedNanos);
			final var client = new com.dell.spt.base.load.step.client.TestLoadStepClient(
							durationDeleteConfig("duration-controller-stop-client"),
							List.<Extension> of(),
							List.<Config> of(),
							metricsMgr);
			addStepSlice(client, slice);
			final var stopFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
			final Thread stopper = new Thread(() -> {
				try {
					assertTrue(
									sliceAwaitEntered.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
									"the controller never awaited the slice");
					awaitCondition(() -> driver.scheduledOpCount() == inventory, "the whole inventory handed to the queue");
					client.stop();
				} catch (final Throwable failure) {
					stopFailure.set(failure);
				}
			}, "duration-controller-stopper");
			try {
				step.start();
				stopper.start();
				final var failure = assertThrows(
								IntegrityTerminalException.class,
								() -> client.await(30, TimeUnit.SECONDS));
				final long returnedNanos = System.nanoTime();
				assertTrue(failure.getMessage().contains("stopped before the requested duration"), failure.getMessage());
				assertFalse(failure.getMessage().contains("exhausted"), failure.getMessage());
				assertTrue(admissionClosedNanos.get() != 0, "the controller's stop did not close the slice's admission");
				final long afterAdmissionClosedMillis = TimeUnit.NANOSECONDS.toMillis(returnedNanos - admissionClosedNanos.get());
				assertTrue(
								afterAdmissionClosedMillis < 1_000,
								"the controller's await returned " + afterAdmissionClosedMillis + " ms after admission closed");
				stopper.join(2 * WAIT_TIMEOUT_MILLIS);
				assertFalse(stopper.isAlive(), "the controller's stop did not return");
				assertNull(stopFailure.get());
			} finally {
				stopper.interrupt();
				stopper.join(WAIT_TIMEOUT_MILLIS);
				try {
					client.close();
				} catch (final IntegrityTerminalException closeFailure) {
					// The stopped slice never reached its deadline, so the run fails; not as exhausted.
					assertFalse(closeFailure.getMessage().contains("exhausted"), closeFailure.getMessage());
				} finally {
					step.close();
				}
			}
		}
	}

	/**
	 * {@code step} as the controller sees it, recording when the controller first awaits it and
	 * when the controller closes its admission.
	 */
	private static LoadStep observedSlice(
					final LoadStep step,
					final java.util.concurrent.CountDownLatch awaitEntered,
					final java.util.concurrent.atomic.AtomicLong admissionClosedNanos) {
		return (LoadStep) java.lang.reflect.Proxy.newProxyInstance(
						LoadStep.class.getClassLoader(),
						new Class<?>[]{LoadStep.class
						},
						(proxy, method, args) -> {
							switch (method.getName()) {
							case "equals" -> {
								return proxy == args[0];
							}
							case "hashCode" -> {
								return System.identityHashCode(proxy);
							}
							case "toString" -> {
								return "observed " + step;
							}
							case "await" -> awaitEntered.countDown();
							case "closeOperationAdmissionForStepStop" -> admissionClosedNanos.compareAndSet(0, System.nanoTime());
							default -> {
								// forwarded unchanged
							}
							}
							try {
								return method.invoke(step, args);
							} catch (final java.lang.reflect.InvocationTargetException e) {
								throw e.getCause();
							}
						});
	}

	@SuppressWarnings("unchecked")
	private static void addStepSlice(final LoadStepClientBase<?> client, final LoadStep slice) throws Exception {
		final java.lang.reflect.Field field = LoadStepClientBase.class.getDeclaredField("stepSlices");
		field.setAccessible(true);
		((List<LoadStep>) field.get(client)).add(slice);
	}

	/** A local step with one real context per driver, as a mixed or weighted step builds them. */
	private static final class TwoContextStep extends LoadStepLocalBase {
		private final List<LatencyStorageDriver<?, ?>> drivers;
		private volatile List<MetricsContext<? extends AllMetricsSnapshot>> contexts = List.of();

		private TwoContextStep(
						final Config config,
						final List<LatencyStorageDriver<?, ?>> drivers,
						final MetricsManager metricsManager) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			this.drivers = drivers;
		}

		@Override
		public String getTypeName() {
			return "steady-two-context-test";
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"
		})
		protected void init() {
			final Config loadConfig = config.configVal("load");
			for (int i = 0; i < drivers.size(); i++) {
				initMetrics(i, OpType.CREATE, C, config.configVal("output-metrics"), new SizeInBytes(1024), false);
				final LatencyStorageDriver driver = drivers.get(i);
				final LoadGenerator generator = new LoadGeneratorBuilderImpl()
								.itemConfig(config.configVal("item"))
								.loadConfig(loadConfig)
								.itemType(ItemType.DATA)
								.itemFactory((ItemFactory) new DataItemFactoryImpl<>())
								.loadOperationsOutput(driver)
								.authConfig(config.configVal("storage-auth"))
								.originIndex(i)
								.build();
				stepContexts.add(new LoadStepContextImpl<>(
								loadStepId() + "-" + i, generator, driver, metricsContexts.get(i), loadConfig, false));
			}
			contexts = List.copyOf(metricsContexts);
		}

		MetricsContext<? extends AllMetricsSnapshot> metrics(final int index) {
			return contexts.get(index);
		}

		boolean windowsClosed() {
			final var current = contexts;
			return !current.isEmpty() && current.stream().allMatch(ctx -> ctx.steadyResult() != null);
		}
	}

	/** A standalone DELETE duration slice over an inventory it cannot exhaust in a test. */
	private static final class DurationDeleteStep extends LoadStepLocalBase {
		private final LatencyStorageDriver<?, ?> driver;
		private final int inventory;
		private volatile List<MetricsContext<? extends AllMetricsSnapshot>> contexts = List.of();

		private DurationDeleteStep(
						final Config config, final LatencyStorageDriver<?, ?> driver, final MetricsManager metricsManager) {
			this(config, driver, metricsManager, 1_000_000_000);
		}

		private DurationDeleteStep(
						final Config config,
						final LatencyStorageDriver<?, ?> driver,
						final MetricsManager metricsManager,
						final int inventory) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			this.driver = driver;
			this.inventory = inventory;
		}

		@Override
		public String getTypeName() {
			return "steady-duration-delete-test";
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"
		})
		protected void init() {
			final Config loadConfig = config.configVal("load");
			initMetrics(0, OpType.DELETE, C, config.configVal("output-metrics"), new SizeInBytes(0), false);
			final LoadGenerator generator = new LoadGeneratorBuilderImpl()
							.authConfig(config.configVal("storage-auth"))
							.itemConfig(config.configVal("item"))
							.itemFactory((ItemFactory) new DataItemFactoryImpl<>())
							.itemType(ItemType.DATA)
							.loadConfig(loadConfig)
							.itemInput(new LargeManifestInput(inventory))
							.loadOperationsOutput((LatencyStorageDriver) driver)
							.originIndex(0)
							.build();
			stepContexts.add(new LoadStepContextImpl<>(
							loadStepId(), generator, (LatencyStorageDriver) driver, metricsContexts.get(0), loadConfig, false));
			contexts = List.copyOf(metricsContexts);
		}

		MetricsContext<? extends AllMetricsSnapshot> metrics() {
			return contexts.get(0);
		}

		boolean windowsClosed() {
			final var current = contexts;
			return !current.isEmpty() && current.stream().allMatch(ctx -> ctx.steadyResult() != null);
		}
	}

	/** A frozen DELETE inventory far larger than a test consumes. */
	private static final class LargeManifestInput implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private int next;

		private LargeManifestInput(final int count) {
			this.count = count;
		}

		@Override
		public IntegrityManifestDataItem get() {
			return next < count ? item(next++) : null;
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = next;
			while (next < count && next - start < limit) {
				buffer.add(item(next++));
			}
			return next - start;
		}

		private static IntegrityManifestDataItem item(final int index) {
			return new IntegrityManifestDataItem("bucket", "key-" + index, index, null);
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
		}

		@Override
		public void close() {}
	}
}
