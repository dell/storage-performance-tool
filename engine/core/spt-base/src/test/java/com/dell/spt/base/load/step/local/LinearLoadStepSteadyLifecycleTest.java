package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.WAIT_TIMEOUT_MILLIS;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.assertWithin;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.awaitCondition;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.sleepUntil;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.windowEndMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.ProbeLinearStep;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import com.github.akurilov.confuse.Config;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Lifecycle tests of the Steady-State Window through the shipped linear step: its own init, its
 * load step context, the real load generator, real metrics contexts and the real metrics manager
 * loop, with a storage driver of fixed latency {@code L} and concurrency {@code C}.
 *
 * <p><b>Tolerance.</b> A closed loop at a fixed latency runs at most at {@code C / L}. The steady
 * {@code TPAvg} is asserted within [0.85, 1.05] of {@code C / L}. The low side allows for timer
 * slack on each completion and for the generator's output back-off, which can leave a freed slot
 * idle for up to about 1 ms, on a loaded 2-core machine; the high side allows for wave
 * quantization, at most one wave of {@code C} completions over a window of tens of waves. Timing
 * bounds compare the window with deliberately slow operations (hundreds of milliseconds), so they
 * hold far above scheduler noise.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LinearLoadStepSteadyLifecycleTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;
	private static final double C_OVER_L = C * 1000.0 / L_MILLIS;
	private static final double RATE_LOW = 0.85;
	private static final double RATE_HIGH = 1.05;
	/** Every window timing statistic stays below this; every deliberately slow operation exceeds it. */
	private static final long WINDOW_TIMING_BOUND_MICROS = TimeUnit.MILLISECONDS.toMicros(150);
	private static final long SLOW_FIRST_MILLIS = 600;
	private static final long DRAIN_HOLD_MILLIS = 200;

	/**
	 * Rate, slow first wave and drain exclusion at the deadline, in one time-limited step: the
	 * first {@code C} requests are slow (the ramp), and the requests in flight when the window
	 * closes at the time limit are held until the drain.
	 */
	@Test
	void timeLimitedStepMeasuresCOverLWithoutTheSlowFirstWaveOrTheDrain() throws Exception {
		final Config config = createStepConfig("steady-rate", C);
		config.val("load-step-limit-time", "2s");
		steadySettings(config, 1.0, 20.0, 1.0);
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			driver.slowFirstWave(C, SLOW_FIRST_MILLIS)
							.holdCompletionsWhen(step::windowsClosed)
							.releaseHeldAfterAdmissionClose(DRAIN_HOLD_MILLIS)
							.stopProbe(step::windowsClosed);
			try {
				step.run();

				assertNull(step.runFailure());
				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				final AllMetricsSnapshot frozen = step.metricsSnapshots().get(0);
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertEquals("run exit", steady.cause());

				// Rate: the window holds C / L.
				final double steadyRate = window.successSnapshot().mean();
				assertWithin(steadyRate, RATE_LOW * C_OVER_L, RATE_HIGH * C_OVER_L, "steady TPAvg vs C/L");
				// The ramp (no completion before the slow first wave) and the held drain are outside
				// the window, so the whole-step TPAvg is lower by at least half their share.
				final double totalElapsedMillis = total.successSnapshot().elapsedTimeMillis();
				final double rampAndDrainShare = (SLOW_FIRST_MILLIS + DRAIN_HOLD_MILLIS - 2.0 * L_MILLIS) / totalElapsedMillis;
				final double totalRate = total.successSnapshot().mean();
				assertTrue(
								totalRate < steadyRate * (1 - rampAndDrainShare / 2),
								"total TPAvg " + totalRate + " vs steady " + steadyRate + ", share " + rampAndDrainShare);
				assertTrue(steady.startOffsetMillis() >= SLOW_FIRST_MILLIS - 50, steady.toString());

				// Slow first wave: in the whole-step p99 and max, not in the window's.
				final long slowMicros = TimeUnit.MILLISECONDS.toMicros(SLOW_FIRST_MILLIS) * 9 / 10;
				assertTrue(total.durationSnapshot().max() >= slowMicros, "total max");
				assertTrue(total.durationSnapshot().percentile(0.99) >= slowMicros, "total p99");
				assertTrue(total.latencySnapshot().max() >= slowMicros, "total latency max");
				assertTrue(window.durationSnapshot().max() < WINDOW_TIMING_BOUND_MICROS, "window max");
				assertTrue(window.durationSnapshot().percentile(0.99) < WINDOW_TIMING_BOUND_MICROS, "window p99");
				assertTrue(window.latencySnapshot().max() < WINDOW_TIMING_BOUND_MICROS, "window latency max");

				// Drain exclusion at the deadline: the requests in flight at the cut were held until
				// the drain, completed there and counted in the whole-step total only.
				final int drained = driver.heldTotal();
				assertTrue(drained >= 1, "requests were in flight at the deadline");
				assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before the stop reached the context");
				assertTrue(frozen.successSnapshot().count() + drained <= total.successSnapshot().count());
				assertTrue(
								window.successSnapshot().count() + C + drained <= total.successSnapshot().count(),
								"window " + window.successSnapshot().count() + ", total " + total.successSnapshot().count());
				assertEquals(window.successSnapshot().count(), window.durationSnapshot().count());
			} finally {
				step.close();
			}
		}
	}

	/** Drain exclusion at a user stop: requests held in flight at the stop complete in the drain. */
	@Test
	void userStopExcludesTheRequestsDrainedAfterIt() throws Exception {
		final Config config = createStepConfig("steady-user-stop", C);
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS);
		final AtomicBoolean holdAll = new AtomicBoolean();
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			driver.holdCompletionsWhen(holdAll::get)
							.releaseHeldAfterAdmissionClose(DRAIN_HOLD_MILLIS)
							.stopProbe(step::windowsClosed);
			final Thread runner = Thread.ofPlatform().name("steady-user-stop-run").start(step);
			try {
				awaitCondition(() -> driver.completedCount() >= 32L * C, "about 0.8 s of steady load");
				holdAll.set(true);
				awaitCondition(() -> driver.heldNow() == C, "every slot held in flight");

				step.stop(); // the user stop: doShutdown, then each context's drain
				runner.join(WAIT_TIMEOUT_MILLIS);
				assertFalse(runner.isAlive());

				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals("shutdown", steady.cause());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before the stop reached the context");
				assertEquals(C, driver.heldTotal());
				assertTrue(window.durationSnapshot().max() < TimeUnit.MILLISECONDS.toMicros(DRAIN_HOLD_MILLIS), "window max");
				assertTrue(total.durationSnapshot().max() >= TimeUnit.MILLISECONDS.toMicros(DRAIN_HOLD_MILLIS), "total max");
				assertTrue(window.successSnapshot().count() + 2L * C <= total.successSnapshot().count());
			} finally {
				runner.join(WAIT_TIMEOUT_MILLIS);
				step.close();
			}
		}
	}

	/** Exceptional exit: a throw from await with initialized contexts closes the window at run exit. */
	@Test
	void throwFromAwaitClosesTheWindowAtRunExitBeforeTheFreeze() throws Exception {
		final Config config = createStepConfig("steady-await-throw", C);
		steadySettings(config, 1.0, 5.0, 0.2);
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS);
		final RuntimeException failure = new IllegalStateException("injected await failure");
		final AtomicBoolean thrown = new AtomicBoolean();
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			driver.stopProbe(step::windowsClosed).awaitLoopHook(() -> {
				if (driver.completedCount() >= 20L * C && thrown.compareAndSet(false, true)) {
					throw failure;
				}
			});
			try {
				step.run(); // an ordinary step records and logs the failure, as today

				assertTrue(thrown.get());
				assertSame(failure, step.runFailure());
				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				assertEquals("run exit", steady.cause());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before the stop reached the context");
				final AllMetricsSnapshot frozen = step.metricsSnapshots().get(0);
				assertTrue(window.successSnapshot().count() + C <= frozen.successSnapshot().count());
			} finally {
				step.close();
			}
		}
	}

	/** Observed cutoff: a late return from await takes the cut when it is observed. */
	@Test
	void lateReturnFromAwaitTakesTheCutWhenObserved() throws Exception {
		final Config config = createStepConfig("steady-late-await", C);
		config.val("load-step-limit-time", "1s");
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS);
		final AtomicLong firstPollNanos = new AtomicLong();
		final CountDownLatch awaitHeld = new CountDownLatch(1);
		final CountDownLatch awaitRelease = new CountDownLatch(1);
		driver.awaitLoopHook(() -> {
			final long now = System.nanoTime();
			firstPollNanos.compareAndSet(0, now);
			if (awaitHeld.getCount() > 0 && now - firstPollNanos.get() >= TimeUnit.MILLISECONDS.toNanos(500)) {
				awaitHeld.countDown();
				try {
					awaitRelease.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			final Thread runner = Thread.ofPlatform().name("steady-late-await-run").start(step);
			try {
				assertTrue(awaitHeld.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
				// The await began before its first poll, so its 1 s deadline has passed by then.
				sleepUntil(firstPollNanos.get() + TimeUnit.MILLISECONDS.toNanos(1_300));
				assertNull(step.metrics().steadyResult(), "the scheduled deadline does not close the window");
				final long releasedNanos = System.nanoTime();
				awaitRelease.countDown();
				runner.join(WAIT_TIMEOUT_MILLIS);
				assertFalse(runner.isAlive());

				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				assertEquals("run exit", steady.cause());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				// Step start precedes the first poll, so this bounds the cut from below.
				final double releasedAfterStartMillis = (releasedNanos - firstPollNanos.get()) / 1e6;
				assertTrue(windowEndMillis(steady) >= releasedAfterStartMillis - 1, steady + " released at " + releasedAfterStartMillis);
				assertTrue(windowEndMillis(steady) > 1_250, steady.toString());
				// Admission stayed open until the cut, so the late period is ordinary load.
				assertWithin(window.successSnapshot().mean(), RATE_LOW * C_OVER_L, RATE_HIGH * C_OVER_L, "steady TPAvg vs C/L");
			} finally {
				awaitRelease.countDown();
				runner.join(WAIT_TIMEOUT_MILLIS);
				step.close();
			}
		}
	}

	/**
	 * Failed step: a terminal failure after the window closed keeps the measured statistics and
	 * status, and the step reports the failure as today: a typed cause ends {@code run()}, an
	 * ordinary one is recorded and logged.
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false
	})
	void failureAfterTheWindowClosedLeavesTheWindowAsMeasured(final boolean typed) throws Exception {
		final Config config = createStepConfig("steady-failed-step", C);
		config.val("load-step-limit-time", "1s");
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS);
		final RuntimeException failure = typed
						? new RunTerminalException(
										IntegrityTerminalException.Category.EXECUTION, "injected terminal failure", null)
						: new IllegalStateException("injected cleanup failure");
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			driver.stopProbe(step::windowsClosed).failShutdownOnce(failure);
			try {
				if (typed) {
					final RunTerminalException reported = assertThrows(RunTerminalException.class, step::run);
					assertEquals(step.loadStepId(), reported.stepId());
					assertEquals("injected terminal failure", reported.getMessage());
					assertSame(failure, reported.getCause());
					assertSame(reported, step.runFailure());
				} else {
					step.run();
					assertSame(failure, step.runFailure());
				}

				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				assertTrue(driver.stopProbeHeldAtAdmissionClose(), "the window closed before the failing shutdown");
				assertEquals("run exit", steady.cause());
				assertEquals(SteadyStatus.VALID, steady.status(), steady.toString());
				assertWithin(window.successSnapshot().mean(), RATE_LOW * C_OVER_L, RATE_HIGH * C_OVER_L, "steady TPAvg vs C/L");
			} finally {
				step.close();
			}
		}
	}
}
