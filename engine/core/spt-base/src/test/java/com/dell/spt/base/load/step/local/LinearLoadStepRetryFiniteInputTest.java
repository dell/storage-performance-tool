package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.ProbeLinearStep;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A step reading a finite input ends its Steady-State Window at the last dispatch, with
 * {@code load-op-retry} on as with it off. The driver's queue takes the whole input at once, so the
 * input's end is read after the last handoff, and the last operation is slow, so it is still running
 * when the window ends.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LinearLoadStepRetryFiniteInputTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;
	private static final int ITEMS = 24 * C;
	private static final long TAIL_MILLIS = 800;
	private static final String EXHAUSTION = "scheduling exhaustion";

	@TempDir
	Path tempDir;

	@ParameterizedTest
	@ValueSource(booleans = {false, true
	})
	void aSlowLastOperationStaysOutOfTheWindowAndDoesNotLowerItsRate(final boolean retry) throws Exception {
		final Config config = finiteReadConfig("retry-finite-slow-" + retry, retry, 3);
		final Outcome outcome = run(config, driver(0));
		assertEquals(EXHAUSTION, outcome.steady.cause(), outcome.steady.toString());
		assertTrue(outcome.window.successSnapshot().count() < ITEMS, "window " + outcome.window.successSnapshot().count());
		assertEquals(ITEMS, outcome.total.successSnapshot().count());
		final double capacity = C * 1_000.0 / L_MILLIS;
		assertTrue(
						outcome.window.successSnapshot().mean() >= capacity / 2,
						"the window's rate is near the driver's " + capacity + ": " + outcome.steady);
	}

	@Test
	void aRetriedTailCountsInTheTotalRowOnly() throws Exception {
		final LatencyStorageDriver<?, ?> driver = driver(1);
		final Outcome outcome = run(finiteReadConfig("retry-finite-retried", true, 3), driver);
		assertEquals(EXHAUSTION, outcome.steady.cause(), outcome.steady.toString());
		assertEquals(ITEMS + 1, driver.dispatchedCount(), "the retry was dispatched after the signal");
		assertEquals(ITEMS, outcome.total.successSnapshot().count());
		assertEquals(0, outcome.total.failsSnapshot().count());
		assertTrue(outcome.window.successSnapshot().count() < ITEMS, "window " + outcome.window.successSnapshot().count());
		assertEquals(0, outcome.window.failsSnapshot().count());
	}

	@Test
	void aFailurePastTheRetryLimitEndsTheStepAndCountsInTheTotalRowOnly() throws Exception {
		final LatencyStorageDriver<?, ?> driver = driver(2);
		final Outcome outcome = run(finiteReadConfig("retry-finite-failed", true, 1), driver);
		assertEquals(EXHAUSTION, outcome.steady.cause(), outcome.steady.toString());
		assertEquals(ITEMS + 1, driver.dispatchedCount());
		assertEquals(ITEMS - 1, outcome.total.successSnapshot().count());
		assertEquals(1, outcome.total.failsSnapshot().count());
		assertEquals(0, outcome.window.failsSnapshot().count());
	}

	@Test
	void aListedPathClosesItsWindowAtExhaustion() throws Exception {
		final Config config = finiteReadConfig("retry-finite-listed", true, 3);
		config.val("item-input-file", null);
		config.val("item-input-path", "/steady-bucket");
		final Outcome outcome = run(config, driver(0).listing(ITEMS));
		assertEquals(EXHAUSTION, outcome.steady.cause(), outcome.steady.toString());
		assertEquals(ITEMS, outcome.total.successSnapshot().count());
	}

	private static LatencyStorageDriver<?, ?> driver(final int tailFailures) {
		return LatencyStorageDriver.data(C, L_MILLIS)
						.inputQueue(1_000_000)
						.slowTail(ITEMS - 1, TAIL_MILLIS, tailFailures);
	}

	private Config finiteReadConfig(final String stepId, final boolean retry, final int retryLimit) throws Exception {
		final StringBuilder lines = new StringBuilder();
		for (int i = 0; i < ITEMS; i++) {
			lines.append("/steady-bucket/item-").append(i).append(',').append(i).append(",1024,0/0\n");
		}
		final Config config = createStepConfig(stepId, C);
		config.val("item-input-file", Files.writeString(tempDir.resolve("items.csv"), lines).toString());
		config.val("load-op-type", "read");
		config.val("load-batch-size", 1024);
		config.val("load-op-retry", retry);
		config.val("load-op-retryLimit", retryLimit);
		steadySettings(config, 1.0, 5.0, 0.2);
		return config;
	}

	private static Outcome run(final Config config, final LatencyStorageDriver<?, ?> driver) throws Exception {
		try (var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.run();
				final SteadyResult steady = step.metrics().steadyResult();
				return new Outcome(steady, statistics(steady), metricsMgr.total(step.metrics()));
			} finally {
				step.close();
			}
		}
	}

	private record Outcome(SteadyResult steady, AllMetricsSnapshot window, AllMetricsSnapshot total) {}
}
