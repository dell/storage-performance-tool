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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A count-limited step on a driver that tracks custody, as every shipped driver does, ends its
 * window once nothing scheduled waits for dispatch, with and without recycle mode. In recycle mode
 * completed operations keep re-entering the generator after the count is reached, so custody alone
 * would never drain.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LinearLoadStepTrackedCountLimitTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;

	@ParameterizedTest
	@ValueSource(booleans = {false, true
	})
	void trackedCountLimitEndsAtSchedulingExhaustion(final boolean recycle) throws Exception {
		final long countLimit = 24L * C;
		final Config config = createStepConfig("tracked-count-" + recycle, C);
		config.val("load-op-limit-count", countLimit);
		config.val("load-batch-size", 1024);
		config.val("load-op-recycle-mode", recycle);
		steadySettings(config, 1.0, 5.0, 0.2);
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.trackedData(C, L_MILLIS).inputQueue(1_000_000);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			// Hold the last wave once everything is dispatched, so the manager, not the run exit, closes.
			driver.gateCompletionsWhile(() -> driver.queuedNow() == 0
							&& driver.dispatchedCount() >= countLimit
							&& driver.activeOpCount() < C
							&& !step.windowsClosed());
			try {
				step.run();
				final SteadyResult steady = step.metrics().steadyResult();
				assertEquals("scheduling exhaustion", steady.cause(), steady.toString());
				final AllMetricsSnapshot window = statistics(steady);
				final long inFlightAtCut = window.concurrencySnapshot().last();
				assertTrue(
								window.successSnapshot().count() <= countLimit - C - inFlightAtCut,
								"the last wave is outside the window: " + window.successSnapshot().count());
				assertTrue(window.successSnapshot().count() >= countLimit - 4L * C, "window " + window.successSnapshot().count());
			} finally {
				step.close();
			}
		}
	}
}
