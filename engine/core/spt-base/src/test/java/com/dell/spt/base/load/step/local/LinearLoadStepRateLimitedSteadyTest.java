package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.ProbeLinearStep;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.github.akurilov.confuse.Config;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * With the paced rate throttle, a rate-limited step's steady {@code TPAvg} is its limit within 1%,
 * the tolerance of the throttle fix's own acceptance.
 *
 * <p>The driver's capacity ({@code C / L} = 320 ops/s) is well above the limit, and its input queue
 * accepts a whole batch at once, as a shipped driver's does. A throttle that granted the first
 * batch in full would run the step at capacity for longer than the step lasts: with the
 * java-commons {@code RateThrottle} this test measures about 318 ops/s.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LinearLoadStepRateLimitedSteadyTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;
	private static final double LIMIT = 200;
	private static final int BATCH = 1000;

	@Test
	void rateLimitedStepsSteadyRateIsTheLimit() throws Exception {
		final Config config = createStepConfig("steady-rate-limited", C);
		config.val("load-step-limit-time", "6s");
		config.val("load-op-limit-rate", LIMIT);
		config.val("load-batch-size", BATCH);
		steadySettings(config, 1.0, 20.0, 1.0);
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(C, L_MILLIS).inputQueue(10 * BATCH);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.run();
				assertNull(step.runFailure());
				final SteadyResult steady = step.metrics().steadyResult();
				final AllMetricsSnapshot window = statistics(steady);
				assertEquals(LIMIT, window.successSnapshot().mean(), LIMIT * 0.01, steady.toString());
			} finally {
				step.close();
			}
		}
	}
}
