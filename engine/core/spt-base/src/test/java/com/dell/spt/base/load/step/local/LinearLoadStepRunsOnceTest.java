package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.ProbeLinearStep;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.github.akurilov.confuse.Config;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A step object runs once, through the shipped linear step and a real metrics manager. */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LinearLoadStepRunsOnceTest {

	private static final int CONCURRENCY = 2;
	private static final long COUNT_LIMIT = 8;

	private static Config countLimitedConfig(final String stepId) {
		final Config config = createStepConfig(stepId, CONCURRENCY);
		config.val("load-op-limit-count", COUNT_LIMIT);
		return config;
	}

	@Test
	void aSecondRunIsRefusedAndKeepsTheRecordOfTheFirst() throws Exception {
		final Config config = countLimitedConfig("runs-once");
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(CONCURRENCY, 1);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.run();
				assertNull(step.runFailure());
				assertTrue(step.isClosed());
				final var snapshots = step.metricsSnapshots();
				assertEquals(1, snapshots.size());
				assertEquals(COUNT_LIMIT, snapshots.get(0).successSnapshot().count());

				final var failure = assertThrows(RunTerminalException.class, step::run);

				assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
				assertEquals(step.loadStepId(), failure.stepId());
				assertEquals(
								"this step object has already run; a step object runs once. "
												+ "Build a step with config() for each run and call run() on a step that has not run.",
								failure.getMessage());
				assertNull(step.runFailure(), "the first run's record stays");
				assertSame(snapshots, step.metricsSnapshots(), "the first run's metrics stay");
			} finally {
				step.close();
			}
		}
	}

	@Test
	void aStartedStepStillRunsToItsEnd() throws Exception {
		final Config config = countLimitedConfig("start-then-run");
		final LatencyStorageDriver<?, ?> driver = LatencyStorageDriver.data(CONCURRENCY, 1);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.start();
				step.run();

				assertNull(step.runFailure());
				assertTrue(step.isClosed());
				assertEquals(COUNT_LIMIT, step.metricsSnapshots().get(0).successSnapshot().count());
			} finally {
				step.close();
			}
		}
	}
}
