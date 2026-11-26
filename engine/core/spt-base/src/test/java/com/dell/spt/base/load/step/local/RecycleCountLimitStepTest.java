package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.ProbeLinearStep;
import com.dell.spt.base.load.step.local.SteadyLifecycleSupport.RecordingMetricsManager;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A recycling step with fewer items than its count limit does exactly that count, and always ends.
 * None of these steps has a time limit: the count, or the stop, has to end each of them.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RecycleCountLimitStepTest {

	private static final int C = 8;
	private static final long L_MILLIS = 25;
	// One operation into a wave of eight: the wave that ends the step has more than the count needs.
	private static final long COUNT = 24L * C + 1;
	// About 2 s of dispatch, so a late metrics observation still starts the window before the count.
	private static final long STEADY_COUNT = 80L * C + 1;
	// Far above any prompt stop, so a stop that waits out the in-flight limit cannot pass.
	private static final long STOP_WAIT_LIMIT_MILLIS = 30_000;

	@TempDir
	Path tempDir;

	@ParameterizedTest
	@ValueSource(ints = {4, 1024
	})
	void totalsEqualTheCountAndTheSteadyCountStaysWithinIt(final int batchSize) throws Exception {
		final var driver = driver();
		final Config config = stepConfig("recycle-exact", C, STEADY_COUNT);
		config.val("load-batch-size", batchSize);
		steadySettings(config, 1.0, 5.0, 0.2);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.run();
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals(STEADY_COUNT, total.successSnapshot().count());
				assertEquals(0, total.failsSnapshot().count());
				assertEquals(STEADY_COUNT, driver.dispatchedCount());
				final OperationLifecycleCounters lifecycle = driver.operationLifecycle().counters();
				assertEquals(STEADY_COUNT, lifecycle.accepted(), lifecycle.toString());
				assertEquals(STEADY_COUNT, lifecycle.terminalResults(), lifecycle.toString());
				final long steadyCount = statistics(step.metrics().steadyResult()).successSnapshot().count();
				assertTrue(steadyCount <= STEADY_COUNT, "steady count " + steadyCount);
			} finally {
				step.close();
			}
		}
	}

	/** A retry repeats a counted operation and takes nothing from the count. */
	@Test
	void retriesDoNotChangeTheCount() throws Exception {
		final long count = 5L * C + 1;
		final var completions = new AtomicLong();
		final var retried = new AtomicLong();
		final var driver = driver().completionStatus(op -> {
			if (op.opRetryCount() == 0 && completions.incrementAndGet() % 5 == 0) {
				retried.incrementAndGet();
				return Status.FAIL_IO;
			}
			return Status.SUCC;
		});
		final Config config = stepConfig("recycle-retry", C, count);
		config.val("load-op-retry", true);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.run();
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertTrue(retried.get() > 0, "the driver failed some attempts");
				assertEquals(count, total.successSnapshot().count());
				assertEquals(0, total.failsSnapshot().count());
				assertEquals(count + retried.get(), driver.dispatchedCount());
			} finally {
				step.close();
			}
		}
	}

	/** A failed operation is counted and leaves circulation, and the others make up the count. */
	@Test
	void failuresCountTowardTheLimitAndTheStepEnds() throws Exception {
		final long count = 5L * C + 1;
		final int failures = 3;
		final var completions = new AtomicLong();
		final var driver = driver().completionStatus(
						op -> completions.incrementAndGet() <= failures ? Status.RESP_FAIL_CLIENT : Status.SUCC);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(stepConfig("recycle-failures", 2 * C, count), driver, metricsMgr);
			try {
				step.run();
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals(failures, total.failsSnapshot().count());
				assertEquals(count - failures, total.successSnapshot().count());
				assertEquals(count, driver.dispatchedCount());
			} finally {
				step.close();
			}
		}
	}

	/** With every operation failed nothing is left to reach the count, so the step ends without it. */
	@Test
	void aStepWhoseOperationsAllFailEndsBelowTheCount() throws Exception {
		final var driver = driver().completionStatus(op -> Status.RESP_FAIL_CLIENT);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(stepConfig("recycle-all-fail", C, COUNT), driver, metricsMgr);
			try {
				step.run();
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertEquals(C, total.failsSnapshot().count());
				assertEquals(0, total.successSnapshot().count());
			} finally {
				step.close();
			}
		}
	}

	/** Operations a stop interrupts are not counted, and the step does not wait for the count. */
	@Test
	void stoppingWithOperationsInFlightEndsTheStepPromptly() throws Exception {
		final long count = 1_000_000;
		final long holdFrom = 2L * C;
		final var driver = driver();
		driver.completionStatus(op -> driver.admissionClosedNanos() == 0 ? Status.SUCC : Status.INTERRUPTED)
						.holdCompletionsWhen(() -> driver.completedCount() >= holdFrom)
						.releaseHeldAfterAdmissionClose(20);
		final Config config = stepConfig("recycle-stop", C, count);
		config.val("load-op-wait-limit", (int) TimeUnit.MILLISECONDS.toSeconds(STOP_WAIT_LIMIT_MILLIS));
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new ProbeLinearStep(config, driver, metricsMgr);
			try {
				step.start();
				final long inFlightDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
				while (driver.heldNow() != C) {
					assertTrue(System.nanoTime() - inFlightDeadlineNanos < 0, "every circulating operation in flight");
					Thread.sleep(2);
				}

				final long stopStartNanos = System.nanoTime();
				step.stop();
				assertTrue(step.await(STOP_WAIT_LIMIT_MILLIS, TimeUnit.MILLISECONDS), "the stopped step ended");
				final long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStartNanos);

				// Well below the configured wait for in-flight operations, which would otherwise end it.
				assertTrue(stopMillis < STOP_WAIT_LIMIT_MILLIS / 2, "the stop took " + stopMillis + " ms");
				final AllMetricsSnapshot total = metricsMgr.total(step.metrics());
				assertTrue(total.successSnapshot().count() < count, "the count was not reached");
				assertEquals(0, driver.activeOpCount());
			} finally {
				step.close();
			}
		}
	}

	private static LatencyStorageDriver<Item, Operation<Item>> driver() {
		return LatencyStorageDriver.<Item, Operation<Item>> trackedData(C, L_MILLIS).inputQueue(1_000_000);
	}

	/** A READ step over {@code itemCount} items in recycle mode, limited by count alone. */
	private Config stepConfig(final String stepId, final int itemCount, final long count) throws IOException {
		final Path items = tempDir.resolve(stepId + ".csv");
		final StringBuilder lines = new StringBuilder();
		for (int i = 0; i < itemCount; i++) {
			lines.append("/bucket/item-").append(i).append(",0,1024,0/0\n");
		}
		Files.writeString(items, lines);
		final Config config = createStepConfig(stepId, C);
		config.val("item-input-file", items.toString());
		config.val("item-output-path", null);
		config.val("load-op-type", "read");
		config.val("load-op-recycle-mode", true);
		config.val("load-op-limit-count", count);
		config.val("load-step-limit-time", 0);
		return config;
	}
}
