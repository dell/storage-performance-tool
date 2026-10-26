package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.WAIT_TIMEOUT_MILLIS;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.awaitCondition;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.createStepConfig;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.sleepUntil;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.statistics;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.steadySettings;
import static com.dell.spt.base.load.step.local.SteadyLifecycleSupport.windowEndMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
	/** A request completed after the cut waits at least this long; window timings stay below it. */
	private static final long LATE_HOLD_MILLIS = 150;

	/** Slow first drain: with two contexts, the second context's cutoff is not delayed. */
	@Test
	void slowFirstDrainDoesNotDelayTheSecondContextsCutoff() throws Exception {
		final Config config = createStepConfig("steady-two-contexts", C);
		final LatencyStorageDriver<?, ?> slowDrain = LatencyStorageDriver.data(C, L_MILLIS);
		final LatencyStorageDriver<?, ?> other = LatencyStorageDriver.data(C, L_MILLIS);
		try (final var metricsMgr = new RecordingMetricsManager()) {
			final var step = new TwoContextStep(config, List.of(slowDrain, other), metricsMgr);
			// The first context's requests in flight at the stop stay there until the test lets go.
			slowDrain.holdCompletionsWhen(step::windowsClosed).stopProbe(step::windowsClosed);
			other.stopProbe(step::windowsClosed);
			final Thread runner = Thread.ofPlatform().name("steady-two-contexts-run").start(step);
			final Thread stopper = Thread.ofPlatform().name("steady-two-contexts-stop").unstarted(step::stop);
			try {
				awaitCondition(
								() -> slowDrain.completedCount() >= 24L * C && other.completedCount() >= 24L * C,
								"steady load on both contexts");
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
								secondWindow.successSnapshot().count() + C + 4L * C <= secondTotal.successSnapshot().count(),
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
			driver.holdCompletionsWhen(step::windowsClosed).stopProbe(step::windowsClosed);
			try {
				step.start();
				final long durationNanos = stop == EarlyStop.WORKER_DEADLINE_GUARD
								? TimeUnit.MILLISECONDS.toNanos(700)
								: TimeUnit.SECONDS.toNanos(30);
				step.startDurationInterval(durationNanos);
				assertNull(step.metrics().steadyResult(), "arming the duration interval is not an end event");
				awaitCondition(() -> driver.completedCount() >= 12L * C, "steady DELETE load");

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
				// Every request in flight at the cut is held; then they complete, before doShutdown.
				awaitCondition(
								() -> driver.heldNow() >= 1 && driver.heldNow() == driver.activeOpCount(),
								"every request in flight held");
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
				assertTrue(window.durationSnapshot().max() < TimeUnit.MILLISECONDS.toMicros(LATE_HOLD_MILLIS), "window max");
				assertTrue(total.durationSnapshot().max() >= TimeUnit.MILLISECONDS.toMicros(LATE_HOLD_MILLIS), "total max");
				assertTrue(
								window.successSnapshot().count() + C + late <= total.successSnapshot().count(),
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
			driver.holdCompletionsWhen(step::windowsClosed)
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
				assertTrue(window.durationSnapshot().max() < TimeUnit.MILLISECONDS.toMicros(LATE_HOLD_MILLIS), "window max");
				assertTrue(total.durationSnapshot().max() >= TimeUnit.MILLISECONDS.toMicros(LATE_HOLD_MILLIS), "total max");
				assertTrue(window.successSnapshot().count() + C + drained <= total.successSnapshot().count());
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
		private volatile List<MetricsContext<? extends AllMetricsSnapshot>> contexts = List.of();

		private DurationDeleteStep(
						final Config config, final LatencyStorageDriver<?, ?> driver, final MetricsManager metricsManager) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			this.driver = driver;
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
							.itemInput(new LargeManifestInput())
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
		private static final int COUNT = 1_000_000_000;
		private int next;

		@Override
		public IntegrityManifestDataItem get() {
			return next < COUNT ? item(next++) : null;
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= COUNT) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = next;
			while (next < COUNT && next - start < limit) {
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
			return (long) COUNT - next;
		}

		@Override
		public void reset() {
			next = 0;
		}

		@Override
		public void close() {}
	}
}
