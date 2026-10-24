package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Shared fixtures for the Steady-State Window lifecycle tests. */
final class SteadyLifecycleSupport {

	/** Every wait in these tests is bounded by this, far above any expected latency. */
	static final long WAIT_TIMEOUT_MILLIS = 5_000;

	private SteadyLifecycleSupport() {}

	/**
	 * A configuration for a closed-loop CREATE step on the latency driver. Waiting for in-flight
	 * operations at stop is enabled, so a held request completes in the drain.
	 */
	static Config createStepConfig(final String stepId, final int concurrency) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId + "-" + System.nanoTime());
		config.val("load-op-type", "create");
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 5);
		config.val("load-step-limit-time", "30s");
		config.val("storage-driver-type", LatencyDriverExtension.ID);
		config.val("storage-driver-limit-concurrency", concurrency);
		config.val("item-data-size", "1KB");
		config.val("item-input-path", null); // new items, not a listing
		config.val("item-output-path", "steady-bucket");
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("output-metrics-trace-persist", false);
		config.val("output-metrics-timing-persist", false);
		steadySettings(config, 1.0, 10.0, 0.5);
		return config;
	}

	/** Sets {@code output.metrics.steady}; the minimum duration is in seconds. */
	static void steadySettings(
					final Config config,
					final double warmupTurnovers,
					final double minTurnovers,
					final double minDurationSeconds) {
		config.val("output-metrics-steady-warmup-turnovers", warmupTurnovers);
		config.val("output-metrics-steady-min-turnovers", minTurnovers);
		config.val("output-metrics-steady-min-duration", minDurationSeconds);
	}

	static void awaitCondition(final BooleanSupplier condition, final String description)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_TIMEOUT_MILLIS);
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadlineNanos > 0) {
				throw new AssertionError("timed out waiting for " + description);
			}
			Thread.sleep(2);
		}
	}

	/** Parks until {@code nanoTime} reaches {@code targetNanos}; a test-controlled hold, not an assertion. */
	static void sleepUntil(final long targetNanos) throws InterruptedException {
		long remainingNanos;
		while ((remainingNanos = targetNanos - System.nanoTime()) > 0) {
			TimeUnit.NANOSECONDS.sleep(remainingNanos);
		}
	}

	/** The window's end, in milliseconds from step start. */
	static double windowEndMillis(final SteadyResult result) {
		return result.startOffsetMillis() + result.durationNanos() / 1e6;
	}

	static AllMetricsSnapshot statistics(final SteadyResult result) {
		assertNotNull(result, "the steady-state window is closed");
		assertNotNull(result.statistics(), "statistics are reported for " + result);
		return result.statistics();
	}

	static void assertWithin(
					final double actual, final double low, final double high, final String description) {
		assertTrue(actual >= low && actual <= high, description + ": " + actual + " not in [" + low + ", " + high + "]");
	}

	/**
	 * The real metrics manager, with its 100 Hz loop observing each window, recording the snapshot
	 * it writes for each local context at unregister: the whole-step (total) snapshot.
	 */
	static final class RecordingMetricsManager extends MetricsManagerImpl {
		private final Map<MetricsContext<?>, AllMetricsSnapshot> totals = Collections.synchronizedMap(new IdentityHashMap<>());

		RecordingMetricsManager() {
			super(ServiceTaskExecutor.VT_EXECUTOR);
		}

		@Override
		public void unregister(final MetricsContext metricsCtx) {
			super.unregister(metricsCtx);
			final AllMetricsSnapshot snapshot = metricsCtx.lastSnapshot();
			totals.putIfAbsent(metricsCtx, snapshot);
		}

		/** The snapshot written at unregister, after the drain. */
		AllMetricsSnapshot total(final MetricsContext<?> metricsCtx) {
			final AllMetricsSnapshot snapshot = totals.get(metricsCtx);
			assertNotNull(snapshot, "the context was unregistered");
			return snapshot;
		}
	}

	/** Supplies one prebuilt latency driver to the step's own driver construction. */
	static final class LatencyDriverExtension<I extends Item, O extends Operation<I>> extends ExtensionBase
					implements StorageDriverFactory<I, O, LatencyStorageDriver<I, O>> {
		static final String ID = "latency-mock";
		private final LatencyStorageDriver<I, O> driver;

		LatencyDriverExtension(final LatencyStorageDriver<I, O> driver) {
			this.driver = driver;
		}

		@Override
		public String id() {
			return ID;
		}

		@Override
		public LatencyStorageDriver<I, O> create(
						final String stepId,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize)
						throws IllegalConfigurationException {
			return driver;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		protected String defaultsFileName() {
			return null;
		}

		@Override
		protected List<String> resourceFilesToInstall() {
			return List.of();
		}
	}

	/** The shipped linear step, built by its own {@code init()}, exposing its metrics contexts. */
	static final class ProbeLinearStep extends LinearLoadStepLocal {
		private volatile List<MetricsContext<? extends AllMetricsSnapshot>> contexts = List.of();

		ProbeLinearStep(
						final Config config, final LatencyStorageDriver<?, ?> driver, final MetricsManager metricsManager) {
			super(config, List.<Extension> of(new LatencyDriverExtension<>(driver)), List.of(), metricsManager);
		}

		@Override
		protected void init() {
			super.init();
			contexts = List.copyOf(metricsContexts);
		}

		MetricsContext<? extends AllMetricsSnapshot> metrics() {
			final var current = contexts;
			assertTrue(current.size() == 1, "one metrics context: " + current);
			return current.get(0);
		}

		/** Whether the step's contexts exist and every window is closed. */
		boolean windowsClosed() {
			final var current = contexts;
			return !current.isEmpty() && current.stream().allMatch(ctx -> ctx.steadyResult() != null);
		}
	}
}
