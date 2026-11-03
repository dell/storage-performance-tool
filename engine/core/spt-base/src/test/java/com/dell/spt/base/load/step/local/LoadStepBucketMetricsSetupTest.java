package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.bucket.BucketContextReply;
import com.dell.spt.base.metrics.bucket.BucketMeters;
import com.dell.spt.base.metrics.bucket.BucketMetricsRange;
import com.dell.spt.base.metrics.bucket.BucketRecorder;
import com.dell.spt.base.metrics.bucket.BucketSteadyOutcome;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LoadStepBucketMetricsSetupTest {

	private static final class SetupStep extends LoadStepLocalBase {
		SetupStep(final Config config, final MetricsManager metricsManager) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
		}

		@Override
		public String getTypeName() {
			return "bucket-metrics-setup";
		}

		@Override
		protected void init() {}

		BucketRecorder recorder(final Config config, final Map<OpType, MetricsContext> byType) {
			return initBucketMetrics(config, context(OpType.CREATE), byType);
		}

		int metersBuilt() {
			return bucketMeters().size();
		}

		/** Builds what a real step's {@code init()} does for one context, ahead of the barrier. */
		MetricsContext barrierContext(final Config stepConfig, final LoadStepContext stepContext) {
			stepContexts.add(stepContext);
			initMetrics(0, OpType.CREATE, 1, stepConfig.configVal("output").configVal("metrics"), new SizeInBytes(1), false);
			final MetricsContext context = metricsContexts.get(0);
			assertNotNull(initBucketMetrics(stepConfig, context, null));
			return context;
		}

		void startContexts() {
			doStartWrapped();
		}

		void closeWindows() {
			closeSteadyWindows("the step stopped");
		}
	}

	private static void awaitState(final SetupStep step, final PreparationStatus.State state)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (step.preparationStatus(7).state() != state) {
			assertTrue(System.nanoTime() < deadline, "preparation state " + step.preparationStatus(7));
			TimeUnit.MILLISECONDS.sleep(5);
		}
	}

	private static BucketContextReply onlyReply(final SetupStep step) {
		final var reply = assertTimeoutPreemptively(
						Duration.ofSeconds(4), () -> step.bucketMetrics(BucketMetricsRange.ALL), "a fetch must not wait");
		assertEquals(1, reply.contexts().size());
		return reply.contexts().get(0);
	}

	private static MetricsContext context(final OpType type) {
		return MetricsContextImpl.builder()
						.loadStepId("setup")
						.runId(1L)
						.opType(type)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
	}

	private static Config config(final int count) {
		final Config config = TestConfigBuilder.config();
		config.val("item-output-path", "/bench");
		config.val(TargetBucketSet.COUNT, count);
		return config;
	}

	@Test
	void shippedDefaultIsOnAndBothSpellingsReachOneSetting() {
		final Config config = TestConfigBuilder.config();
		assertTrue(config.boolVal("output-metrics-bucket-enabled"));
		config.val("output-metrics-bucket-enabled", false);
		assertEquals(false, config.configVal("output").configVal("metrics").configVal("bucket").boolVal("enabled"));
	}

	@Test
	void aSingleBucketAllocatesNothing() {
		final var step = new SetupStep(config(1), mock(MetricsManager.class));
		assertNull(step.recorder(config(1), null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void aOneNameListAllocatesNothing() {
		final Config config = config(1);
		config.val(TargetBucketSet.NAMES, List.of("only"));
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNull(step.recorder(config, null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void theOffSwitchAllocatesNothing() {
		final Config config = config(4);
		config.val("output-metrics-bucket-enabled", false);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNull(step.recorder(config, null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void aPhaseThatPersistsNoTotalsAllocatesNothing() {
		final Config config = config(4);
		config.val("output-metrics-summary-persist", false);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNull(step.recorder(config, null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void aSetBuildsOneBlockForEachContext() {
		final Config config = config(4);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNotNull(step.recorder(config, null));
		assertEquals(1, step.metersBuilt());
		assertEquals(4, step.bucketMeters().get(0).memberCount());
	}

	@Test
	void aMixedStepBuildsOneBlockForEachOperationType() {
		final Config config = config(4);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		final Map<OpType, MetricsContext> byType = new EnumMap<>(OpType.class);
		byType.put(OpType.READ, context(OpType.READ));
		byType.put(OpType.DELETE, context(OpType.DELETE));
		assertNotNull(step.recorder(config, byType));
		// The primary and the two types: a step's primary is normally also one of its types.
		assertEquals(3, step.metersBuilt());
	}

	/**
	 * A NIC set defers the Load Interval to the admission barrier's release, so the meters built in
	 * {@code init()} wait through the preparation. Nothing may reach the metrics manager or start the
	 * window until the release.
	 */
	@Test
	void aBarrierStepRegistersItsContextOnlyAtTheRelease() throws Exception {
		final Config config = config(4);
		final MetricsManager manager = mock(MetricsManager.class);
		final var step = new SetupStep(config, manager);
		final MetricsContext context = step.barrierContext(config, mock(LoadStepContext.class));
		step.startPreparation(7, TimeUnit.MINUTES.toNanos(1));
		step.startContexts();
		awaitState(step, PreparationStatus.State.READY);
		verify(manager, never()).register(any());

		assertTrue(step.releasePreparedAdmission(7));
		verify(manager, times(1)).register(context);

		step.closeWindows();
		final BucketContextReply reply = onlyReply(step);
		assertEquals(5, reply.wholeStep().size(), "4 members and the catch-all");
		reply.wholeStep().forEach(record -> assertEquals(0L, record.succeeded(), record.bucket()));
		assertEquals(BucketSteadyOutcome.Kind.NOT_REACHED, reply.steady().kind(), "no warm-up completed");
	}

	/**
	 * A step fenced before the release admitted nothing, and a worker that fenced itself may never be
	 * closed, because its entry node is gone and the next step replaces it without closing it. So the
	 * fence itself frees the meters, and a later fetch or stop finds nothing to wait for.
	 */
	@Test
	void aFenceBeforeTheReleaseFreesTheBucketMeters() throws Exception {
		final Config config = config(4);
		final MetricsManager manager = mock(MetricsManager.class);
		final var step = new SetupStep(config, manager);
		final LoadStepContextImpl<?, ?> stepContext = mock(LoadStepContextImpl.class);
		step.barrierContext(config, stepContext);
		final WeakReference<BucketMeters> meters = new WeakReference<>(step.bucketMeters().get(0));
		step.startPreparation(7, TimeUnit.MILLISECONDS.toNanos(1));
		step.startContexts();
		awaitState(step, PreparationStatus.State.FENCED);
		verify(manager, never()).register(any());
		verify(stepContext, timeout(20_000).atLeastOnce()).bucketRecorder(null);

		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (meters.get() != null) {
			assertTrue(System.nanoTime() < deadline, "something still holds the fenced step's meters");
			System.gc();
			TimeUnit.MILLISECONDS.sleep(20);
		}
		assertTrue(step.bucketMeters().isEmpty());
		assertNull(assertTimeoutPreemptively(Duration.ofSeconds(4), () -> step.bucketMetrics(BucketMetricsRange.ALL)));
		assertTimeoutPreemptively(Duration.ofSeconds(4), step::closeWindows, "no end pass to wait for");
	}

	@Test
	void bucketMetersNeverReachTheMetricsManager() {
		final Config config = config(1000);
		final MetricsManager manager = mock(MetricsManager.class);
		final var step = new SetupStep(config, manager);
		assertNotNull(step.recorder(config, null));
		assertEquals(1000, step.bucketMeters().get(0).memberCount());
		verify(manager, never()).register(any());
	}
}
