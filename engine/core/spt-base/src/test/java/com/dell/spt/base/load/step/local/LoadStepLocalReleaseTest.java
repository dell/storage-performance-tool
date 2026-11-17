package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.range.RangeReadArtifact;
import com.dell.spt.base.metrics.range.RangeReadSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** A slice whose stop or close fails is still released, once, and reports its own failure. */
class LoadStepLocalReleaseTest {

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	@Test
	void aSliceWhoseLifecyclePublishFailsIsReleasedAtCloseWithTheStopFailure() throws Exception {
		final LoadStepContext first = mock(LoadStepContext.class);
		final LoadStepContext second = mock(LoadStepContext.class);
		final MetricsManager metricsMgr = mock(MetricsManager.class);
		final Step step = new Step(config("create"), metricsMgr, first, second);
		final IllegalStateException publishFailure = new IllegalStateException("injected lifecycle publish failure");
		try (MockedStatic<OperationLifecycleArtifact> artifact = mockStatic(OperationLifecycleArtifact.class)) {
			artifact.when(() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()))
							.thenThrow(publishFailure);
			step.start();
			final MetricsContext<?> metricsCtx = step.metricsContext();
			verify(metricsMgr).register(metricsCtx);
			// Ten seconds of the step's time have passed: the stop accounts them once.
			step.startedSecondsAgo(10);
			final long limitBeforeStop = step.timeLimitSec();

			assertSame(publishFailure, assertThrows(IllegalStateException.class, step::stop));
			verify(metricsMgr).unregister(metricsCtx);
			// The close repeats the stop; the stop runs once, so the close releases the slice and throws
			// the stop's failure, not a second publish's.
			assertSame(publishFailure, assertThrows(IllegalStateException.class, step::close));

			artifact.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
			verify(first).stop();
			verify(second).stop();
			verify(first).close();
			verify(second).close();
			assertEquals(limitBeforeStop - 10, step.timeLimitSec(), "the elapsed time is accounted once");
			assertFalse(step.awaitsCleanupRetry());

			// Released: closing again releases nothing and throws nothing.
			assertDoesNotThrow(step::close);
			assertTrue(step.isClosed());
			verify(first, times(1)).close();
			verify(second, times(1)).close();
			artifact.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
		}
	}

	@Test
	void aContextThatFailsToStopDoesNotKeepTheOthersRunningOrTheSliceUnreleased() throws Exception {
		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext other = mock(LoadStepContext.class);
		final IllegalStateException stopFailure = new IllegalStateException("injected context stop failure");
		when(failing.stop()).thenThrow(stopFailure);
		final MetricsManager metricsMgr = mock(MetricsManager.class);
		final Step step = new Step(config("update"), metricsMgr, failing, other);
		step.start();
		final MetricsContext<?> metricsCtx = step.metricsContext();

		assertSame(stopFailure, assertThrows(IllegalStateException.class, step::stop));
		verify(other).stop();
		verify(metricsMgr).unregister(metricsCtx);

		assertSame(stopFailure, assertThrows(IllegalStateException.class, step::close));
		verify(failing, times(1)).stop();
		verify(failing).close();
		verify(other).close();
	}

	@Test
	void aFailedRangeReadPublishDoesNotPublishTheLifecycleFileAgain() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.rangeReadSnapshot()).thenReturn(mock(RangeReadSnapshot.class));
		final Step step = new Step(config("read"), mock(MetricsManager.class), context);
		final IllegalStateException rangeFailure = new IllegalStateException("injected range-read publish failure");
		try (MockedStatic<OperationLifecycleArtifact> lifecycle = mockStatic(OperationLifecycleArtifact.class);
						MockedStatic<RangeReadArtifact> range = mockStatic(RangeReadArtifact.class)) {
			range.when(() -> RangeReadArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()))
							.thenThrow(rangeFailure);
			step.start();

			assertSame(rangeFailure, assertThrows(IllegalStateException.class, step::stop));
			assertSame(rangeFailure, assertThrows(IllegalStateException.class, step::close));

			lifecycle.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
			range.verify(
							() -> RangeReadArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
			verify(context).close();
		}
	}

	@Test
	void aStandaloneRunWhoseStopFailsStillEndsWithThatFailure() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.isDone()).thenReturn(true);
		final Step step = new Step(config("create"), mock(MetricsManager.class), context);
		final IllegalStateException publishFailure = new IllegalStateException("injected lifecycle publish failure");
		try (MockedStatic<OperationLifecycleArtifact> artifact = mockStatic(OperationLifecycleArtifact.class)) {
			artifact.when(() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()))
							.thenThrow(publishFailure);

			assertSame(publishFailure, assertThrows(IllegalStateException.class, step::run));
			assertSame(publishFailure, step.runFailure());
			artifact.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
		}
	}

	@Test
	void aDurationDeleteSliceKeepsItsResourcesUntilItsLastFailedCloseAndIsReleasedAfterIt() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		doThrow(new IllegalStateException("injected admission close failure"))
						.when(context).closeOperationAdmissionForStepStop();
		final Config config = config("delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("load-op-wait-limit", 1);
		final MetricsManager metricsMgr = mock(MetricsManager.class);
		final Step step = new Step(config, metricsMgr, context);
		final int limit = Step.closeAttemptLimit();

		step.start();
		final MetricsContext<?> metricsCtx = step.metricsContext();
		assertDoesNotThrow(step::stop);
		for (var attempt = 1; attempt < limit; attempt++) {
			assertThrows(RuntimeException.class, step::close, "close attempt " + attempt);
			assertTrue(step.awaitsCleanupRetry(), "a retry is still expected after attempt " + attempt);
		}
		final int admissionAttempts = limit;
		verify(context, times(admissionAttempts - 1)).closeOperationAdmissionForStepStop();
		assertThrows(RuntimeException.class, step::close, "the last close attempt");
		assertFalse(step.awaitsCleanupRetry());
		verify(metricsMgr, times(1)).unregister(metricsCtx);

		// Released: a later close attempts no cleanup again. As after a standalone run's last retry,
		// the contexts are left as they are.
		assertDoesNotThrow(step::close);
		verify(context, times(admissionAttempts)).closeOperationAdmissionForStepStop();
		verify(context, never()).close();
	}

	private static Config config(final String opType) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "release-" + opType + "-" + UUID.randomUUID());
		config.val("load-op-type", opType);
		return config;
	}

	private static final class Step extends LoadStepLocalBase {
		private final List<LoadStepContext> contexts;

		Step(final Config config, final MetricsManager metricsMgr, final LoadStepContext... contexts) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsMgr);
			this.contexts = List.of(contexts);
			// A step that is stopped before it starts, as the duration case is, has its contexts already.
			stepContexts.addAll(this.contexts);
		}

		static int closeAttemptLimit() {
			return DURATION_RUN_CLOSE_ATTEMPT_LIMIT;
		}

		@Override
		public String getTypeName() {
			return "release-test";
		}

		@Override
		protected void init() {
			stepContexts.clear();
			stepContexts.addAll(contexts);
			metricsContexts.clear();
			initMetrics(0, OpType.CREATE, 1, config.configVal("output").configVal("metrics"), new SizeInBytes(1L), false);
		}

		MetricsContext<?> metricsContext() {
			return metricsContexts.getFirst();
		}

		long timeLimitSec() throws ReflectiveOperationException {
			return ((AtomicLong) field("timeLimitSec").get(this)).get();
		}

		void startedSecondsAgo(final long seconds) throws ReflectiveOperationException {
			field("startTimeSec").setLong(this, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - seconds);
			// A step with no time limit has nothing to account; give it one.
			((AtomicLong) field("timeLimitSec").get(this)).set(1_000);
		}

		private static Field field(final String name) throws NoSuchFieldException {
			final Field field = LoadStepBase.class.getDeclaredField(name);
			field.setAccessible(true);
			return field;
		}
	}
}
