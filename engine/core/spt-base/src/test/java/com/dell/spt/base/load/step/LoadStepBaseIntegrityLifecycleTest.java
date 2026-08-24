package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LoadStepBaseIntegrityLifecycleTest {

	@Test
	void metadataStartFailureCleansUpThenEscapesWithStepIdentity() {
		final var step = new TestStep(metadataConfig(), new IllegalStateException("start failed"), false);

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("integrity-step", failure.stepId());
		assertTrue(step.closeCalled);
	}

	@Test
	void existingTypedCauseKeepsCategoryAcrossStepBoundary() {
		final var typed = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, "manifest malformed");
		final var step = new TestStep(metadataConfig(), typed, false);

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		assertEquals("integrity-step", failure.stepId());
		assertTrue(step.closeCalled);
	}

	@Test
	void metadataCleanupFailureIsTerminal() {
		final var step = new TestStep(metadataConfig(), null, true);

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertEquals(IntegrityTerminalException.Category.CLEANUP, failure.category());
		assertEquals("integrity-step", failure.stepId());
	}

	@Test
	void typedTerminalCloseFailureEscapesEvenWhenMetadataModeIsDisabled() {
		final Config config = metadataConfig();
		config.val("storage-integrity-mode", "none");
		final var closeFailure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "standalone DELETE unresolved");
		final var step = new TestStep(config, null, false, true, closeFailure);

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("integrity-step", failure.stepId());
	}

	@Test
	void ordinaryStartAndCleanupFailuresRetainLegacyNonThrowingBehavior() {
		final Config config = metadataConfig();
		config.val("storage-integrity-mode", "none");
		final var startFailure = new IllegalStateException("start failed");
		final var step = new TestStep(config, startFailure, true);

		assertDoesNotThrow(step::run);
		assertTrue(step.closeCalled);
		assertSame(startFailure, step.runFailure(),
						"best-effort callers need the real swallowed lifecycle outcome");
		assertEquals(1, step.runFailure().getSuppressed().length,
						"cleanup failure should remain distinguishable behind the first failure");
	}

	@Test
	void standaloneDeleteDurationFailsWhenFrozenInventoryExhaustsBeforeDeadline() {
		final var step = new TestStep(standaloneDeleteDurationConfig(), null, false, true);

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("inventory slice exhausted before the requested duration"));
		assertTrue(failure.getMessage().contains("increase --seed-objects"));
		assertTrue(step.closeCalled);
	}

	@Test
	void standaloneDeleteDurationThatReachesDeadlineCompletesNormally() {
		final var step = new TestStep(standaloneDeleteDurationConfig(), null, false, false);

		assertDoesNotThrow(step::run);
		assertTrue(step.closeCalled);
	}

	@Test
	void standaloneDeleteCountCompletionIsNotDurationExhaustion() {
		final var config = standaloneDeleteDurationConfig();
		config.val("load-op-delete-duration", false);
		config.val("load-step-limit-time", "0s");
		final var step = new TestStep(config, null, false, true);

		assertDoesNotThrow(step::run);
	}

	@Test
	void finalCloseInterruptsRetainedFailedDurationPhase() throws Exception {
		final var step = new TestStep(metadataConfig(), null, false, false);
		final CountDownLatch phaseEntered = new CountDownLatch(1);
		final CountDownLatch phaseInterrupted = new CountDownLatch(1);

		final var attempt = step.invokeBlockingDurationPhase(phaseEntered, phaseInterrupted);

		assertTrue(phaseEntered.await(1, TimeUnit.SECONDS));
		assertFalse(attempt.succeeded());
		assertDoesNotThrow(step::close);
		assertTrue(
						phaseInterrupted.await(1, TimeUnit.SECONDS),
						"final close did not interrupt the retained duration phase");
	}

	@Test
	void durationRunCleanupMaySucceedOnFinalBoundedAttempt() {
		final var step = new BoundedCloseStep(3);

		assertDoesNotThrow(step::run);

		assertEquals(3, step.closeAttempts());
		assertTrue(step.isClosed());
	}

	@Test
	void durationRunCleanupExhaustionPreservesFirstFailureAndCancelsRetainedPhase()
					throws Exception {
		final var step = new BoundedCloseStep(Integer.MAX_VALUE);
		final CountDownLatch phaseEntered = new CountDownLatch(1);
		final CountDownLatch phaseInterrupted = new CountDownLatch(1);
		final var attempt = step.invokeBlockingDurationPhase(phaseEntered, phaseInterrupted);
		assertTrue(phaseEntered.await(1, TimeUnit.SECONDS));
		assertFalse(attempt.succeeded());

		final var failure = assertThrows(IntegrityTerminalException.class, step::run);

		assertTrue(failure.getMessage().contains("cleanup attempt 1"));
		assertEquals(3, step.closeAttempts());
		assertEquals(2, failure.getSuppressed().length);
		assertTrue(
						phaseInterrupted.await(1, TimeUnit.SECONDS),
						"bounded run cleanup did not cancel its retained phase executor");
	}

	private static Config metadataConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "integrity-step");
		config.val("run-id", 123L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
		return config;
	}

	private static Config standaloneDeleteDurationConfig() {
		final Config config = metadataConfig();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private static final class TestStep extends LoadStepBase {
		private final RuntimeException startFailure;
		private final boolean cleanupFailure;
		private final boolean awaitResult;
		private final RuntimeException closeFailure;
		private boolean closeCalled;

		private TestStep(
						final Config config,
						final RuntimeException startFailure,
						final boolean cleanupFailure) {
			this(config, startFailure, cleanupFailure, true);
		}

		private TestStep(
						final Config config,
						final RuntimeException startFailure,
						final boolean cleanupFailure,
						final boolean awaitResult) {
			this(config, startFailure, cleanupFailure, awaitResult, null);
		}

		private TestStep(
						final Config config,
						final RuntimeException startFailure,
						final boolean cleanupFailure,
						final boolean awaitResult,
						final RuntimeException closeFailure) {
			super(config, Collections.emptyList(), Collections.emptyList(), mock(MetricsManager.class));
			this.startFailure = startFailure;
			this.cleanupFailure = cleanupFailure;
			this.awaitResult = awaitResult;
			this.closeFailure = closeFailure;
		}

		@Override
		protected void doStartWrapped() {
			if (startFailure != null) {
				throw startFailure;
			}
		}

		@Override
		protected void init() {}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			return awaitResult;
		}

		@Override
		public String getTypeName() {
			return "test";
		}

		private DurationPhaseAttempt invokeBlockingDurationPhase(
						final CountDownLatch entered,
						final CountDownLatch interrupted) {
			return invokeRetainedDurationPhase(
							"test-retained-phase",
							List.of("handle"),
							handle -> {
								entered.countDown();
								try {
									new CountDownLatch(1).await();
								} catch (final InterruptedException expected) {
									interrupted.countDown();
									throw expected;
								}
							},
							durationDeadlineNanos(TimeUnit.MILLISECONDS.toNanos(20)),
							"spt-delete-test-retained-");
		}

		@Override
		protected void doClose() throws IOException {
			closeCalled = true;
			super.doClose();
			if (closeFailure != null) {
				throw closeFailure;
			}
			if (cleanupFailure) {
				throw new IOException("cleanup failed");
			}
		}
	}

	private static final class BoundedCloseStep extends LoadStepBase {
		private final int succeedingAttempt;
		private final AtomicInteger closeAttempts = new AtomicInteger();

		private BoundedCloseStep(final int succeedingAttempt) {
			super(
							standaloneDeleteDurationConfig(),
							Collections.emptyList(),
							Collections.emptyList(),
							mock(MetricsManager.class));
			this.succeedingAttempt = succeedingAttempt;
		}

		@Override
		protected void doStartWrapped() {}

		@Override
		protected void init() {}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			return false;
		}

		@Override
		public String getTypeName() {
			return "bounded-close-test";
		}

		@Override
		protected void doClose() {
			final int attempt = closeAttempts.incrementAndGet();
			if (attempt < succeedingAttempt) {
				throw new IntegrityTerminalException(
								IntegrityTerminalException.Category.CLEANUP,
								"cleanup attempt " + attempt);
			}
			closeRetainedDurationPhases();
		}

		private int closeAttempts() {
			return closeAttempts.get();
		}

		private DurationPhaseAttempt invokeBlockingDurationPhase(
						final CountDownLatch entered,
						final CountDownLatch interrupted) {
			return invokeRetainedDurationPhase(
							"bounded-close-retained-phase",
							List.of("handle"),
							handle -> {
								entered.countDown();
								try {
									new CountDownLatch(1).await();
								} catch (final InterruptedException expected) {
									interrupted.countDown();
									throw expected;
								}
							},
							durationDeadlineNanos(TimeUnit.MILLISECONDS.toNanos(20)),
							"spt-delete-test-bounded-close-");
		}
	}
}
