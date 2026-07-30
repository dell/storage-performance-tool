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
import java.util.concurrent.TimeUnit;
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
	void ordinaryStartAndCleanupFailuresRetainLegacyNonThrowingBehavior() {
		final Config config = metadataConfig();
		config.val("storage-integrity-mode", "none");
		final var step = new TestStep(config, new IllegalStateException("start failed"), true);

		assertDoesNotThrow(step::run);
		assertTrue(step.closeCalled);
	}

	private static Config metadataConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "integrity-step");
		config.val("run-id", 123L);
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
		return config;
	}

	private static final class TestStep extends LoadStepBase {
		private final RuntimeException startFailure;
		private final boolean cleanupFailure;
		private boolean closeCalled;

		private TestStep(
						final Config config,
						final RuntimeException startFailure,
						final boolean cleanupFailure) {
			super(config, Collections.emptyList(), Collections.emptyList(), mock(MetricsManager.class));
			this.startFailure = startFailure;
			this.cleanupFailure = cleanupFailure;
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
			return true;
		}

		@Override
		public String getTypeName() {
			return "test";
		}

		@Override
		protected void doClose() throws IOException {
			closeCalled = true;
			super.doClose();
			if (cleanupFailure) {
				throw new IOException("cleanup failed");
			}
		}
	}
}
