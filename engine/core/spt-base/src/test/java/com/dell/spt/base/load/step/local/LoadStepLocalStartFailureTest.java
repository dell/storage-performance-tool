package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.rmi.RemoteException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A step missing part of its load mustn't report as the load that was asked for. */
class LoadStepLocalStartFailureTest {

	private static class Step extends LoadStepLocalBase {
		private final RuntimeException initFailure;
		private final List<LoadStepContext> contexts;

		private Step(final RuntimeException initFailure, final List<LoadStepContext> contexts) {
			super(config(), List.<Extension> of(), List.<Config> of(), mock(MetricsManager.class));
			this.initFailure = initFailure;
			this.contexts = contexts;
		}

		private static Config config() {
			final Config config = TestConfigBuilder.config();
			config.val("load-step-id", "start-failure-" + java.util.UUID.randomUUID());
			return config;
		}

		@Override
		public String getTypeName() {
			return "start-failure-test";
		}

		@Override
		protected void init() {
			if (initFailure != null) {
				throw initFailure;
			}
			stepContexts.addAll(contexts);
		}
	}

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	private static LoadStepContext context(final Exception startFailure) throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		if (startFailure != null) {
			when(context.start()).thenThrow(startFailure);
		}
		return context;
	}

	@Test
	void oneContextThatFailsToStartFailsTheRun() throws Exception {
		final var step = new Step(null, List.of(context(null), context(new IllegalStateException("no driver"))));
		final var failure = assertThrows(RunTerminalException.class, step::run);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("failed to start a load step context; see the engine log", failure.getMessage());
		assertTrue(failure.stepId().startsWith("start-failure-"));
		assertNotNull(step.runFailure());
	}

	@Test
	void aContextUnreachableAtStartFailsTheRun() throws Exception {
		final var step = new Step(null, List.of(context(new RemoteException("gone"))));
		final var failure = assertThrows(RunTerminalException.class, step::run);
		assertTrue(failure.getMessage().startsWith("failed to start a load step context"));
	}

	@Test
	void aContextsTypedFailureIsKept() throws Exception {
		final var typed = new RunTerminalException(IntegrityTerminalException.Category.INPUT, "items file line 7 is blank", null);
		final var step = new Step(null, List.of(context(new IllegalStateException("wrapped", typed))));
		final var failure = assertThrows(RunTerminalException.class, step::run);
		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		assertEquals("items file line 7 is blank", failure.getMessage());
	}

	@Test
	void anInitFailureEndsTheRunWithItsReason() {
		final var step = new Step(new IllegalStateException("No item input available"), List.of());
		final var failure = assertThrows(RunTerminalException.class, step::run);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("step failed to start; see the engine log", failure.getMessage());
	}

	@Test
	void aStopThatCutsAStartShortStaysAStop() {
		final var step = new Step(new IllegalStateException("closed by the stop"), List.of()) {
			@Override
			protected void init() {
				Thread.currentThread().interrupt();
				super.init();
			}
		};
		try {
			assertThrows(InterruptedException.class, step::run);
		} finally {
			Thread.interrupted();
		}
	}
}
