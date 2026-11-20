package com.dell.spt.base.control.run;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.control.ApiStatus;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A node run cut by a signal reports {@code STOPPED}, however its scenario ends once the shutdown
 * hook has begun. The scenarios run on the engine's real script engine.
 */
class StatusAwareRunJvmExitTest {

	private static final long RUN_ID = 41L;

	private final ApiStatus status = new ApiStatus();

	@BeforeEach
	void markRunning() {
		status.setRunning("posted-step", RUN_ID);
	}

	@AfterEach
	void clearFlag() {
		JvmExitTestSupport.reset();
	}

	@Test
	void aStepRefusedAtExitIsAStopAndNothingEscapes() {
		JvmExit.markBegun();

		assertDoesNotThrow(run("Steps.refused();")::run);

		assertStopped();
	}

	@Test
	void aScenarioThatEndsNormallyAfterTheHookClosedItsStepIsStopped() {
		JvmExit.markBegun();

		run("var ended = true;").run();

		assertStopped();
	}

	@Test
	void aScenarioFailureAfterTheHookBeganIsStopped() {
		JvmExit.markBegun();

		run("Steps.failed();").run();

		assertStopped();
	}

	@Test
	void aScenarioCompletesWhileTheJvmIsNotExiting() {
		run("var ended = true;").run();

		assertEquals(ApiStatus.State.COMPLETED, status.state());
	}

	@Test
	void anInterruptionWithoutAnExitStillReachesTheExecutor() {
		final Throwable thrown = assertThrows(Throwable.class, run("Steps.refused();")::run);

		assertInstanceOf(InterruptedException.class, thrown);
	}

	private void assertStopped() {
		assertEquals(ApiStatus.State.STOPPED, status.state(), String.valueOf(status.snapshot()));
		assertNull(status.failureCategory());
		assertNull(status.failureMessage());
	}

	private RunServlet.StatusAwareRun run(final String scenario) {
		final ScriptEngine engine = new ScriptEngineManager().getEngineByName("js");
		engine.put("Steps", new Steps());
		return new RunServlet.StatusAwareRun(new RunImpl("", scenario, engine, RUN_ID), status);
	}

	public static final class Steps {

		public void refused() {
			throwUnchecked(new InterruptedException("not started, the engine is exiting"));
		}

		public void failed() {
			throw new IllegalStateException("closed under the scenario");
		}
	}
}
