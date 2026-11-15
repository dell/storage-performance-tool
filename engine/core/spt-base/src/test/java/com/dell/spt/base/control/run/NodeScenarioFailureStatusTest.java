package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.SingleTaskExecutorImpl;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.load.step.client.TestLoadStepClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A node reports a scenario that fails without a typed terminal failure as {@code FAILED}, with a
 * {@code configuration} or {@code execution} category and an engine-built message that never quotes
 * the scenario. The scenarios run on the engine's real script engine through the node's
 * {@link RunServlet.StatusAwareRun}.
 */
class NodeScenarioFailureStatusTest {

	private static final String STEP_ID = "posted-step";
	private static final long RUN_ID = 31L;
	private static final String MARKER = "AKIAFAKEMARKER0123456789";

	private static ScriptEngine engine(final Map<String, Object> bindings) {
		final var engine = new ScriptEngineManager().getEngineByName("js");
		engine.put("Load", new TestLoadStepClient(TestConfigBuilder.config(), List.of(), null, null));
		bindings.forEach(engine::put);
		return engine;
	}

	private static ApiStatus runOnNode(final String scenario) {
		return runOnNode(scenario, Map.of());
	}

	private static ApiStatus runOnNode(final String scenario, final Map<String, Object> bindings) {
		final var status = new ApiStatus();
		status.setRunning(STEP_ID, RUN_ID);
		new RunServlet.StatusAwareRun(new RunImpl("", scenario, engine(bindings), RUN_ID), status).run();
		return status;
	}

	private static void assertFailed(final ApiStatus status, final Category category) {
		assertEquals(ApiStatus.State.FAILED, status.state(), String.valueOf(status.snapshot()));
		assertEquals(category, status.failureCategory(), String.valueOf(status.snapshot()));
		assertEquals(RUN_ID, status.runId());
		assertEquals(STEP_ID, status.stepId(), "a failure without a step keeps the node's step id");
	}

	private static void assertNoMarker(final ApiStatus status) {
		assertFalse(String.valueOf(status.snapshot()).contains(MARKER), String.valueOf(status.snapshot()));
	}

	@Test
	void aRefusedBooleanIsAConfigurationFailure() {
		final var status = runOnNode("Load.config({load: {op: {shuffle: \"yes\"}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid boolean value for \"load-op-shuffle\"; give true or false", status.failureMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = {"MARKER", "\\\"MARKER\\\"", "line one\\nMARKER\\nline three", "MARKER\\\"; x: \\\"MARKER"
	})
	void aRefusedBooleanValueIsNeverPublished(final String value) {
		final var status = runOnNode(
						"Load.config({load: {op: {shuffle: \"" + value.replace("MARKER", MARKER) + "\"}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid boolean value for \"load-op-shuffle\"; give true or false", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aLongMultilineBooleanValueIsNeverPublished() {
		// The marker leads, so that the 1024-character cap alone would not hide it
		final var value = "\\\"" + MARKER + "\\n" + "x".repeat(1500) + "\\n" + MARKER + "\\\"";
		final var status = runOnNode("Load.append({load: {op: {recycle: {mode: \"" + value + "\"}}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid boolean value for \"load-op-recycle-mode\"; give true or false", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void anUnknownOptionIsNamedByItsDefinedPrefix() {
		final var status = runOnNode("Load.config({load: {op: {noSuchOption: 1}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid configuration option under \"load-op\"", status.failureMessage());
	}

	@Test
	void aMalformedOptionIsNamedWithoutItsValue() {
		final var status = runOnNode("Load.config({load: {op: \"" + MARKER + "\"}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid value for configuration option \"load-op\"", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void anUnknownKeyUnderARealPrefixIsNeverPublished() {
		final var status = runOnNode("var o = {}; o[\"" + MARKER + "\"] = 1; Load.config({load: {op: o}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid configuration option under \"load-op\"", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void anUnknownTopLevelKeyIsNeverPublished() {
		final var status = runOnNode("var o = {}; o[\"" + MARKER + "\"] = {deeper: 1}; Load.config(o).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid configuration option", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void anUnknownKeyWithTheSeparatorIsNeverPublished() {
		final var status = runOnNode("Load.config({load: {\"op-" + MARKER + "\": 1}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid configuration option under \"load-op\"", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aBadNumberIsAConfigurationFailureThatDoesNotQuoteTheValue() {
		final var status = runOnNode("Load.config({load: {op: {limit: {count: \"" + MARKER + "\"}}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("scenario failed: NumberFormatException; see the node's log", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aBadAppendIsAConfigurationFailure() {
		final var status = runOnNode("Load.append({load: {op: {recycle: {mode: \"yes\"}}}}).run();");
		assertFailed(status, Category.CONFIGURATION);
		assertEquals("invalid boolean value for \"load-op-recycle-mode\"; give true or false", status.failureMessage());
	}

	@Test
	void aSyntaxErrorIsAConfigurationFailureThatDoesNotQuoteTheLine() {
		final var status = runOnNode("var x = 1;\nvar secret = \"" + MARKER + "\"; Load.config({ );\n");
		assertFailed(status, Category.CONFIGURATION);
		assertTrue(
						status.failureMessage().matches("scenario failed at line 2, column \\d+; see the node's log"),
						status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aScriptThrowIsAnExecutionFailureThatDoesNotQuoteIt() {
		final var status = runOnNode("throw new Error(\"" + MARKER + "\");");
		assertFailed(status, Category.EXECUTION);
		assertEquals("scenario failed: ECMAException; see the node's log", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aScriptsOwnNumberParseIsAnExecutionFailure() {
		final var status = runOnNode("java.lang.Integer.parseInt(\"" + MARKER + "\");");
		assertFailed(status, Category.EXECUTION);
		assertEquals("scenario failed: NumberFormatException; see the node's log", status.failureMessage());
		assertNoMarker(status);
	}

	@Test
	void aJavaErrorThroughTheScriptIsAnExecutionFailure() {
		final var status = new ApiStatus();
		status.setRunning(STEP_ID, RUN_ID);
		final var run = new RunServlet.StatusAwareRun(
						new RunImpl("", "Steps.fail();", engine(Map.of("Steps", new FailingSteps())), RUN_ID), status);
		// The executor logs the error, as before
		assertThrows(AssertionError.class, run::run);
		assertFailed(status, Category.EXECUTION);
		assertEquals("scenario failed: AssertionError; see the node's log", status.failureMessage());
	}

	@Test
	void aValidScenarioCompletes() {
		final var status = runOnNode("var config = Load.config({load: {op: {shuffle: \"true\"}}});");
		assertEquals(ApiStatus.State.COMPLETED, status.state());
		assertNull(status.failureCategory());
		assertNull(status.failureMessage());
	}

	@Test
	void aTypedTerminalFailureKeepsItsCategoryAndMessage() {
		final var status = new ApiStatus();
		status.setRunning(STEP_ID, RUN_ID);
		final var run = new RunServlet.StatusAwareRun(
						new RunImpl("", "Steps.missingInput();", engine(Map.of("Steps", new FailingSteps())), RUN_ID),
						status);
		final var failure = assertThrows(IntegrityTerminalException.class, run::run);
		assertSame(FailingSteps.MISSING_INPUT, failure);
		assertFailed(status, Category.INPUT);
		assertEquals("item input file is missing", status.failureMessage());
	}

	@Test
	void aFailedAwaitInTheStartAwaitCloseFormIsAFailure() {
		final var step = new StubStep(true, false);
		final var status = runOnNode(
						"var step = Load.start(); step.await(); step.close();", Map.of("Load", step));
		assertFailed(status, Category.EXECUTION);
		assertEquals("scenario failed: IllegalStateException; see the node's log", status.failureMessage());
	}

	@Test
	void aFailedCloseInTheStartAwaitCloseFormIsAFailure() {
		final var step = new StubStep(false, true);
		final var status = runOnNode(
						"var step = Load.start(); step.await(); step.close();", Map.of("Load", step));
		assertFailed(status, Category.EXECUTION);
		assertEquals("scenario failed: IllegalStateException; see the node's log", status.failureMessage());
	}

	@Test
	void aStopDuringAScriptSleepReadsStopped() throws Exception {
		assertStopReadsStopped("Gate.countDown(); java.lang.Thread.sleep(60000);", Map.of());
	}

	@Test
	void aStopDuringARunningStepReadsStopped() throws Exception {
		assertStopReadsStopped(
						"Gate.countDown(); var step = Load.start(); step.await(); step.close();",
						Map.of("Load", new BlockingStep()));
	}

	@Test
	void aStopThatTheScenarioReportsAsAFailureReadsStopped() throws Exception {
		// The step turns the stop's interruption into a failure the scenario returns, not an interruption
		assertStopReadsStopped(
						"Gate.countDown(); var step = Load.start(); step.await(); step.close();",
						Map.of("Load", new InterruptionToFailureStep()));
	}

	@Test
	void aScriptFailureWithoutAPositionOrCauseIsNamedByItsType() {
		final var noPosition = new javax.script.ScriptException("failed");
		assertEquals(Category.EXECUTION, ScenarioFailureStatus.category(noPosition));
		assertEquals("scenario failed: ScriptException; see the node's log", ScenarioFailureStatus.message(noPosition));
	}

	private static void assertStopReadsStopped(final String scenario, final Map<String, Object> extra)
					throws Exception {
		final var gate = new CountDownLatch(1);
		final var bindings = new java.util.HashMap<String, Object>(extra);
		bindings.put("Gate", gate);
		final var status = new ApiStatus();
		status.setRunning(STEP_ID, RUN_ID);
		final var run = new RunServlet.StatusAwareRun(new RunImpl("", scenario, engine(bindings), RUN_ID), status);
		final var executor = new SingleTaskExecutorImpl();
		try {
			executor.execute(run);
			assertTrue(gate.await(10, TimeUnit.SECONDS), "the scenario did not start");
			// Let the scenario reach its blocking call
			Thread.sleep(200);
			assertTrue(run.stopAndAwait(executor, 10, TimeUnit.SECONDS), "the stop did not finish");
			assertEquals(ApiStatus.State.STOPPED, status.state(), String.valueOf(status.snapshot()));
			assertNull(status.failureCategory());
		} finally {
			executor.close();
		}
	}

	/** Java calls a scenario makes that fail. */
	public static final class FailingSteps {

		static final IntegrityTerminalException MISSING_INPUT = new IntegrityTerminalException(Category.INPUT, "item input file is missing");

		private final boolean failing = true;

		public void fail() {
			if (failing) {
				throw new AssertionError(MARKER);
			}
		}

		public void missingInput() {
			if (failing) {
				throw MISSING_INPUT;
			}
		}
	}

	/** A step in the list workload's {@code start()/await()/close()} form, whose await or close fails. */
	public static final class StubStep {

		private final boolean failAwait;
		private final boolean failClose;

		StubStep(final boolean failAwait, final boolean failClose) {
			this.failAwait = failAwait;
			this.failClose = failClose;
		}

		public StubStep start() {
			return this;
		}

		public void await() {
			if (failAwait) {
				throw new IllegalStateException(MARKER);
			}
		}

		public void close() {
			if (failClose) {
				throw new IllegalStateException(MARKER);
			}
		}
	}

	/** A step whose await blocks until the run is stopped, as a running step does. */
	public static final class BlockingStep {

		public BlockingStep start() {
			return this;
		}

		public void await() throws InterruptedException {
			new CountDownLatch(1).await();
		}

		public void close() {}
	}

	/** A step whose await, once the run is stopped, fails with an ordinary exception. */
	public static final class InterruptionToFailureStep {

		public InterruptionToFailureStep start() {
			return this;
		}

		public void await() {
			try {
				new CountDownLatch(1).await();
			} catch (final InterruptedException e) {
				throw new IllegalStateException("await failed after the stop");
			}
		}

		public void close() {}
	}
}
