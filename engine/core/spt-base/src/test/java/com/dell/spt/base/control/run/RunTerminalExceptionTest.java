package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.junit.jupiter.api.Test;

class RunTerminalExceptionTest {

	@Test
	void findLocatesGenericAndIntegrityCausesThroughWrappers() {
		final var generic = new RunTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "target bucket is missing", null);
		final var integrity = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, "manifest malformed");

		assertSame(generic, RunTerminalException.find(new IllegalStateException("outer", generic)));
		assertSame(integrity, RunTerminalException.find(new RuntimeException(integrity)));
		assertNull(RunTerminalException.find(new IllegalStateException("ordinary")));
		assertNull(IntegrityTerminalException.find(new IllegalStateException("outer", generic)),
						"integrity discovery must keep ignoring generic terminal causes");
	}

	@Test
	void findStopsOnCauseCycles() {
		final var first = new IllegalStateException("first");
		final var second = new IllegalStateException("second", first);
		first.initCause(second);

		assertNull(RunTerminalException.find(first));
	}

	@Test
	void withStepIdAttachesIdentityOnceAndKeepsCategoryMessageAndSuppressed() {
		final var original = new RunTerminalException(
						IntegrityTerminalException.Category.CONFIGURATION, "dynamic bucket", null);
		original.addSuppressed(new IllegalStateException("secondary"));

		final var attached = original.withStepId("create-step");

		assertNotSame(original, attached);
		assertEquals(RunTerminalException.class, attached.getClass());
		assertEquals("create-step", attached.stepId());
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, attached.category());
		assertEquals("dynamic bucket", attached.getMessage());
		assertSame(original, attached.getCause());
		assertEquals(1, attached.getSuppressed().length);
		assertSame(attached, attached.withStepId("other-step"));
		assertSame(original, original.withStepId(" "));
	}

	@Test
	void integrityTerminalExceptionIsARunTerminalException() {
		final RunTerminalException integrity = new IntegrityTerminalException(
						IntegrityTerminalException.Category.PUBLICATION, "rename failed");

		assertEquals(IntegrityTerminalException.Category.PUBLICATION, integrity.category());
		assertInstanceOf(IntegrityTerminalException.class, integrity.withStepId("read"));
	}

	@Test
	void genericTerminalCauseSurvivesRuntimeAndScriptWrappers() throws Exception {
		final var terminal = new RunTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "create", "target bucket is missing", null);
		final var runtimeEngine = mock(ScriptEngine.class);
		when(runtimeEngine.eval("scenario")).thenThrow(new IllegalStateException("wrapper", terminal));
		final var scriptEngine = mock(ScriptEngine.class);
		when(scriptEngine.eval("scenario")).thenThrow(new ScriptException(new RuntimeException(terminal)));

		assertSame(terminal, assertThrows(
						RunTerminalException.class, () -> new RunImpl("", "scenario", runtimeEngine, 21L).run()));
		assertSame(terminal, assertThrows(
						RunTerminalException.class, () -> new RunImpl("", "scenario", scriptEngine, 22L).run()));
	}

	@Test
	void statusAwareRunPublishesStickyGenericFailure() {
		final Run delegate = mock(Run.class);
		final var status = new ApiStatus();
		status.setRunning("configured-step", 23L);
		final var terminal = new RunTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"create",
						"target bucket is missing",
						new IllegalStateException("technical detail"));
		when(delegate.runId()).thenReturn(23L);
		doThrow(terminal).when(delegate).run();

		final var wrapped = new RunServlet.StatusAwareRun(delegate, status);
		assertSame(terminal, assertThrows(RunTerminalException.class, wrapped::run));

		assertEquals(ApiStatus.State.FAILED, status.state());
		assertEquals(23L, status.runId());
		assertEquals("create", status.stepId());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, status.failureCategory());
		assertEquals("target bucket is missing", status.failureMessage());
		status.completeIfNotStopped();
		status.setStopped();
		assertEquals(ApiStatus.State.FAILED, status.state(), "a generic FAILED status must stay sticky");
	}
}
