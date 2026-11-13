package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.junit.jupiter.api.Test;

/**
 * An engine started with a scenario learns of the scenario's failure from
 * {@link RunImpl#runAndReturnFailure()} and exits 1; a node's {@link RunImpl#run()} keeps reporting
 * the failure through the step status.
 */
class RunImplFailureTest {

	@Test
	void aScenarioThatRunsToItsEndReturnsNoFailure() throws Exception {
		final var engine = mock(ScriptEngine.class);
		when(engine.eval("scenario")).thenReturn(null);
		assertNull(new RunImpl("", "scenario", engine, 1L).runAndReturnFailure());
	}

	@Test
	void aScenarioSyntaxErrorIsReturned() throws Exception {
		final var engine = mock(ScriptEngine.class);
		final var syntaxError = new ScriptException("Expected comma but found )", "<eval>", 1, 53);
		when(engine.eval("scenario")).thenThrow(syntaxError);
		assertSame(syntaxError, new RunImpl("", "scenario", engine, 2L).runAndReturnFailure());
	}

	@Test
	void aScenarioConfigurationErrorIsReturned() throws Exception {
		// Nashorn passes a Java exception from Load.config(...) through as it is
		final var engine = mock(ScriptEngine.class);
		final var configError = new InvalidValuePathException("load-step-limit-count");
		when(engine.eval("scenario")).thenThrow(configError);
		assertSame(configError, new RunImpl("", "scenario", engine, 3L).runAndReturnFailure());
	}

	@Test
	void aNodeRunStillSwallowsTheSameFailures() throws Exception {
		final var engine = mock(ScriptEngine.class);
		when(engine.eval("syntax")).thenThrow(new ScriptException("Expected comma but found )"));
		when(engine.eval("config")).thenThrow(new InvalidValuePathException("load-step-limit-count"));
		assertDoesNotThrow(() -> new RunImpl("", "syntax", engine, 4L).run());
		assertDoesNotThrow(() -> new RunImpl("", "config", engine, 5L).run());
	}

	@Test
	void aTypedTerminalFailureIsStillThrown() throws Exception {
		final var engine = mock(ScriptEngine.class);
		final var terminal = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, "read", "invalid manifest", null);
		when(engine.eval("runtime")).thenThrow(new IllegalStateException("wrapper", terminal));
		when(engine.eval("script")).thenThrow(new ScriptException(new RuntimeException(terminal)));
		assertSame(
						terminal,
						assertThrows(
										IntegrityTerminalException.class,
										() -> new RunImpl("", "runtime", engine, 6L).runAndReturnFailure()));
		assertSame(
						terminal,
						assertThrows(
										IntegrityTerminalException.class,
										() -> new RunImpl("", "script", engine, 7L).runAndReturnFailure()));
	}
}
