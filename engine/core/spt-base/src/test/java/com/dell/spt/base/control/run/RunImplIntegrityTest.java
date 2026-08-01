package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.client.CsvArtifactAggregator;
import com.dell.spt.base.load.step.file.FileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunImplIntegrityTest {
	@TempDir
	Path tempDir;

	@Test
	void typedTerminalCauseSurvivesRuntimeWrapper() throws Exception {
		final var engine = mock(ScriptEngine.class);
		final var terminal = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, "read", "invalid manifest", null);
		when(engine.eval("scenario")).thenThrow(new IllegalStateException("wrapper", terminal));

		final var thrown = assertThrows(
						IntegrityTerminalException.class,
						() -> new RunImpl("", "scenario", engine, 12L).run());
		assertSame(terminal, thrown);
	}

	@Test
	void typedTerminalCauseSurvivesScriptExceptionWrapper() throws Exception {
		final var engine = mock(ScriptEngine.class);
		final var terminal = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "read", "verification failed", null);
		when(engine.eval("scenario")).thenThrow(new ScriptException(new RuntimeException(terminal)));

		assertSame(
						terminal,
						assertThrows(
										IntegrityTerminalException.class,
										() -> new RunImpl("", "scenario", engine, 13L).run()));
	}

	@Test
	void unrelatedScriptFailureRetainsLegacyNonThrowingBehavior() throws Exception {
		final var engine = mock(ScriptEngine.class);
		when(engine.eval("scenario")).thenThrow(new ScriptException("ordinary failure"));
		assertDoesNotThrow(() -> new RunImpl("", "scenario", engine, 14L).run());
	}

	@Test
	void zeroSuccessfulWritesStopScenarioBeforeReadCallback() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\n");
		final var writeAggregation = new CsvArtifactAggregator(
						"create-step", List.of(FileManager.INSTANCE), List.of(),
						manifest.toString(), 16L, 0);
		final var readEntered = new AtomicBoolean();
		final var engine = mock(ScriptEngine.class);
		when(engine.eval("write-then-read")).thenAnswer(invocation -> {
			writeAggregation.close();
			readEntered.set(true);
			return null;
		});

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> new RunImpl("", "write-then-read", engine, 16L).run());

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("zero successful objects"));
		assertFalse(readEntered.get(), "READ callback ran after terminal zero-write evidence");
	}

	@Test
	void statusAwareRunRecordsTypedFailureBeforeCompletion() {
		final Run delegate = mock(Run.class);
		final var status = new ApiStatus();
		status.setRunning("configured-step", 15L);
		final var terminal = new IntegrityTerminalException(
						IntegrityTerminalException.Category.PUBLICATION,
						"failed-step",
						"rename failed",
						null);
		when(delegate.runId()).thenReturn(15L);
		doThrow(terminal).when(delegate).run();

		final var wrapped = new RunServlet.StatusAwareRun(delegate, status);
		assertSame(terminal, assertThrows(IntegrityTerminalException.class, wrapped::run));
		assertEquals(ApiStatus.State.FAILED, status.state());
		assertEquals("failed-step", status.stepId());
		assertEquals(IntegrityTerminalException.Category.PUBLICATION, status.failureCategory());
		assertEquals("rename failed", status.failureMessage());
	}
}
