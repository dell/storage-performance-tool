package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.file.FileManager;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeleteArtifactLoggerRegistrationTest {

	@Test
	void everyDeleteArtifactHasAReflectionVisibleStepScopedAppender() throws Exception {
		final Map<String, String> expected = Map.of(
						Loggers.DELETE_METRICS_TOTAL.getName(), "delete.metrics.total.csv",
						Loggers.DELETE_REQUESTS.getName(), "delete.requests.csv",
						Loggers.DELETE_OBJECTS.getName(), "delete.objects.csv",
						Loggers.DELETE_RESIDUAL.getName(), "items.csv",
						Loggers.DELETE_SELECTION.getName(), "verify-input.csv",
						Loggers.DELETE_SELECTION_COMPLETION.getName(), "verify-input.complete.json",
						Loggers.DELETE_COMPLETION.getName(), "delete.complete.json");

		for (final var entry : expected.entrySet()) {
			final String path = FileManager.INSTANCE.logFileName(entry.getKey(), "delete-step");
			assertEquals(entry.getValue(), Path.of(path).getFileName().toString());
			final String shortName = entry.getKey().substring(Loggers.BASE.length());
			assertTrue(Loggers.DESCRIPTIONS_BY_NAME.containsKey(shortName));
		}
	}
}
