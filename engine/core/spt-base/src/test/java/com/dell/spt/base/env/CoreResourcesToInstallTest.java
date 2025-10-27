package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class CoreResourcesToInstallTest {

	@Test
	void resourceFilesToInstallMatchesManifest() throws IOException {
		final var underTest = new CoreResourcesToInstall();
		final List<String> actual = underTest.resourceFilesToInstall();
		try (
						var in = Objects.requireNonNull(
										getClass().getResourceAsStream(CoreResourcesToInstall.RESOURCES_FILE_NAME),
										"missing manifest resource");
						var reader = new LineNumberReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
			final var expected = reader.lines().toList();
			assertEquals(expected, actual, "Resource manifest should be read using UTF-8 without loss");
		}
	}
}
