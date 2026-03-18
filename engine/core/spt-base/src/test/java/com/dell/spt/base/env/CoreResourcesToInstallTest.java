package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

	@Test
	void appHomePath_returnsTempDir_byDefault() {
		// Skip if $SPT_HOME is set in the environment — that overrides the temp-dir path
		final var sptHome = System.getenv("SPT_HOME");
		if (sptHome != null && !sptHome.isEmpty()) {
			return;
		}
		final var underTest = new CoreResourcesToInstall();
		final var path = underTest.appHomePath();
		assertNotNull(path, "appHomePath should not be null");
		assertTrue(Files.isDirectory(path), "appHomePath should be an existing directory");
		assertTrue(path.getFileName().toString().startsWith("spt-"),
						"Temp dir name should start with 'spt-', was: " + path.getFileName());
	}

	@Test
	void appHomePath_usesSptHomeEnvVar_ifSet() {
		final var sptHome = System.getenv("SPT_HOME");
		if (sptHome == null || sptHome.isEmpty()) {
			// Cannot test env-var branch without $SPT_HOME set; covered by appHomePath_returnsTempDir
			return;
		}
		final var underTest = new CoreResourcesToInstall();
		assertEquals(Path.of(sptHome), underTest.appHomePath(),
						"Should use $SPT_HOME when set");
	}

	@Nested
	class DeleteRecursively {

		@Test
		void removesDirectoryTree(@TempDir Path tmpDir) throws IOException {
			// Build a small tree: tmpDir/a/b/file.txt and tmpDir/a/other.txt
			final var dirA = tmpDir.resolve("a");
			final var dirB = dirA.resolve("b");
			Files.createDirectories(dirB);
			Files.writeString(dirB.resolve("file.txt"), "hello");
			Files.writeString(dirA.resolve("other.txt"), "world");

			CoreResourcesToInstall.deleteRecursively(dirA);

			assertFalse(Files.exists(dirA), "Directory tree should be fully removed");
		}

		@Test
		void handlesEmptyDirectory(@TempDir Path tmpDir) throws IOException {
			final var emptyDir = tmpDir.resolve("empty");
			Files.createDirectory(emptyDir);

			CoreResourcesToInstall.deleteRecursively(emptyDir);

			assertFalse(Files.exists(emptyDir), "Empty directory should be removed");
		}

		@Test
		void doesNotThrowOnNonExistentPath(@TempDir Path tmpDir) {
			final var missing = tmpDir.resolve("does-not-exist");
			// Should not throw — best-effort cleanup
			CoreResourcesToInstall.deleteRecursively(missing);
		}
	}
}
