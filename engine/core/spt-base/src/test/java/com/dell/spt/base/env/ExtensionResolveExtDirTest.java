package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExtensionResolveExtDirTest {

	private Path createdExtDir;

	@AfterEach
	void cleanUp() throws IOException {
		if (createdExtDir != null && Files.exists(createdExtDir)) {
			Files.delete(createdExtDir);
		}
	}

	/** Resolve the parent directory that resolveExtDir() will check for ext/. */
	private static Path codeSourceParent() throws URISyntaxException {
		final var codeSource = Extension.class.getProtectionDomain().getCodeSource();
		assertNotNull(codeSource, "CodeSource should be available in test environment");
		return Path.of(codeSource.getLocation().toURI()).getParent();
	}

	@Test
	void returnsNull_whenNoExtDirNextToCodeSource() throws URISyntaxException {
		final var parent = codeSourceParent();
		final var extPath = parent.resolve("ext");
		// Precondition: ext/ should not exist in the build tree normally
		if (Files.exists(extPath)) {
			// If it happens to exist, skip this test rather than assert incorrectly
			return;
		}
		assertNull(Extension.resolveExtDir(), "Should return null when ext/ directory does not exist");
	}

	@Test
	void returnsExtDir_whenExtDirExists() throws URISyntaxException, IOException {
		final var parent = codeSourceParent();
		createdExtDir = parent.resolve("ext");
		Files.createDirectories(createdExtDir);

		final File result = Extension.resolveExtDir();

		assertNotNull(result, "Should return non-null when ext/ exists next to code source");
		assertTrue(result.isDirectory(), "Returned file should be a directory");
		assertEquals(createdExtDir.toFile().getAbsolutePath(), result.getAbsolutePath());
	}

	@Test
	void returnsNull_whenExtPathIsFileNotDirectory() throws URISyntaxException, IOException {
		final var parent = codeSourceParent();
		createdExtDir = parent.resolve("ext");
		// Create ext as a regular file, not a directory
		Files.writeString(createdExtDir, "not a directory");

		assertNull(Extension.resolveExtDir(), "Should return null when ext is a file, not a directory");
	}
}
