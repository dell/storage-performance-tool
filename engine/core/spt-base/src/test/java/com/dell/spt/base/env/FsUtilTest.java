package com.dell.spt.base.env;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FsUtilTest {

	@TempDir
	Path tmpDir;

	@Test
	void createsParentDirsAndIsIdempotent() throws Exception {
		final var nested = tmpDir.resolve("a/b/c/output.txt");

		// Should create parent directories when missing
		FsUtil.createParentDirsIfNotExist(nested);
		assertTrue(Files.exists(nested.getParent()));
		assertTrue(Files.isDirectory(nested.getParent()));

		// Call again should not throw and keep directories
		FsUtil.createParentDirsIfNotExist(nested);
		assertTrue(Files.exists(nested.getParent()));
	}
}
