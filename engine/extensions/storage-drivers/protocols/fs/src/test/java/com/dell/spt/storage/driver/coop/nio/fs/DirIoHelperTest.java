package com.dell.spt.storage.driver.coop.nio.fs;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DirIoHelperTest {

	@Test
	public void createParentDirCreatesAndReturnsFile() throws Exception {
		final Path tmp = Files.createTempDirectory("fs-dir-io-helper-");
		final String sub = tmp.resolve("subdir").toString();

		final File created = DirIoHelper.createParentDir(sub);
		assertNotNull(created, "Expected non-null File for valid path");
		assertTrue(created.exists(), "Directory should exist after creation");
		assertTrue(created.isDirectory(), "Created path should be a directory");
	}

	@Test
	public void createParentDirReturnsSameIfExists() throws Exception {
		final Path tmp = Files.createTempDirectory("fs-dir-io-helper-exists-");
		final File created = DirIoHelper.createParentDir(tmp.toString());
		assertNotNull(created);
		assertEquals(tmp.toFile().getAbsolutePath(), created.getAbsolutePath());
		assertTrue(created.exists());
	}

	@Test
	public void createParentDirReturnsNullOnException() {
		// Passing null should cause an exception internally and return null
		final File created = DirIoHelper.createParentDir(null);
		assertNull(created, "Expected null when path is invalid");
	}
}
