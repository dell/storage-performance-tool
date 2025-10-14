package com.dell.spt.storage.driver.coop.nio.fs;

import org.junit.jupiter.api.Test;

import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class FsConstantsTest {

	@Test
	public void fsAndProviderAreAvailable() {
		assertNotNull(FsConstants.FS);
		assertNotNull(FsConstants.FS_PROVIDER);
	}

	@Test
	public void openOptionsContainExpectedFlags() {
		assertTrue(FsConstants.CREATE_OPEN_OPT.contains(StandardOpenOption.CREATE));
		assertTrue(FsConstants.CREATE_OPEN_OPT.contains(StandardOpenOption.TRUNCATE_EXISTING));
		assertTrue(FsConstants.CREATE_OPEN_OPT.contains(StandardOpenOption.WRITE));

		assertTrue(FsConstants.READ_OPEN_OPT.contains(StandardOpenOption.READ));

		assertTrue(FsConstants.WRITE_OPEN_OPT.contains(StandardOpenOption.WRITE));
		// Only requirement is WRITE present; no assumption about others
		for (OpenOption opt : FsConstants.WRITE_OPEN_OPT) {
			assertNotNull(opt);
		}
	}
}
