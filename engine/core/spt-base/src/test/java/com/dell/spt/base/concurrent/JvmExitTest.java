package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JvmExitTest {

	@AfterEach
	void clearFlag() {
		JvmExit.reset();
	}

	@Test
	void theFlagStaysSetOnceMarked() {
		assertFalse(JvmExit.begun());
		JvmExit.markBegun();
		assertTrue(JvmExit.begun());
		assertTrue(JvmExit.begun());
	}

	@Test
	void onlyTheFirstStoppedStartOfAnExitReportsIt() {
		assertFalse(JvmExit.claimStartStopReport(), "no exit has begun");
		JvmExit.markBegun();
		assertTrue(JvmExit.claimStartStopReport());
		assertFalse(JvmExit.claimStartStopReport());
	}

	@Test
	void closingEveryDaemonIsNotAJvmExit() {
		DaemonBase.closeAll();
		assertFalse(JvmExit.begun(), "only the shutdown hook's thread marks the exit");
	}
}
