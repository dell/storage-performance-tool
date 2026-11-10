package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;

class NicStepFileTest {

	/** A logging configuration that gives the logger no file has nothing for a step to remove. */
	@Test
	void aLoggerWithoutAFileHasNothingToDiscard() {
		assertDoesNotThrow(() -> NicStepFile.discard(
						LogManager.getLogger(NicStepFileTest.class.getName() + ".NoFile"), "step", "test"));
	}
}
