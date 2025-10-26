package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class LogUtilColorTest {

	@Test
	@DisplayName("Zero failures -> strong green code")
	void zeroFailures() {
		final var c = LogUtil.getFailureRatioAnsiColorCode(100, 0);
		assertTrue(c.startsWith("\u001B[38;2;"));
		assertTrue(c.endsWith("m"));
		assertEquals("\u001B[38;2;0;200;0m", c);
	}

	@Test
	@DisplayName("All failures or more -> red shade")
	void allFailuresOrMore() {
		final var c = LogUtil.getFailureRatioAnsiColorCode(10, 10);
		assertEquals("\u001B[38;2;227;0;0m", c);
	}

	@Test
	@DisplayName("Some failures less than successes -> mixed RG")
	void partialFailures() {
		final var c = LogUtil.getFailureRatioAnsiColorCode(90, 10);
		assertEquals("\u001B[38;2;126;180;0m", c);
	}
}
