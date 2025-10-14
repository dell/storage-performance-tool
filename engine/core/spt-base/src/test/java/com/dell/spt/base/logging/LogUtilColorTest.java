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

	// @Test
	// @DisplayName("All failures >= successes -> red shade")
	// void allFailuresOrMore() {
	//     final var c = LogUtil.getFailureRatioAnsiColorCode(10, 10);
	//     assertTrue(c.startsWith("\u001B[38;2;"));
	//     // format \u001B[38;2;R;0;0m with R in [200,255]
	//     assertTrue(c.matches("\\\\u001B\\[38;2;2[0-5][0-9];0;0m"));
	// }

	// @Test
	// @DisplayName("Some failures < successes -> mixed RG")
	// void partialFailures() {
	//     final var c = LogUtil.getFailureRatioAnsiColorCode(90, 10);
	//     assertTrue(c.startsWith("\u001B[38;2;"));
	//     // R;G;B=0 with R,G numeric
	//     assertTrue(c.matches("\\\\u001B\\[38;2;[0-9]{1,3};[0-9]{1,3};0m"));
	// }
}
