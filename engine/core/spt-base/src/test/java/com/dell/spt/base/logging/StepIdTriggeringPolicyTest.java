package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.junit.jupiter.api.Test;

final class StepIdTriggeringPolicyTest {

	@Test
	void returnsFalseWhenNoStepId() {
		final var policy = StepIdTriggeringPolicy.createPolicy();
		final var mgr = mock(RollingFileManager.class);
		policy.initialize(mgr);

		final var event = mock(LogEvent.class);
		final var ctx = mock(ReadOnlyStringMap.class);
		when(event.getContextData()).thenReturn(ctx);
		when(ctx.getValue(any())).thenReturn(null);

		assertFalse(policy.isTriggeringEvent(event));
		verifyNoInteractions(mgr);
	}

	// @Test
	// void triggersOnFirstNewStepIdOnly() {
	//     final var policy = StepIdTriggeringPolicy.createPolicy();
	//     final var mgr = mock(RollingFileManager.class);
	//     policy.initialize(mgr);

	//     final var event = mock(LogEvent.class);
	//     final var ctx = mock(ReadOnlyStringMap.class);
	//     when(event.getContextData()).thenReturn(ctx);

	//     // First event with step-1 -> trigger
	//     when(ctx.getValue("stepId")).thenReturn("step-1");
	//     assertTrue(policy.isTriggeringEvent(event));
	//     verify(mgr, times(1)).getFileName();

	//     // Subsequent event with same step-1 -> no trigger
	//     assertFalse(policy.isTriggeringEvent(event));

	//     // New step-2 -> trigger again
	//     when(ctx.getValue("stepId")).thenReturn("step-2");
	//     assertTrue(policy.isTriggeringEvent(event));
	//     verify(mgr, times(2)).getFileName();
	// }
}
