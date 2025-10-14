package com.dell.spt.base.control;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApiStatusTest {

	@Test
	void defaultsToIdleAndTransitions() {
		ApiStatus s = new ApiStatus();
		assertEquals(ApiStatus.State.IDLE, s.state());
		assertEquals(0L, s.runId());
		assertNull(s.stepId());

		s.setRunning("step-1", 42L);
		assertEquals(ApiStatus.State.RUNNING, s.state());
		assertEquals(42L, s.runId());
		assertEquals("step-1", s.stepId());

		// Completion should set COMPLETED
		s.completeIfNotStopped();
		assertEquals(ApiStatus.State.COMPLETED, s.state());

		// STOPPED should not be overridden by completion
		s.setRunning("step-2", 77L);
		s.setStopped();
		s.completeIfNotStopped();
		assertEquals(ApiStatus.State.STOPPED, s.state());
		assertEquals(77L, s.runId());
		assertEquals("step-2", s.stepId());

		// setIdle resets identifiers
		s.setIdle();
		assertEquals(ApiStatus.State.IDLE, s.state());
		assertEquals(0L, s.runId());
		assertNull(s.stepId());
	}
}
