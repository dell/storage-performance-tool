package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import org.junit.jupiter.api.Test;

class ApiStatusTest {

	@Test
	void defaultsToIdleAndTransitions() {
		final var status = new ApiStatus();
		assertEquals(ApiStatus.State.IDLE, status.state());
		assertEquals(0L, status.runId());
		assertNull(status.stepId());

		status.setRunning("step-1", 42L);
		assertEquals(ApiStatus.State.RUNNING, status.state());
		assertEquals(42L, status.runId());
		assertEquals("step-1", status.stepId());

		status.completeIfNotStopped();
		assertEquals(ApiStatus.State.COMPLETED, status.state());
		status.setStopped();
		assertEquals(ApiStatus.State.COMPLETED, status.state());

		status.setRunning("step-2", 77L);
		status.setStopped();
		status.completeIfNotStopped();
		assertEquals(ApiStatus.State.STOPPED, status.state());
		assertEquals(77L, status.runId());
		assertEquals("step-2", status.stepId());

		status.setIdle();
		assertEquals(ApiStatus.State.IDLE, status.state());
		assertEquals(0L, status.runId());
		status.setStopped();
		assertEquals(ApiStatus.State.IDLE, status.state());
		assertNull(status.stepId());
	}

	@Test
	void failureDominatesStopInEitherOrderingAndCompletion() {
		final var failureThenStop = new ApiStatus();
		failureThenStop.setRunning("read", 101L);
		failureThenStop.setFailed(
						"read", IntegrityTerminalException.Category.PUBLICATION, "publish failed");
		failureThenStop.setStopped();
		failureThenStop.completeIfNotStopped();
		assertFailed(failureThenStop, "read", "publish failed");

		final var stopThenFailure = new ApiStatus();
		stopThenFailure.setRunning("read", 102L);
		stopThenFailure.setStopped();
		stopThenFailure.setFailed(
						"read", IntegrityTerminalException.Category.PUBLICATION, "publish failed");
		stopThenFailure.completeIfNotStopped();
		assertFailed(stopThenFailure, "read", "publish failed");
	}

	@Test
	void firstFailureIsStickyAndMessageIsSanitizedAndBounded() {
		final var status = new ApiStatus();
		status.setRunning("read", 103L);
		status.setFailed(
						"read",
						IntegrityTerminalException.Category.INPUT,
						"bad\nrow\t" + "x".repeat(ApiStatus.MAX_FAILURE_MESSAGE_LENGTH));
		status.setFailed(
						"later", IntegrityTerminalException.Category.CLEANUP, "must not replace first");

		assertEquals(IntegrityTerminalException.Category.INPUT, status.failureCategory());
		assertEquals("read", status.stepId());
		assertFalse(status.failureMessage().contains("\n"));
		assertFalse(status.failureMessage().contains("\t"));
		assertEquals(ApiStatus.MAX_FAILURE_MESSAGE_LENGTH, status.failureMessage().length());
	}

	private static void assertFailed(
					final ApiStatus status, final String stepId, final String message) {
		assertEquals(ApiStatus.State.FAILED, status.state());
		assertEquals(stepId, status.stepId());
		assertEquals(IntegrityTerminalException.Category.PUBLICATION, status.failureCategory());
		assertEquals(message, status.failureMessage());
	}
}
