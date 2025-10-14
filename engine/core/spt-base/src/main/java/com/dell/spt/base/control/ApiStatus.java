package com.dell.spt.base.control;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe holder for node run status exposed via /status.
 */
public final class ApiStatus {

	public enum State {
		IDLE, RUNNING, COMPLETED, STOPPED
	}

	private final AtomicReference<State> stateRef = new AtomicReference<>(State.IDLE);
	private final AtomicLong sinceMillisRef = new AtomicLong(Instant.now().toEpochMilli());
	private final AtomicLong runIdRef = new AtomicLong(0L);
	private final AtomicReference<String> stepIdRef = new AtomicReference<>(null);

	public void setIdle() {
		stateRef.set(State.IDLE);
		sinceMillisRef.set(System.currentTimeMillis());
		runIdRef.set(0L);
		stepIdRef.set(null);
	}

	public void setRunning(final String stepId, final long runId) {
		stepIdRef.set(stepId);
		runIdRef.set(runId);
		stateRef.set(State.RUNNING);
		sinceMillisRef.set(System.currentTimeMillis());
	}

	public void setStopped() {
		stateRef.set(State.STOPPED);
		sinceMillisRef.set(System.currentTimeMillis());
	}

	/**
	 * Transition to COMPLETED only if the state is not STOPPED (i.e., DELETE /run already handled).
	 */
	public void completeIfNotStopped() {
		if (stateRef.get() != State.STOPPED) {
			stateRef.set(State.COMPLETED);
			sinceMillisRef.set(System.currentTimeMillis());
		}
	}

	public State state() {
		return stateRef.get();
	}

	public long sinceMillis() {
		return sinceMillisRef.get();
	}

	public long runId() {
		return runIdRef.get();
	}

	public String stepId() {
		return stepIdRef.get();
	}
}
