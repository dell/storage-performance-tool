package com.dell.spt.base.control;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe holder for the atomic node run status exposed via {@code /status}. */
public final class ApiStatus {

	public static final int MAX_FAILURE_MESSAGE_LENGTH = 1024;

	public enum State {
		IDLE, RUNNING, COMPLETED, STOPPED, FAILED
	}

	public record Snapshot(
					State state,
					long sinceMillis,
					long runId,
					String stepId,
					IntegrityTerminalException.Category failureCategory,
					String failureMessage) {}

	private final AtomicReference<Snapshot> snapshotRef = new AtomicReference<>(
					new Snapshot(State.IDLE, Instant.now().toEpochMilli(), 0L, null, null, null));

	public void setIdle() {
		snapshotRef.set(new Snapshot(State.IDLE, System.currentTimeMillis(), 0L, null, null, null));
	}

	public void setRunning(final String stepId, final long runId) {
		snapshotRef.set(new Snapshot(
						State.RUNNING, System.currentTimeMillis(), runId, stepId, null, null));
	}

	public void setStopped() {
		snapshotRef.updateAndGet(current -> current.state() == State.RUNNING || current.state() == State.STOPPED
						? withState(current, State.STOPPED)
						: current);
	}

	public void setFailed(
					final String stepId,
					final IntegrityTerminalException.Category category,
					final String message) {
		if (category == null) {
			throw new IllegalArgumentException("failure category is required");
		}
		final String sanitizedMessage = sanitizeFailureMessage(message);
		snapshotRef.updateAndGet(current -> {
			if (current.state() == State.FAILED) {
				return current;
			}
			final String failedStepId = stepId == null || stepId.isBlank() ? current.stepId() : stepId;
			return new Snapshot(
							State.FAILED,
							System.currentTimeMillis(),
							current.runId(),
							failedStepId,
							category,
							sanitizedMessage);
		});
	}

	/** Transition to COMPLETED only from RUNNING. Terminal states remain sticky. */
	public void completeIfNotStopped() {
		snapshotRef.updateAndGet(current -> current.state() == State.RUNNING
						? withState(current, State.COMPLETED)
						: current);
	}

	public Snapshot snapshot() {
		return snapshotRef.get();
	}

	public State state() {
		return snapshot().state();
	}

	public long sinceMillis() {
		return snapshot().sinceMillis();
	}

	public long runId() {
		return snapshot().runId();
	}

	public String stepId() {
		return snapshot().stepId();
	}

	public IntegrityTerminalException.Category failureCategory() {
		return snapshot().failureCategory();
	}

	public String failureMessage() {
		return snapshot().failureMessage();
	}

	static String sanitizeFailureMessage(final String message) {
		if (message == null) {
			return "";
		}
		final var sanitized = new StringBuilder(Math.min(message.length(), MAX_FAILURE_MESSAGE_LENGTH));
		for (var i = 0; i < message.length() && sanitized.length() < MAX_FAILURE_MESSAGE_LENGTH; i++) {
			final char ch = message.charAt(i);
			sanitized.append(Character.isISOControl(ch) ? ' ' : ch);
		}
		return sanitized.toString();
	}

	private static Snapshot withState(final Snapshot current, final State state) {
		return new Snapshot(
						state,
						System.currentTimeMillis(),
						current.runId(),
						current.stepId(),
						current.failureCategory(),
						current.failureMessage());
	}
}
