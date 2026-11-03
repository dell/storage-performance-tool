package com.dell.spt.base.load.step;

import java.io.Serializable;

/**
 * One slice's progress through the preparation a NIC-set step does while admission is held.
 *
 * @param state where the preparation is
 * @param phase what it is doing, or was doing when it ended
 * @param message why it failed or was fenced; empty otherwise
 * @param elapsedNanos how long it has run, or ran until it ended
 */
public record PreparationStatus(State state, String phase, String message, long elapsedNanos)
				implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The message of a preparation fenced at its own deadline. */
	public static final String DEADLINE_MESSAGE = "preparation did not finish before its deadline";

	/** Returns whether this preparation was fenced at its own deadline. */
	public boolean deadlinePassed() {
		return state == State.FENCED && DEADLINE_MESSAGE.equals(message);
	}

	/** A preparation is fenced for good: nothing later can open admission for it. */
	public enum State {
		RUNNING, READY, FAILED, FENCED
	}
}
