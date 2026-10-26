package com.dell.spt.base.metrics.steady;

/** What a Steady-State Window's measurement covers. It never reflects whether the step failed. */
public enum SteadyStatus {

	/** At least the minimum turnovers and the minimum duration. */
	VALID("valid"),
	/** A window exists but misses a criterion, including a zero-length window or one without completions. */
	SHORT("short"),
	/** The window closed before it started. */
	NOT_REACHED("not-reached"),
	/** Entry Node only: a contributor is missing or has no closed window. */
	INCOMPLETE("incomplete");

	private final String label;

	SteadyStatus(final String label) {
		this.label = label;
	}

	/** The value written to {@code SteadyStatus} and the JSON {@code status}. */
	public String label() {
		return label;
	}

	/** Whether the window's statistics are reported. */
	public boolean hasStatistics() {
		return this == VALID || this == SHORT;
	}
}
