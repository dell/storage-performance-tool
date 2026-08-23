package com.dell.spt.base.load.failure;

/** Globally aggregated DELETE evidence consumed only by the controller policy. */
public record ObjectFailureBudgetCounters(
			long selectedObjects,
			long attemptedObjects,
			long acceptedObjects,
			long operationalFailedObjects,
			long excludedFailedObjects,
			long unattemptedObjects,
			long unresolvedObjects,
			long fullSuccessfulRequests,
			boolean terminalComplete) {

	/** Cumulative terminal operational outcomes used as the percentage denominator. */
	public long attemptedOperationalOutcomes() {
		return Math.addExact(acceptedObjects, operationalFailedObjects);
	}
}
