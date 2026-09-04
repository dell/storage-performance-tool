package com.dell.spt.base.load.failure;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_FIXED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_PERCENTAGE;

/** Supported controller-level object failure policies. */
public enum ObjectFailureBudgetMode {
	FIXED(DELETE_FAILURE_POLICY_MODE_FIXED), PERCENTAGE(DELETE_FAILURE_POLICY_MODE_PERCENTAGE);

	private final String wireValue;

	ObjectFailureBudgetMode(final String wireValue) {
		this.wireValue = wireValue;
	}

	/** Returns the stable schema-v4 failure-policy value. */
	public String wireValue() {
		return wireValue;
	}

	/** Resolves only the explicit schema-v4 failure-policy vocabulary. */
	public static ObjectFailureBudgetMode fromWireValue(final String value) {
		final String normalized = value == null ? "" : value.trim();
		return switch (normalized) {
		case DELETE_FAILURE_POLICY_MODE_FIXED -> FIXED;
		case DELETE_FAILURE_POLICY_MODE_PERCENTAGE -> PERCENTAGE;
		default -> throw new IllegalArgumentException("unknown failure-budget mode: " + value);
		};
	}
}
