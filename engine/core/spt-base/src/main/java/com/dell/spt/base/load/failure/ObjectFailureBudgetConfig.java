package com.dell.spt.base.load.failure;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import java.time.Duration;
import java.util.Locale;

/** Immutable additive configuration for controller-level failed-object policy. */
public record ObjectFailureBudgetConfig(
			ObjectFailureBudgetMode mode,
			long maxFailedObjects,
			double maxFailurePercent,
			Duration grace) {

	private static final String CONFIG_PATH = "op-failureBudget";
	private static final long DEFAULT_MAX_FAILED_OBJECTS = 100_000L;
	private static final Duration DEFAULT_GRACE = Duration.ofSeconds(30);

	public ObjectFailureBudgetConfig {
		if (mode == null) {
			throw invalid("failure budget mode is required");
		}
		if (maxFailedObjects < 0) {
			throw invalid("maxFailedObjects must be greater than or equal to zero");
		}
		if (!Double.isFinite(maxFailurePercent)
					|| maxFailurePercent < 0
					|| maxFailurePercent > 100) {
			throw invalid("maxFailurePercent must be between 0 and 100 inclusive");
		}
		if (grace == null || grace.isNegative()) {
			throw invalid("failure budget grace must be greater than or equal to zero");
		}
	}

	/** Reads the additive node without changing the legacy load.op.limit.fail controls. */
	public static ObjectFailureBudgetConfig from(final Config loadConfig) {
		try {
			final Config budget = loadConfig.configVal(CONFIG_PATH);
			final ObjectFailureBudgetMode mode;
			try {
				mode = ObjectFailureBudgetMode.valueOf(
						budget.stringVal("mode").trim().toUpperCase(Locale.ROOT));
			} catch (final RuntimeException failure) {
				throw invalid("failure budget mode must be fixed or percentage", failure);
			}
			return new ObjectFailureBudgetConfig(
					mode,
					budget.longVal("maxFailedObjects"),
					budget.doubleVal("maxFailurePercent"),
					Duration.ofSeconds(budget.longVal("graceSeconds")));
		} catch (final IllegalConfigurationException failure) {
			throw failure;
		} catch (final RuntimeException failure) {
			throw invalid("invalid load.op.failureBudget configuration", failure);
		}
	}

	public static ObjectFailureBudgetConfig fixed(final long maxFailedObjects) {
		return new ObjectFailureBudgetConfig(
				ObjectFailureBudgetMode.FIXED, maxFailedObjects, 0, DEFAULT_GRACE);
	}

	public static ObjectFailureBudgetConfig percentage(
			final double maxFailurePercent, final Duration grace) {
		return new ObjectFailureBudgetConfig(
				ObjectFailureBudgetMode.PERCENTAGE,
				DEFAULT_MAX_FAILED_OBJECTS,
				maxFailurePercent,
				grace);
	}

	/** Human-readable policy including the object unit. */
	public String description() {
		return mode == ObjectFailureBudgetMode.FIXED
				? "fixed failed-object limit " + maxFailedObjects
				: "failed-object percentage limit " + maxFailurePercent + "% "
						+ (maxFailurePercent == 0 ? "enforced immediately" : "after " + grace);
	}

	private static IllegalConfigurationException invalid(final String message) {
		return new IllegalConfigurationException(message);
	}

	private static IllegalConfigurationException invalid(
			final String message, final Throwable cause) {
		return new IllegalConfigurationException(message, cause);
	}
}
