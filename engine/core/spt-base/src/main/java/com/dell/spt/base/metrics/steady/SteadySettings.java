package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TimeUtil;
import com.github.akurilov.confuse.Config;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The {@code output.metrics.steady} settings of a Steady-State Window.
 *
 * @param warmupTurnovers completions per operation in flight before the window starts
 * @param minTurnovers the fewest turnovers a {@code valid} window covers; 0 disables the criterion
 * @param minDurationMillis the shortest {@code valid} window; 0 disables the criterion
 */
public record SteadySettings(double warmupTurnovers, double minTurnovers, long minDurationMillis) {

	public static final SteadySettings DEFAULTS = new SteadySettings(1, 20, TimeUnit.SECONDS.toMillis(20));

	public SteadySettings {
		requireNonNegative("output.metrics.steady.warmup.turnovers", warmupTurnovers);
		requireNonNegative("output.metrics.steady.min.turnovers", minTurnovers);
		requireNonNegative("output.metrics.steady.min.duration", minDurationMillis);
	}

	/** Reads the settings from the {@code output.metrics} configuration. */
	public static SteadySettings fromMetricsConfig(final Config metricsConfig) {
		return new SteadySettings(
						metricsConfig.doubleVal("steady-warmup-turnovers"),
						metricsConfig.doubleVal("steady-min-turnovers"),
						durationMillis(metricsConfig.val("steady-min-duration")));
	}

	private static final Pattern DURATION = Pattern.compile(
					"[0-9]+([smhdSMHD]|\\.[a-zA-Z]{4,7})?");

	private static long durationMillis(final Object raw) {
		if (raw instanceof String text) {
			if (!DURATION.matcher(text.trim()).matches()) {
				throw new IllegalConfigurationException(
								"output.metrics.steady.min.duration is not a duration: \"" + text + "\"");
			}
			final TimeUnit unit;
			final long value;
			try {
				unit = TimeUtil.getTimeUnit(text);
				value = TimeUtil.getTimeValue(text);
			} catch (final IllegalArgumentException e) {
				throw new IllegalConfigurationException(
								"output.metrics.steady.min.duration is not a duration: \"" + text + "\"", e);
			}
			return unit == null ? TimeUnit.SECONDS.toMillis(value) : unit.toMillis(value);
		}
		if (raw instanceof Number number) {
			return Math.round(number.doubleValue() * TimeUnit.SECONDS.toMillis(1));
		}
		throw new IllegalConfigurationException(
						"output.metrics.steady.min.duration is not a duration: " + raw);
	}

	private static void requireNonNegative(final String name, final long value) {
		if (value < 0) {
			throw new IllegalConfigurationException(name + " must be 0 or more, got " + value);
		}
	}

	private static void requireNonNegative(final String name, final double value) {
		if (!(value >= 0)) {
			throw new IllegalConfigurationException(name + " must be 0 or more, got " + value);
		}
	}
}
