package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable counts and levels a storage driver reports about itself, by name.
 *
 * <p>A counter only grows during a step; a gauge is a level at the moment of the snapshot. Names
 * are lower-case with underscores and appear unchanged in the metrics JSON. The core does not
 * interpret them: it carries them to the node metrics row, sums them into fleet totals and
 * retains the final value of a step.
 */
public final class DriverStatsSnapshot implements Serializable {

	private final String driver;
	private final TreeMap<String, Long> counters;
	private final TreeMap<String, Long> gauges;

	private DriverStatsSnapshot(final Builder builder) {
		this.driver = builder.driver;
		this.counters = new TreeMap<>(builder.counters);
		this.gauges = new TreeMap<>(builder.gauges);
	}

	/**
	 * Starts a snapshot for the driver of the given type.
	 *
	 * @param driver the driver type, as selected by {@code storage.driver.type}
	 */
	public static Builder builder(final String driver) {
		return new Builder(driver);
	}

	/** Returns the type of the driver that produced the statistics. */
	public String driver() {
		return driver;
	}

	/** Returns the counters by name, in name order. */
	public Map<String, Long> counters() {
		return Collections.unmodifiableMap(counters);
	}

	/** Returns the gauges by name, in name order. */
	public Map<String, Long> gauges() {
		return Collections.unmodifiableMap(gauges);
	}

	@Override
	public boolean equals(final Object other) {
		return other instanceof DriverStatsSnapshot that
						&& driver.equals(that.driver)
						&& counters.equals(that.counters)
						&& gauges.equals(that.gauges);
	}

	@Override
	public int hashCode() {
		return Objects.hash(driver, counters, gauges);
	}

	@Override
	public String toString() {
		return "DriverStatsSnapshot(" + driver + ", counters=" + counters + ", gauges=" + gauges + ")";
	}

	/** Collects the named values of one snapshot. */
	public static final class Builder {

		private final String driver;
		private final TreeMap<String, Long> counters = new TreeMap<>();
		private final TreeMap<String, Long> gauges = new TreeMap<>();

		private Builder(final String driver) {
			this.driver = Objects.requireNonNull(driver, "driver type");
		}

		/** Sets a counter: a count of events that only grows. */
		public Builder counter(final String name, final long value) {
			counters.put(Objects.requireNonNull(name, "counter name"), value);
			return this;
		}

		/** Sets a gauge: a level that can rise and fall. */
		public Builder gauge(final String name, final long value) {
			gauges.put(Objects.requireNonNull(name, "gauge name"), value);
			return this;
		}

		public DriverStatsSnapshot build() {
			return new DriverStatsSnapshot(this);
		}
	}
}
