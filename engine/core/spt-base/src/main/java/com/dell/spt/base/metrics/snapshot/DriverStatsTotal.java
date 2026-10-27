package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The storage driver statistics of a distributed step: the sum of what its contributors reported.
 *
 * <p>Counters and gauges are both summed by name. A summed gauge adds levels that each
 * contributor read at its own moment; it is not a level the fleet held at one moment.
 *
 * <p>Contributors that report different drivers have no common names to sum. The total then has
 * no sum and only says which drivers were seen.
 */
public final class DriverStatsTotal implements Serializable {

	private final DriverStatsSnapshot sum;
	private final TreeSet<String> drivers;
	private final int reporting;
	private final boolean partial;

	private DriverStatsTotal(
					final DriverStatsSnapshot sum, final Set<String> drivers, final int reporting, final boolean partial) {
		this.sum = sum;
		this.drivers = new TreeSet<>(drivers);
		this.reporting = reporting;
		this.partial = partial;
	}

	/**
	 * Totals what the present contributors of a step reported.
	 *
	 * @param contributions the statistics of each present contributor; null for a contributor that
	 *     reported none
	 * @param allPresent whether every expected contributor is among them
	 * @return the total, or null when no contributor reported statistics
	 */
	public static DriverStatsTotal of(final List<DriverStatsSnapshot> contributions, final boolean allPresent) {
		final Set<String> drivers = new TreeSet<>();
		final Map<String, Long> counters = new TreeMap<>();
		final Map<String, Long> gauges = new TreeMap<>();
		DriverStatsSnapshot first = null;
		boolean sameNames = true;
		int reporting = 0;
		for (final DriverStatsSnapshot stats : contributions) {
			if (stats == null) {
				continue;
			}
			reporting++;
			drivers.add(stats.driver());
			if (first == null) {
				first = stats;
			} else if (!stats.counters().keySet().equals(first.counters().keySet())
							|| !stats.gauges().keySet().equals(first.gauges().keySet())) {
				sameNames = false;
			}
			stats.counters().forEach((name, value) -> counters.merge(name, value, Long::sum));
			stats.gauges().forEach((name, value) -> gauges.merge(name, value, Long::sum));
		}
		if (first == null) {
			return null;
		}
		if (drivers.size() > 1) {
			return new DriverStatsTotal(null, drivers, 0, true);
		}
		final DriverStatsSnapshot.Builder sum = DriverStatsSnapshot.builder(first.driver());
		counters.forEach(sum::counter);
		gauges.forEach(sum::gauge);
		final boolean complete = allPresent && reporting == contributions.size() && sameNames;
		return new DriverStatsTotal(sum.build(), drivers, reporting, !complete);
	}

	/** Returns the sum by name, or null when the contributors reported different drivers. */
	public DriverStatsSnapshot sum() {
		return sum;
	}

	/** Returns the driver types the contributors reported, in name order. */
	public Set<String> drivers() {
		return Collections.unmodifiableSet(drivers);
	}

	/** Returns the number of contributors whose statistics are in the sum. */
	public int reporting() {
		return reporting;
	}

	/**
	 * Returns false only when every expected contributor is present and reported statistics for
	 * the same driver under the same names.
	 */
	public boolean partial() {
		return partial;
	}

	@Override
	public boolean equals(final Object other) {
		return other instanceof DriverStatsTotal that
						&& Objects.equals(sum, that.sum)
						&& drivers.equals(that.drivers)
						&& reporting == that.reporting
						&& partial == that.partial;
	}

	@Override
	public int hashCode() {
		return Objects.hash(sum, drivers, reporting, partial);
	}

	@Override
	public String toString() {
		return "DriverStatsTotal(" + sum + ", drivers=" + drivers + ", reporting=" + reporting + ", partial=" + partial
						+ ")";
	}
}
