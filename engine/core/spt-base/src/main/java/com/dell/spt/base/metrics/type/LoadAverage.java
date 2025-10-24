package com.dell.spt.base.metrics.type;

import java.util.concurrent.TimeUnit;

/** Interface for computing moving load averages with exponential decay. */
public interface LoadAverage {

	/**
	 * Update the moving average with a new value.
	 *
	 * @param n the new value
	 */
	void update(final long n);

	/** Mark the passage of time and decay the current rate accordingly. */
	void tick();

	/**
	 * Returns the rate in the given units of time.
	 *
	 * @param rateUnit the unit of time
	 * @return the rate
	 */
	double rate(final TimeUnit rateUnit);
}
