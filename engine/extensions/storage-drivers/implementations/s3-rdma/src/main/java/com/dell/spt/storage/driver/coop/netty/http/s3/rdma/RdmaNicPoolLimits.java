package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;

/**
 * The size of one NIC Pool (item 14, E7, S1).
 *
 * @param budgetBytes its part of the driver's pool budget, split equally among the NICs
 * @param minimum the buffers per size class its demand requires: its share of the concurrency limit,
 *     or the whole limit in the shared mode; 0 without a limit, whose demand is unknown
 * @param cap the most buffers it holds per size class
 */
record RdmaNicPoolLimits(long budgetBytes, int minimum, int cap) {

	/** The buffers above the minimum the cap allows. */
	int headroom() {
		return cap - minimum;
	}

	/**
	 * @param concurrencyLimit the driver's limit {@code P}; 0 or less for none
	 * @param nics the driver's NIC count {@code N}
	 * @param index the NIC's index in layout order
	 * @param driverBudgetBytes the driver's whole pool budget
	 * @param unlimitedCap the cap without a concurrency limit
	 */
	static RdmaNicPoolLimits of(final int concurrencyLimit, final int nics, final int index, final long driverBudgetBytes,
					final int unlimitedCap) {
		final long budget = driverBudgetBytes / nics;
		if (concurrencyLimit <= 0) {
			return new RdmaNicPoolLimits(budget, 0, unlimitedCap);
		}
		final int minimum = NicSetSettings.hasShares(concurrencyLimit, nics)
						? NicSetSettings.openLimit(concurrencyLimit, nics, index)
						: concurrencyLimit;
		return new RdmaNicPoolLimits(budget, minimum, minimum + 1);
	}
}
