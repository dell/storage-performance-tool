package com.dell.spt.base.nic;

import java.util.List;

/**
 * One NIC-set driver's event loops (D64), fixed when its NIC set opens.
 *
 * @param driverThreads the configured {@code storage.driver.threads}, or 0 for the default
 * @param eventLoopsPerNic each NIC's group size: {@code driverThreads} when set, otherwise
 *     {@code max(4, hardware threads / 8)} (D49)
 * @param nics the NICs that opened, in layout order; empty when the NIC set never opened
 */
public record NicSetResources(int driverThreads, int eventLoopsPerNic, List<String> nics) {

	public NicSetResources {
		nics = List.copyOf(nics);
	}

	/** The driver's event loops over all its NICs. */
	public int eventLoops() {
		return eventLoopsPerNic * nics.size();
	}
}
