package com.dell.spt.base.nic;

import java.util.List;

/**
 * One NIC-set driver's event loops (D64), fixed when its NIC set opens.
 *
 * @param driverThreads the configured {@code storage.driver.threads}, or 0 for the default
 * @param eventLoopsPerNic each NIC's group size: {@code driverThreads} when set, otherwise
 *     {@code max(4, hardware threads / 8)} (D49)
 * @param nics the NICs that opened, in layout order; empty when the NIC set never opened
 * @param baseEventLoops the driver's base group, which carries its control requests, as created
 */
public record NicSetResources(int driverThreads, int eventLoopsPerNic, List<String> nics, int baseEventLoops) {

	public NicSetResources {
		nics = List.copyOf(nics);
	}

	/** The event loops of the driver's per-NIC groups, over all its NICs. */
	public int eventLoops() {
		return eventLoopsPerNic * nics.size();
	}

	/** Every event loop the driver owns: its per-NIC groups and its base group. */
	public int totalEventLoops() {
		return eventLoops() + baseEventLoops;
	}
}
