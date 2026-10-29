package com.dell.spt.base.nic;

/** A storage driver with a NIC set, whose per-path counters and event loops its step publishes (items 17, 18). */
public interface NicPathCounting {

	/**
	 * The terminal rows once the driver's stop has run; null before that, and when the NIC set
	 * never opened.
	 */
	NicPathRows nicPathRows();

	/** The driver's event loops; its NICs are empty before the NIC set opened, and when it never did. */
	NicSetResources nicSetResources();
}
