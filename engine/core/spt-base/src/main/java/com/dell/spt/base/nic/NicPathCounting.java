package com.dell.spt.base.nic;

/** A storage driver with a NIC set, whose per-path counters its step publishes (item 18). */
public interface NicPathCounting {

	/**
	 * The terminal rows once the driver's stop has run; null before that, and when the NIC set
	 * never opened.
	 */
	NicPathRows nicPathRows();
}
