package com.dell.spt.base.nic;

/** A storage driver that owns Netty event loops, which its step reports at stop in {@code event.loops.json}. */
public interface EventLoopCounting {

	/**
	 * The driver's event loops, read once per step report, never per operation; null when it has none
	 * to report.
	 */
	EventLoops eventLoops();
}
