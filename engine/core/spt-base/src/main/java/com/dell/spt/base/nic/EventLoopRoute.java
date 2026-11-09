package com.dell.spt.base.nic;

/** The route a driver's event loops serve, by its name in {@code event.loops.json}. */
public enum EventLoopRoute {
	DEFAULT("default"), NIC_SET("nic_set");

	private final String reportName;

	EventLoopRoute(final String reportName) {
		this.reportName = reportName;
	}

	/** Its name in {@code event.loops.json}. */
	public String reportName() {
		return reportName;
	}
}
