package com.dell.spt.base.metrics.snapshot;

/** @author veronika K. */
public interface DistributedAllMetricsSnapshot extends AllMetricsSnapshot {

	/** @return values in microseconds */
	int nodeCount();
}
