package com.dell.spt.base.metrics.snapshot;

/** Snapshot of aggregate metrics across all participating nodes. */
public interface DistributedAllMetricsSnapshot extends AllMetricsSnapshot {

	/** Returns the number of nodes represented in the snapshot. */
	int nodeCount();
}
