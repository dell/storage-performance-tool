package com.dell.spt.base.metrics;

import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;

/** Created by andrey on 06.07.17. The entity accepting the metrics updates */
@FunctionalInterface
public interface DistributedMetricsListener {

	/**
	* Update the state with the snapshot
	*
	* @param snapshot the metrics snapshot to update the metrics listener state
	*/
	void notify(final DistributedAllMetricsSnapshot snapshot);
}
