package com.dell.spt.base.metrics.context;

import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;

import java.util.List;

public interface DistributedMetricsContext<S extends DistributedAllMetricsSnapshot>
				extends MetricsContext<S> {

	int nodeCount();

	List<String> nodeAddrs();

	List<Double> quantileValues();

	@Override
	S lastSnapshot();
}
