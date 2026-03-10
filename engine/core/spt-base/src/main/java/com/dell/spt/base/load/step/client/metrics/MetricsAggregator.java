package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.concurrent.AsyncRunnable;
import java.util.List;

public interface MetricsAggregator extends AsyncRunnable {

	List<AllMetricsSnapshot> metricsSnapshotsByIndex(final int originIndex);
}
