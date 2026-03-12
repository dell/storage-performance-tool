package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.util.List;
import java.util.function.Supplier;

public interface MetricsSnapshotsSupplierTask
				extends Supplier<List<? extends AllMetricsSnapshot>>, Task {

	@Override
	List<? extends AllMetricsSnapshot> get();
}
