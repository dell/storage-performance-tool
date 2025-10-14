package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.fiber4j.Fiber;
import java.util.List;
import java.util.function.Supplier;

public interface MetricsSnapshotsSupplierTask
				extends Supplier<List<? extends AllMetricsSnapshot>>, Fiber {

	@Override
	List<? extends AllMetricsSnapshot> get();
}
