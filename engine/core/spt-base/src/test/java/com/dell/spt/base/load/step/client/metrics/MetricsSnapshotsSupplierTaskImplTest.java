package com.dell.spt.base.load.step.client.metrics;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MetricsSnapshotsSupplierTaskImplTest {

	@Test
	void fetchFailureInvalidatesPreviouslyCachedSnapshots() throws Exception {
		final LoadStep step = mock(LoadStep.class);
		final List<AllMetricsSnapshot> fresh = new ArrayList<>();
		fresh.add(mock(AllMetricsSnapshot.class));
		doReturn(fresh)
						.doThrow(new IllegalStateException("worker unavailable"))
						.when(step).metricsSnapshots();
		final var supplier = new MetricsSnapshotsSupplierTaskImpl(
						ServiceTaskExecutor.TASK_EXECUTOR, step);

		try {
			assertSame(fresh, supplier.get());
			assertNull(supplier.get(), "a failed refresh must not publish stale snapshots");
		} finally {
			supplier.close();
		}
	}
}
