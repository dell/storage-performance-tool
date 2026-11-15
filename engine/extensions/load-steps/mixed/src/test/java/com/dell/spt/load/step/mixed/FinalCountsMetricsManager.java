package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Keeps the final counts of a step's local metrics contexts, one per operation type of a mixed
 * step: the retained terminal entry is one per step, and the client's aggregate repeats the local
 * counts.
 */
final class FinalCountsMetricsManager extends MetricsManagerImpl {

	/** One operation type's successful and failed operations when the step unregistered its context. */
	record FinalCount(String stepId, OpType opType, long done) {}

	private final List<FinalCount> finalCounts = new CopyOnWriteArrayList<>();

	FinalCountsMetricsManager() {
		super(ServiceTaskExecutor.VT_EXECUTOR);
	}

	@Override
	public void unregister(final MetricsContext metricsCtx) {
		super.unregister(metricsCtx);
		if (metricsCtx instanceof DistributedMetricsContext) {
			return;
		}
		final AllMetricsSnapshot snapshot = metricsCtx.lastSnapshot();
		finalCounts.add(new FinalCount(
						metricsCtx.loadStepId(),
						metricsCtx.opType(),
						snapshot.successSnapshot().count() + snapshot.failsSnapshot().count()));
	}

	/** Successful and failed operations of every operation type of the step. */
	long counted(final String stepId) {
		return finalCounts.stream()
						.filter(count -> stepId.equals(count.stepId()))
						.mapToLong(FinalCount::done)
						.sum();
	}

	/** Successful and failed operations of one operation type of the step. */
	long counted(final String stepId, final OpType opType) {
		return finalCounts.stream()
						.filter(count -> stepId.equals(count.stepId()) && opType == count.opType())
						.mapToLong(FinalCount::done)
						.sum();
	}
}
