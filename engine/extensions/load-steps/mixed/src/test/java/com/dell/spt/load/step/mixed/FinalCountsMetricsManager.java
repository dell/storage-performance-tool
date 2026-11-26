package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Keeps the final counts of a step's local metrics contexts, one per operation type of a mixed
 * step: the retained terminal entry is one per step, and the client's aggregate repeats the local
 * counts.
 */
final class FinalCountsMetricsManager extends MetricsManagerImpl {

	/** One operation type's successful and failed operations when the step unregistered its context. */
	record FinalCount(String stepId, OpType opType, long done, long concurrencyLast) {}

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
						snapshot.successSnapshot().count() + snapshot.failsSnapshot().count(),
						snapshot.concurrencySnapshot().last()));
	}

	/** Successful and failed operations of every operation type of the step. */
	long counted(final String stepId) {
		return finalCounts.stream()
						.filter(count -> stepId.equals(count.stepId()))
						.mapToLong(FinalCount::done)
						.sum();
	}

	/** The final current concurrency of each operation type of the step, as its last total row reports it. */
	Map<OpType, Long> finalConcurrency(final String stepId) {
		final Map<OpType, Long> concurrency = new EnumMap<>(OpType.class);
		for (final FinalCount count : finalCounts) {
			if (stepId.equals(count.stepId())) {
				concurrency.merge(count.opType(), count.concurrencyLast(), Long::sum);
			}
		}
		return concurrency;
	}

	/** Successful and failed operations of one operation type of the step. */
	long counted(final String stepId, final OpType opType) {
		return finalCounts.stream()
						.filter(count -> stepId.equals(count.stepId()) && opType == count.opType())
						.mapToLong(FinalCount::done)
						.sum();
	}
}
