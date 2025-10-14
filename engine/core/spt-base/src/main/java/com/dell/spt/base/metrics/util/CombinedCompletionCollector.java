package com.dell.spt.base.metrics.util;

import static com.dell.spt.base.metrics.MetricsConstants.*;

import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import io.prometheus.client.Collector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Emits one aggregated completion percentage per load step (0-100 int),
 * combining all distributed contexts that share the same step id.
 */
public final class CombinedCompletionCollector extends Collector {

	private final Supplier<Set<DistributedMetricsContext>> contextsSupplier;

	public CombinedCompletionCollector(final Supplier<Set<DistributedMetricsContext>> contextsSupplier) {
		this.contextsSupplier = contextsSupplier;
	}

	@Override
	public List<MetricFamilySamples> collect() {
		final Map<String, List<DistributedMetricsContext>> byStep = new HashMap<>();
		for (DistributedMetricsContext ctx : contextsSupplier.get()) {
			byStep.computeIfAbsent(ctx.loadStepId(), k -> new ArrayList<>()).add(ctx);
		}

		final List<MetricFamilySamples> out = new ArrayList<>();
		for (Map.Entry<String, List<DistributedMetricsContext>> e : byStep.entrySet()) {
			final String stepId = e.getKey();
			final int percent = aggregatePercent(e.getValue());

			// build a single-sample family, sample name is stable
			final List<MetricFamilySamples.Sample> samples = Collections.singletonList(
							new MetricFamilySamples.Sample(
											String.format(METRIC_FORMAT, METRIC_NAME_STEP_COMPLETION) + "_value",
											// keep labels minimal and stable
											List.of(METADATA_STEP_ID, METADATA_RUN_ID),
											List.of(stepId, String.valueOf(e.getValue().get(0).runId())),
											percent));
			out.add(
							new MetricFamilySamples(
											String.format(METRIC_FORMAT, METRIC_NAME_STEP_COMPLETION + System.currentTimeMillis()),
											Type.GAUGE,
											"Aggregated completion percent for the whole load step",
											samples));
		}
		return out;
	}

	private static int aggregatePercent(final List<DistributedMetricsContext> ctxs) {
		long sumCountLimit = 0L;
		long sumCompleted = 0L;
		boolean allHaveCount = !ctxs.isEmpty();

		long maxElapsedMillis = 0L;
		long timeLimitSec = 0L; // if any > 0 use that; prefer count when all have count

		for (DistributedMetricsContext ctx : ctxs) {
			ctx.refreshLastSnapshot();
			final DistributedAllMetricsSnapshot snap = ctx.lastSnapshot();
			if (snap == null) {
				allHaveCount = false; // cannot compute by count reliably
				continue;
			}

			// count-based accumulation
			long countLimit = 0L;
			final Object cl = ctx.metadata().get(METADATA_LIMIT_OP_COUNT);
			if (cl instanceof Number) {
				countLimit = ((Number) cl).longValue();
			}
			if (countLimit <= 0) {
				allHaveCount = false;
			} else {
				sumCountLimit += countLimit;
				sumCompleted += (snap.successSnapshot().count() + snap.failsSnapshot().count());
			}

			// time-based info
			final Object tl = ctx.metadata().get(METADATA_LIMIT_TIME_SEC);
			if (tl instanceof Number && ((Number) tl).longValue() > 0) {
				timeLimitSec = Math.max(timeLimitSec, ((Number) tl).longValue());
			}
			maxElapsedMillis = Math.max(maxElapsedMillis, snap.elapsedTimeMillis());
		}

		double completion = 0.0;
		if (allHaveCount && sumCountLimit > 0) {
			completion = Math.min(1.0, Math.max(0.0, (double) sumCompleted / (double) sumCountLimit));
		} else if (timeLimitSec > 0) {
			completion = Math.min(1.0, Math.max(0.0, (maxElapsedMillis / 1000.0) / (double) timeLimitSec));
		} // else leave as 0.0 for unlimited runs

		return (int) Math.round(completion * 100.0);
	}
}
