package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Real metrics contexts of a distributed step, for tests of its storage driver statistics. */
public final class DriverStatsFleet {

	public static final long RUN_ID = 7;
	public static final int ITEM_SIZE = 4096;

	private DriverStatsFleet() {}

	/**
	 * Creates the started metrics context of one contributor.
	 *
	 * @param stats what its driver reports when asked; null for a driver that is never asked
	 */
	public static MetricsContext<?> node(final String stepId, final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId(stepId)
						.runId(RUN_ID)
						.opType(OpType.CREATE)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(ITEM_SIZE))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		if (stats != null) {
			ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, stats);
		}
		ctx.start();
		return ctx;
	}

	/** Returns what a contributor hands over when the Entry Node asks it now. */
	public static AllMetricsSnapshot snapshotOf(final MetricsContext<?> node) {
		node.refreshLastSnapshot(true);
		return node.lastSnapshot();
	}

	/**
	 * Creates the started metrics context of a distributed step.
	 *
	 * @param expected the number of contributors the step expects: the Entry Node's own and remote ones
	 * @param contributors the snapshots handed over at each refresh; null for one that did not answer
	 */
	public static DistributedMetricsContext<?> fleet(
					final String stepId,
					final OpType opType,
					final int expected,
					final Supplier<List<AllMetricsSnapshot>> contributors) {
		final List<String> remotes = new ArrayList<>();
		for (int i = 1; i < expected; i++) {
			remotes.add("worker-" + i);
		}
		final List<String> contributorIds = new ArrayList<>(List.of(MetricsConstants.FLEET_LOCAL_CONTRIBUTOR_ID));
		contributorIds.addAll(remotes);
		final DistributedMetricsContext<?> fleet = (DistributedMetricsContext<?>) DistributedMetricsContextImpl.builder()
						.loadStepId(stepId)
						.runId(RUN_ID)
						.opType(opType)
						.nodeCountSupplier(() -> expected)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(ITEM_SIZE))
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(contributors)
						.quantileValues(List.of(0.5))
						.nodeAddrs(remotes)
						.contributorIds(contributorIds)
						.deleteDetailsExpected(opType == OpType.DELETE)
						.comment("")
						.build();
		fleet.start();
		return fleet;
	}
}
