package com.dell.spt.base.load.step.client.metrics;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.commons.concurrent.AsyncRunnableBase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.github.akurilov.confuse.Config;

public final class MetricsAggregatorImpl extends AsyncRunnableBase implements MetricsAggregator {

	private final String loadStepId;
	private final MetricsSnapshotsSupplierTask[] snapshotSuppliers;
	private final int count;

	public MetricsAggregatorImpl(final String loadStepId, final List<LoadStep> stepSlices, final Config metricsConfig) {
		this.loadStepId = loadStepId;
		snapshotSuppliers = stepSlices
						.stream()
						.map(slice -> new MetricsSnapshotsSupplierTaskImpl(slice, metricsConfig))
						.collect(Collectors.toList())
						.toArray(new MetricsSnapshotsSupplierTask[]{});
		count = snapshotSuppliers.length;
	}

	@Override
	public final List<AllMetricsSnapshot> metricsSnapshotsByIndex(final int originIndex) {
		MetricsSnapshotsSupplierTask supplyTask;
		List<? extends AllMetricsSnapshot> snapshots;
		AllMetricsSnapshot snapshot;
		final List<AllMetricsSnapshot> snapshotsByIndex = new ArrayList<>(count);
		for (var i = 0; i < count; i++) {
			supplyTask = snapshotSuppliers[i];
			snapshots = supplyTask.get();
			if (null != snapshots) {
				if (originIndex < snapshots.size()) {
					snapshot = snapshots.get(originIndex);
					if (null != snapshot) {
						snapshotsByIndex.add(snapshot);
					}
				}
			}
		}
		return snapshotsByIndex;
	}

	@Override
	protected final void doStart() {
		Arrays.stream(snapshotSuppliers)
						.forEach(
										snapshotsSupplier -> {
											try (final var logCtx = put(KEY_STEP_ID, loadStepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
												snapshotsSupplier.start();
											}
										});
	}

	@Override
	protected final void doStop() {
		Arrays.stream(snapshotSuppliers)
						.parallel()
						.forEach(
										snapshotsSupplier -> {
											try (final var logCtx = put(KEY_STEP_ID, loadStepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
												snapshotsSupplier.stop();
											}
										});
	}

	@Override
	protected final void doClose() {
		for (var i = 0; i < count; i++) {
			try (final var logCtx = put(KEY_STEP_ID, loadStepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				snapshotSuppliers[i].close();
			}
			snapshotSuppliers[i] = null;
		}
	}
}
