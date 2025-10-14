package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.fiber4j.ExclusiveFiberBase;
import com.github.akurilov.fiber4j.FibersExecutor;

import java.util.List;

import org.apache.logging.log4j.Level;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

public final class MetricsSnapshotsSupplierTaskImpl extends ExclusiveFiberBase
				implements MetricsSnapshotsSupplierTask {

	private final LoadStep loadStep;
	private volatile List<? extends AllMetricsSnapshot> snapshotsByOrigin = null;
	private volatile boolean failedBeforeFlag = false;
	private long lastCalledMillis = 0;
	private int AGGREGATION_PERIOD_MILLIS;

	public MetricsSnapshotsSupplierTaskImpl(final LoadStep loadStep, Config metricsConfig) {
		this(ServiceTaskExecutor.INSTANCE, loadStep);
		AGGREGATION_PERIOD_MILLIS = metricsConfig.intVal("average-aggregation-period");
	}

	public MetricsSnapshotsSupplierTaskImpl(final FibersExecutor executor, final LoadStep loadStep) {
		super(executor);
		this.loadStep = loadStep;
	}

	// as MetricsSnapshotsSupplierTaskImpl is running as a separate fiber this method is called as often as it can be.
	// Effectively leading to continuous metrics flood to entry node. Though we need fresh results for thresholds, but
	// 10 times a second is enough
	@Override
	protected final void invokeTimedExclusively(final long startTimeNanos) {
		try {
			final long nextSnapshotUpdateTs = System.currentTimeMillis();
			if (nextSnapshotUpdateTs - lastCalledMillis >= AGGREGATION_PERIOD_MILLIS) {
				snapshotsByOrigin = loadStep.metricsSnapshots();
				lastCalledMillis = nextSnapshotUpdateTs;
			}

		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.INFO, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
			if (failedBeforeFlag) {
				LogUtil.exception(
								Level.WARN, e, "Failed to fetch the metrics snapshots from \"{}\" twice, stopping", loadStep);
				stop();
			} else {
				failedBeforeFlag = true;
			}
		}
	}

	@Override
	public final List<? extends AllMetricsSnapshot> get() {
		try {
			snapshotsByOrigin = loadStep.metricsSnapshots();
			lastCalledMillis = System.currentTimeMillis();
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.INFO, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
		}
		return snapshotsByOrigin;
	}

	@Override
	protected final void doClose() {
		if (null != snapshotsByOrigin) {
			snapshotsByOrigin.clear();
		}
	}
}
