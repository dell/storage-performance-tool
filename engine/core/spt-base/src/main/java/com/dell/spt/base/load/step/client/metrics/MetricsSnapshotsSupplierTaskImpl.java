package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;

import java.util.List;

import org.apache.logging.log4j.Level;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

public final class MetricsSnapshotsSupplierTaskImpl extends TaskBase
				implements MetricsSnapshotsSupplierTask {

	private final LoadStep loadStep;
	private volatile List<? extends AllMetricsSnapshot> snapshotsByOrigin = null;
	private volatile boolean failedBeforeFlag = false;
	private int AGGREGATION_PERIOD_MILLIS;

	public MetricsSnapshotsSupplierTaskImpl(final LoadStep loadStep, Config metricsConfig) {
		this(ServiceTaskExecutor.VT_EXECUTOR, loadStep);
		AGGREGATION_PERIOD_MILLIS = metricsConfig.intVal("average-aggregation-period");
	}

	public MetricsSnapshotsSupplierTaskImpl(final VirtualThreadExecutor executor, final LoadStep loadStep) {
		super(executor);
		this.loadStep = loadStep;
	}

	@Override
	protected final void doWork() throws Exception {
		try {
			snapshotsByOrigin = loadStep.metricsSnapshots();
			failedBeforeFlag = false;
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.INFO, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
			if (failedBeforeFlag) {
				LogUtil.exception(
								Level.WARN, e, "Failed to fetch the metrics snapshots from \"{}\" twice, stopping", loadStep);
				stop();
				return;
			} else {
				failedBeforeFlag = true;
			}
		}
		Thread.sleep(AGGREGATION_PERIOD_MILLIS);
	}

	@Override
	public final List<? extends AllMetricsSnapshot> get() {
		try {
			snapshotsByOrigin = loadStep.metricsSnapshots();
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
