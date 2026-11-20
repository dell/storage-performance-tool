package com.dell.spt.base.load.step.client.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;

import java.rmi.RemoteException;
import java.util.List;
import java.util.function.Predicate;

import org.apache.logging.log4j.Level;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

public final class MetricsSnapshotsSupplierTaskImpl extends TaskBase
				implements MetricsSnapshotsSupplierTask {

	private final LoadStep loadStep;
	/** Told of each failed fetch in RMI; answers whether the slice is now lost. Null: no slice is lost. */
	private final Predicate<RemoteException> lossRecorder;
	private volatile List<? extends AllMetricsSnapshot> snapshotsByOrigin = null;
	private volatile boolean failedBeforeFlag = false;
	/** Set when a failed fetch lost the slice: no fetch follows. */
	private volatile boolean lost = false;
	/** Set at the first failed fetch, which alone logs at INFO; later ones log at DEBUG. */
	private volatile boolean failureLogged = false;
	private int AGGREGATION_PERIOD_MILLIS;

	public MetricsSnapshotsSupplierTaskImpl(final LoadStep loadStep, Config metricsConfig) {
		this(loadStep, metricsConfig, null);
	}

	/**
	 * @param lossRecorder told of each fetch that fails in RMI, and answers whether that lost the
	 *     slice; null when no fetch failure loses it
	 */
	public MetricsSnapshotsSupplierTaskImpl(
					final LoadStep loadStep, final Config metricsConfig, final Predicate<RemoteException> lossRecorder) {
		this(ServiceTaskExecutor.TASK_EXECUTOR, loadStep, lossRecorder);
		AGGREGATION_PERIOD_MILLIS = metricsConfig.intVal("average-aggregation-period");
	}

	public MetricsSnapshotsSupplierTaskImpl(final ThreadTaskExecutor executor, final LoadStep loadStep) {
		this(executor, loadStep, null);
	}

	private MetricsSnapshotsSupplierTaskImpl(
					final ThreadTaskExecutor executor, final LoadStep loadStep, final Predicate<RemoteException> lossRecorder) {
		super(executor);
		this.loadStep = loadStep;
		this.lossRecorder = lossRecorder;
	}

	/** Preserves the constructor descriptor used by existing extensions. */
	public MetricsSnapshotsSupplierTaskImpl(final VirtualThreadExecutor executor, final LoadStep loadStep) {
		this((ThreadTaskExecutor) executor, loadStep);
	}

	@Override
	protected final void doWork() throws Exception {
		if (lost) {
			stop();
			return;
		}
		try {
			snapshotsByOrigin = loadStep.metricsSnapshots();
			failedBeforeFlag = false;
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			if (fetchFailed(e)) {
				stop();
				return;
			}
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
		if (lost) {
			return null;
		}
		try {
			snapshotsByOrigin = loadStep.metricsSnapshots();
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			fetchFailed(e);
		}
		return snapshotsByOrigin;
	}

	/**
	 * Handles a failed fetch: the snapshots are dropped, as before, and a lost slice gets no further
	 * fetch. Only the first failed fetch from the slice logs at INFO, since the step's metrics output
	 * fetches at each of its reports; a lost slice's own error line comes from the loss recorder.
	 *
	 * @return whether the slice is lost
	 */
	private boolean fetchFailed(final Exception e) {
		snapshotsByOrigin = null;
		if (lost || (e instanceof RemoteException remote && lossRecorder != null && lossRecorder.test(remote))) {
			lost = true;
			return true;
		}
		if (failureLogged) {
			LogUtil.exception(Level.DEBUG, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
		} else {
			failureLogged = true;
			LogUtil.exception(Level.INFO, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
		}
		return false;
	}

	@Override
	protected final void doClose() {
		if (null != snapshotsByOrigin) {
			snapshotsByOrigin.clear();
		}
	}
}
