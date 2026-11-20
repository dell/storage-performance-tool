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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

public final class MetricsSnapshotsSupplierTaskImpl extends TaskBase
				implements MetricsSnapshotsSupplierTask {

	private final LoadStep loadStep;
	/** The entry's record of its lost slices; null when no fetch failure loses the slice. */
	private final MetricsAggregatorImpl.SliceLoss sliceLoss;
	private volatile List<? extends AllMetricsSnapshot> snapshotsByOrigin = null;
	private volatile boolean failedBeforeFlag = false;
	/** Set once the slice is lost, by a failed fetch or by any other route: no fetch follows. */
	private volatile boolean lost = false;
	/**
	 * Set by the first failed fetch that does not lose the slice, which alone logs at INFO; every
	 * later one logs at DEBUG, also after fetches have succeeded again.
	 */
	private final AtomicBoolean failureLogged = new AtomicBoolean();
	private int AGGREGATION_PERIOD_MILLIS;

	public MetricsSnapshotsSupplierTaskImpl(final LoadStep loadStep, Config metricsConfig) {
		this(loadStep, metricsConfig, null);
	}

	/**
	 * @param sliceLoss the entry's record of its lost slices, which tells this supplier when the
	 *     slice is lost and hears of each fetch that fails in RMI; null when no fetch failure loses it
	 */
	MetricsSnapshotsSupplierTaskImpl(
					final LoadStep loadStep, final Config metricsConfig, final MetricsAggregatorImpl.SliceLoss sliceLoss) {
		this(ServiceTaskExecutor.TASK_EXECUTOR, loadStep, sliceLoss);
		AGGREGATION_PERIOD_MILLIS = metricsConfig.intVal("average-aggregation-period");
	}

	public MetricsSnapshotsSupplierTaskImpl(final ThreadTaskExecutor executor, final LoadStep loadStep) {
		this(executor, loadStep, null);
	}

	private MetricsSnapshotsSupplierTaskImpl(
					final ThreadTaskExecutor executor, final LoadStep loadStep, final MetricsAggregatorImpl.SliceLoss sliceLoss) {
		super(executor);
		this.loadStep = loadStep;
		this.sliceLoss = sliceLoss;
	}

	/** Preserves the constructor descriptor used by existing extensions. */
	public MetricsSnapshotsSupplierTaskImpl(final VirtualThreadExecutor executor, final LoadStep loadStep) {
		this((ThreadTaskExecutor) executor, loadStep);
	}

	@Override
	protected final void doWork() throws Exception {
		if (isLost()) {
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
		if (isLost()) {
			snapshotsByOrigin = null;
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

	/** Whether the slice is lost, by a fetch of this supplier or by the entry's other calls to it. */
	private boolean isLost() {
		if (!lost && sliceLoss != null && sliceLoss.isLost(loadStep)) {
			lost = true;
		}
		return lost;
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
		if (lost || (e instanceof RemoteException remote && sliceLoss != null && sliceLoss.fetchFailed(loadStep, remote))) {
			lost = true;
			return true;
		}
		// Atomic, as the background fetch and a synchronous get() can fail together.
		if (failureLogged.compareAndSet(false, true)) {
			LogUtil.exception(Level.INFO, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
		} else {
			LogUtil.exception(Level.DEBUG, e, "Failed to fetch the metrics snapshots from \"{}\"", loadStep);
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
