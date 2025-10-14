package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.DistributedMetricsListener;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static com.dell.spt.base.metrics.MetricsConstants.METADATA_COMMENT;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_ITEM_DATA_SIZE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_CONC;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_NODE_LIST;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_OP_TYPE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_RUN_ID;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_STEP_ID;

public class DistributedMetricsContextImpl<S extends DistributedAllMetricsSnapshotImpl>
				extends MetricsContextBase<S> implements DistributedMetricsContext<S> {

	private final IntSupplier nodeCountSupplier;
	private final Supplier<List<AllMetricsSnapshot>> snapshotsSupplier;
	private final boolean avgPersistFlag;
	private final boolean sumPersistFlag;
	private final boolean timingPersistFlag;
	private volatile DistributedMetricsListener metricsListener = null;
	private final List<Double> quantileValues;

	public DistributedMetricsContextImpl(
					final Map metaData,
					final IntSupplier nodeCountSupplier,
					final int concurrencyThreshold,
					final int updateIntervalSec,
					final boolean stdOutColorFlag,
					final boolean avgPersistFlag,
					final boolean sumPersistFlag,
					final boolean timingPersistFlag,
					final Supplier<List<AllMetricsSnapshot>> snapshotsSupplier,
					final List<Double> quantileValues) {
		super(
						metaData,
						concurrencyThreshold,
						stdOutColorFlag,
						TimeUnit.SECONDS.toMillis(updateIntervalSec));
		this.nodeCountSupplier = nodeCountSupplier;
		this.snapshotsSupplier = snapshotsSupplier;
		this.avgPersistFlag = avgPersistFlag;
		this.sumPersistFlag = sumPersistFlag;
		this.timingPersistFlag = timingPersistFlag;
		this.quantileValues = quantileValues;
	}

	@Override
	public void markSucc(final long bytes, final long duration, final long latency) {}

	@Override
	public void markPartSucc(final long bytes, final long duration, final long latency) {}

	@Override
	public void markSucc(
					final long count,
					final long bytes,
					final long[] durationValues,
					final long[] latencyValues) {}

	@Override
	public void markPartSucc(
					final long bytes, final long[] durationValues, final long[] latencyValues) {}

	@Override
	public void markFail() {}

	@Override
	public void markFail(final long count) {}

	@Override
	public List<String> nodeAddrs() {
		return (List<String>) metadata.get(METADATA_NODE_LIST);
	}

	@Override
	public int nodeCount() {
		return nodeCountSupplier.getAsInt();
	}

	@Override
	public List<Double> quantileValues() {
		return quantileValues;
	}

	@Override
	public boolean avgPersistEnabled() {
		return avgPersistFlag;
	}

	@Override
	public boolean sumPersistEnabled() {
		return sumPersistFlag;
	}

	@Override
	public boolean timingPersistEnabled() {
		return timingPersistFlag;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void refreshLastSnapshot() {

		final var snapshots = snapshotsSupplier.get();
		final var snapshotsCount = snapshots.size();

		if (snapshotsCount > 0) { // do nothing otherwise

			final RateMetricSnapshot successSnapshot;
			final RateMetricSnapshot failsSnapshot;
			final RateMetricSnapshot bytesSnapshot;
			final ConcurrencyMetricSnapshot actualConcurrencySnapshot;
			final TimingMetricSnapshot durSnapshot;
			final TimingMetricSnapshot latSnapshot;

			if (snapshotsCount == 1) { // single

				final var snapshot = snapshots.get(0);
				successSnapshot = snapshot.successSnapshot();
				failsSnapshot = snapshot.failsSnapshot();
				bytesSnapshot = snapshot.byteSnapshot();
				actualConcurrencySnapshot = snapshot.concurrencySnapshot();
				durSnapshot = snapshot.durationSnapshot();
				latSnapshot = snapshot.latencySnapshot();

			} else { // many

				final List<TimingMetricSnapshot> durSnapshots = new ArrayList<>();
				final List<TimingMetricSnapshot> latSnapshots = new ArrayList<>();
				final List<ConcurrencyMetricSnapshot> conSnapshots = new ArrayList<>();
				final List<RateMetricSnapshot> succSnapshots = new ArrayList<>();
				final List<RateMetricSnapshot> failSnapshots = new ArrayList<>();
				final List<RateMetricSnapshot> byteSnapshots = new ArrayList<>();
				for (var i = 0; i < snapshotsCount; i++) {
					final var snapshot = snapshots.get(i);
					durSnapshots.add(snapshot.durationSnapshot());
					latSnapshots.add(snapshot.latencySnapshot());
					succSnapshots.add(snapshot.successSnapshot());
					failSnapshots.add(snapshot.failsSnapshot());
					byteSnapshots.add(snapshot.byteSnapshot());
					conSnapshots.add(snapshot.concurrencySnapshot());
				}
				successSnapshot = RateMetricSnapshotImpl.aggregate(succSnapshots);
				failsSnapshot = RateMetricSnapshotImpl.aggregate(failSnapshots);
				bytesSnapshot = RateMetricSnapshotImpl.aggregate(byteSnapshots);
				actualConcurrencySnapshot = ConcurrencyMetricSnapshotImpl.aggregate(conSnapshots);
				durSnapshot = TimingMetricSnapshotImpl.aggregate(durSnapshots);
				latSnapshot = TimingMetricSnapshotImpl.aggregate(latSnapshots);
			}

			lastSnapshot = (S) new DistributedAllMetricsSnapshotImpl(
							durSnapshot,
							latSnapshot,
							actualConcurrencySnapshot,
							failsSnapshot,
							successSnapshot,
							bytesSnapshot,
							nodeCountSupplier.getAsInt(),
							elapsedTimeMillis());
			if (metricsListener != null) {
				metricsListener.notify(lastSnapshot);
			}
			if (thresholdMetricsCtx != null) {
				thresholdMetricsCtx.refreshLastSnapshot();
			}
		}
	}

	@Override
	protected DistributedMetricsContextImpl<S> newThresholdMetricsContext() {
		return new DistributedContextBuilderImpl()
						.loadStepId(loadStepId())
						.opType(opType())
						.nodeCountSupplier(nodeCountSupplier)
						.concurrencyLimit(concurrencyLimit())
						.concurrencyThreshold(concurrencyThreshold)
						.itemDataSize(itemDataSize())
						.outputPeriodSec((int) TimeUnit.MILLISECONDS.toSeconds(outputPeriodMillis))
						.stdOutColorFlag(stdOutColorFlag)
						.avgPersistFlag(avgPersistFlag)
						.sumPersistFlag(sumPersistFlag)
						.snapshotsSupplier(snapshotsSupplier)
						.quantileValues(quantileValues)
						.nodeAddrs(nodeAddrs())
						.runId(runId())
						.build();
	}

	@Override
	public final boolean equals(final Object other) {
		if (null == other) {
			return false;
		}
		if (other instanceof MetricsContext) {
			return 0 == compareTo((MetricsContext) other);
		} else {
			return false;
		}
	}

	@Override
	public final String toString() {
		return getClass().getSimpleName()
						+ "("
						+ opType().name()
						+ '-'
						+ concurrencyLimit()
						+ "x"
						+ nodeCount()
						+ "@"
						+ loadStepId()
						+ ")";
	}

	@Override
	public final void close() {
		super.close();
	}

	public static DistributedContextBuilder builder() {
		return new DistributedContextBuilderImpl();
	}

	private static class DistributedContextBuilderImpl implements DistributedContextBuilder {

		private Map metaData = new HashMap();
		private IntSupplier nodeCountSupplier;
		private Supplier<List<AllMetricsSnapshot>> snapshotsSupplier;
		private boolean avgPersistFlag;
		private boolean sumPersistFlag;
		private boolean timingPersistFlag;
		private List<Double> quantileValues;
		private int concurrencyThreshold;
		private boolean stdOutColorFlag;
		private int outputPeriodSec;
		private IntSupplier actualConcurrencyGauge = () -> 1; // TODO: How to correctly define for distributed mode

		public DistributedMetricsContextImpl build() {
			return new DistributedMetricsContextImpl(
							metaData,
							nodeCountSupplier,
							concurrencyThreshold,
							outputPeriodSec,
							stdOutColorFlag,
							avgPersistFlag,
							sumPersistFlag,
							timingPersistFlag,
							snapshotsSupplier,
							quantileValues);
		}

		@Override
		public DistributedContextBuilder loadStepId(final String id) {
			this.metaData.put(METADATA_STEP_ID, id);
			return this;
		}

		@Override
		public DistributedContextBuilder runId(final long id) {
			this.metaData.put(METADATA_RUN_ID, id);
			return this;
		}

		@Override
		public DistributedContextBuilder comment(final String comment) {
			this.metaData.put(METADATA_COMMENT, comment);
			return this;
		}

		@Override
		public DistributedContextBuilder opType(final OpType opType) {
			this.metaData.put(METADATA_OP_TYPE, opType);
			return this;
		}

		@Override
		public DistributedContextBuilder concurrencyLimit(final int concurrencyLimit) {
			this.metaData.put(METADATA_LIMIT_CONC, concurrencyLimit);
			return this;
		}

		@Override
		public DistributedContextBuilder concurrencyThreshold(final int concurrencyThreshold) {
			this.concurrencyThreshold = concurrencyThreshold;
			return this;
		}

		@Override
		public DistributedContextBuilder itemDataSize(final SizeInBytes itemDataSize) {
			this.metaData.put(METADATA_ITEM_DATA_SIZE, itemDataSize);
			return this;
		}

		@Override
		public DistributedContextBuilder stdOutColorFlag(final boolean stdOutColorFlag) {
			this.stdOutColorFlag = stdOutColorFlag;
			return this;
		}

		@Override
		public DistributedContextBuilder outputPeriodSec(final int outputPeriodSec) {
			this.outputPeriodSec = outputPeriodSec;
			return this;
		}

		@Override
		public DistributedContextBuilder actualConcurrencyGauge(
						final IntSupplier actualConcurrencyGauge) {
			this.actualConcurrencyGauge = actualConcurrencyGauge;
			return this;
		}

		@Override
		public DistributedContextBuilder avgPersistFlag(final boolean avgPersistFlag) {
			this.avgPersistFlag = avgPersistFlag;
			return this;
		}

		@Override
		public DistributedContextBuilder sumPersistFlag(final boolean sumPersistFlag) {
			this.sumPersistFlag = sumPersistFlag;
			return this;
		}

		@Override
		public DistributedContextBuilder timingPersistFlag(final boolean timingPersistFlag) {
			this.timingPersistFlag = timingPersistFlag;
			return this;
		}

		@Override
		public DistributedContextBuilder quantileValues(final List<Double> quantileValues) {
			this.quantileValues = quantileValues;
			return this;
		}

		@Override
		public DistributedContextBuilder nodeAddrs(final List<String> nodeAddrs) {
			this.metaData.put(METADATA_NODE_LIST, nodeAddrs);
			return this;
		}

		@Override
		public DistributedContextBuilder opCountLimit(final long countLimit) {
			this.metaData.put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT, countLimit);
			return this;
		}

		@Override
		public DistributedContextBuilder timeLimitSec(final long timeLimitSec) {
			this.metaData.put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_TIME_SEC, timeLimitSec);
			return this;
		}

		@Override
		public DistributedContextBuilder nodeCountSupplier(final IntSupplier nodeCountSupplier) {
			this.nodeCountSupplier = nodeCountSupplier;
			return this;
		}

		@Override
		public DistributedContextBuilder snapshotsSupplier(
						final Supplier<List<AllMetricsSnapshot>> snapshotsSupplier) {
			this.snapshotsSupplier = snapshotsSupplier;
			return this;
		}
	}
}
