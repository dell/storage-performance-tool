package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.type.ConcurrencyMeterImpl;
import com.dell.spt.base.metrics.type.LongMeter;
import com.dell.spt.base.metrics.type.RateMeter;
import com.dell.spt.base.metrics.type.RateMeterImpl;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static com.dell.spt.base.metrics.MetricsConstants.METADATA_COMMENT;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_ITEM_DATA_SIZE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_CONC;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_OP_TYPE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_RUN_ID;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_STEP_ID;

public class MetricsContextImpl<S extends AllMetricsSnapshotImpl> extends MetricsContextBase<S>
				implements MetricsContext<S> {

	private final LongMeter<TimingMetricSnapshot> reqDuration, respLatency;
	private final LongMeter<ConcurrencyMetricSnapshot> actualConcurrency;
	private final RateMeter<RateMetricSnapshot> throughputSuccess, throughputFail, reqBytes;
	private volatile TimingMetricSnapshot reqDurSnapshot, respLatSnapshot;
	private volatile ConcurrencyMetricSnapshot actualConcurrencySnapshot;
	private volatile long lastSnapshotsUpdateTs = 0;
	private final IntSupplier actualConcurrencyGauge;

	public MetricsContextImpl(
					final Map<String, Object> metadata,
					final IntSupplier actualConcurrencyGauge,
					final int concurrencyThreshold,
					final int updateIntervalSec,
					final boolean stdOutColorFlag) {
		super(
						metadata,
						concurrencyThreshold,
						stdOutColorFlag,
						TimeUnit.SECONDS.toMillis(updateIntervalSec));
		//
		respLatency = new TimingMeterImpl(MetricsConstants.METRIC_NAME_LAT);
		respLatSnapshot = respLatency.snapshot();
		//
		reqDuration = new TimingMeterImpl(MetricsConstants.METRIC_NAME_DUR);
		reqDurSnapshot = reqDuration.snapshot();
		//
		this.actualConcurrencyGauge = actualConcurrencyGauge;
		actualConcurrency = new ConcurrencyMeterImpl(MetricsConstants.METRIC_NAME_CONC);
		actualConcurrencySnapshot = actualConcurrency.snapshot();
		//
		final var clock = Clock.systemUTC();
		//
		throughputSuccess = new RateMeterImpl(clock, MetricsConstants.METRIC_NAME_SUCC);
		//
		throughputFail = new RateMeterImpl(clock, MetricsConstants.METRIC_NAME_FAIL);
		//
		reqBytes = new RateMeterImpl(clock, MetricsConstants.METRIC_NAME_BYTE);
	}

	@Override
	public final void start() {
		super.start();
		throughputSuccess.resetStartTime();
		throughputFail.resetStartTime();
		reqBytes.resetStartTime();
	}

	@Override
	public final void markSucc(final long bytes, final long duration, final long latency) {
		throughputSuccess.update(1);
		reqBytes.update(bytes);
		updateTimings(latency, duration);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markSucc(bytes, duration, latency);
		}
	}

	@Override
	public final void markPartSucc(final long bytes, final long duration, final long latency) {
		reqBytes.update(bytes);
		updateTimings(latency, duration);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markPartSucc(bytes, duration, latency);
		}
	}

	@Override
	public final void markSucc(
					final long count, final long bytes, final long durationValues[], final long latencyValues[]) {
		throughputSuccess.update(count);
		reqBytes.update(bytes);
		final var timingsLen = Math.min(durationValues.length, latencyValues.length);
		long duration, latency;
		for (var i = 0; i < timingsLen; ++i) {
			duration = durationValues[i];
			latency = latencyValues[i];
			updateTimings(latency, duration);
		}
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markSucc(count, bytes, durationValues, latencyValues);
		}
	}

	@Override
	public final void markPartSucc(
					final long bytes, final long durationValues[], final long latencyValues[]) {
		reqBytes.update(bytes);
		final var timingsLen = Math.min(durationValues.length, latencyValues.length);
		long duration, latency;
		for (var i = 0; i < timingsLen; ++i) {
			duration = durationValues[i];
			latency = latencyValues[i];
			updateTimings(latency, duration);
		}
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markPartSucc(bytes, durationValues, latencyValues);
		}
	}

	private void updateTimings(final long latencyMicros, final long durationMicros) {
		if (durationMicros > 0) {
			reqDuration.update(durationMicros);
			if (latencyMicros > 0) {
				respLatency.update(latencyMicros);
			}
		}
	}

	@Override
	public final void markFail() {
		throughputFail.update(1);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markFail();
		}
	}

	@Override
	public final void markFail(final long count) {
		throughputFail.update(count);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markFail(count);
		}
	}

	@Override
	public final boolean avgPersistEnabled() {
		return false;
	}

	@Override
	public final boolean sumPersistEnabled() {
		return false;
	}

	@Override
	public final boolean timingPersistEnabled() {
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void refreshLastSnapshot() {
		final var currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis - lastSnapshotsUpdateTs > DEFAULT_SNAPSHOT_UPDATE_PERIOD_MILLIS) {
			lastSnapshotsUpdateTs = currentTimeMillis;
			updateTimingSnapshots();
			actualConcurrency.update(actualConcurrencyGauge.getAsInt());
			actualConcurrencySnapshot = actualConcurrency.snapshot();
		}
		lastSnapshot = (S) new AllMetricsSnapshotImpl(
						reqDurSnapshot,
						respLatSnapshot,
						actualConcurrencySnapshot,
						throughputFail.snapshot(),
						throughputSuccess.snapshot(),
						reqBytes.snapshot(),
						elapsedTimeMillis());
		super.refreshLastSnapshot();
	}

	private void updateTimingSnapshots() {
		reqDurSnapshot = reqDuration.snapshot();
		respLatSnapshot = respLatency.snapshot();
	}

	@Override
	protected MetricsContextImpl<S> newThresholdMetricsContext() {
		return new ContextBuilderImpl()
						.loadStepId(loadStepId())
						.opType(opType())
						.actualConcurrencyGauge(actualConcurrencyGauge)
						.concurrencyLimit(concurrencyLimit())
						.concurrencyThreshold(0)
						.itemDataSize(itemDataSize())
						.outputPeriodSec((int) TimeUnit.MILLISECONDS.toSeconds(outputPeriodMillis))
						.stdOutColorFlag(stdOutColorFlag)
						.runId(runId())
						.build();
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(final Object other) {
		if (null == other) {
			return false;
		}
		if (other instanceof MetricsContextImpl) {
			return 0 == compareTo((MetricsContextImpl<S>) other);
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
						+ "x1@"
						+ loadStepId()
						+ ")";
	}

	@Override
	public final void close() {
		super.close();
	}

	public static ContextBuilder builder() {
		return new ContextBuilderImpl();
	}

	private static class ContextBuilderImpl
					implements ContextBuilder<ContextBuilder, MetricsContextImpl> {

		private IntSupplier actualConcurrencyGauge;
		private int concurrencyThreshold;
		private boolean stdOutColorFlag;
		private int outputPeriodSec;
		private Map<String, Object> metadata = new HashMap();

		@Override
		public MetricsContextImpl build() {
			return new MetricsContextImpl(
							metadata,
							actualConcurrencyGauge,
							concurrencyThreshold,
							outputPeriodSec,
							stdOutColorFlag);
		}

		@Override
		public ContextBuilderImpl loadStepId(final String id) {
			this.metadata.put(METADATA_STEP_ID, id);
			return this;
		}

		@Override
		public ContextBuilderImpl runId(final long id) {
			this.metadata.put(METADATA_RUN_ID, id);
			return this;
		}

		@Override
		public ContextBuilder comment(final String comment) {
			this.metadata.put(METADATA_COMMENT, comment);
			return this;
		}

		@Override
		public ContextBuilderImpl opType(final OpType opType) {
			this.metadata.put(METADATA_OP_TYPE, opType);
			return this;
		}

		@Override
		public ContextBuilderImpl concurrencyLimit(final int concurrencyLimit) {
			this.metadata.put(METADATA_LIMIT_CONC, concurrencyLimit);
			return this;
		}

		@Override
		public ContextBuilderImpl concurrencyThreshold(final int concurrencyThreshold) {
			this.concurrencyThreshold = concurrencyThreshold;
			return this;
		}

		@Override
		public ContextBuilderImpl itemDataSize(final SizeInBytes itemDataSize) {
			this.metadata.put(METADATA_ITEM_DATA_SIZE, itemDataSize);
			return this;
		}

		@Override
		public ContextBuilderImpl stdOutColorFlag(final boolean stdOutColorFlag) {
			this.stdOutColorFlag = stdOutColorFlag;
			return this;
		}

		@Override
		public ContextBuilderImpl outputPeriodSec(final int outputPeriodSec) {
			this.outputPeriodSec = outputPeriodSec;
			return this;
		}

		@Override
		public ContextBuilderImpl actualConcurrencyGauge(final IntSupplier actualConcurrencyGauge) {
			this.actualConcurrencyGauge = actualConcurrencyGauge;
			return this;
		}
	}
}
