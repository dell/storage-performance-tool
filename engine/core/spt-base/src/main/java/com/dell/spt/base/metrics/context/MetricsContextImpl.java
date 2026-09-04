package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
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
import java.util.function.Supplier;

import static com.dell.spt.base.metrics.MetricsConstants.METADATA_COMMENT;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_ITEM_DATA_SIZE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_CONC;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_OP_TYPE;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_RUN_ID;
import static com.dell.spt.base.metrics.MetricsConstants.METADATA_STEP_ID;

public class MetricsContextImpl<S extends AllMetricsSnapshotImpl> extends MetricsContextBase<S>
				implements MetricsContext<S> {

	private final LongMeter<TimingMetricSnapshot> reqDuration, respLatency, timeToFirstByte;
	private final LongMeter<ConcurrencyMetricSnapshot> actualConcurrency;
	private final RateMeter<RateMetricSnapshot> throughputSuccess, throughputFail, throughputCorrupt, reqBytes;
	private volatile TimingMetricSnapshot reqDurSnapshot, respLatSnapshot, ttfbSnapshot;
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
		timeToFirstByte = new TimingMeterImpl(MetricsConstants.METRIC_NAME_TTFB);
		ttfbSnapshot = timeToFirstByte.snapshot();
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
		throughputCorrupt = new RateMeterImpl(clock, MetricsConstants.METRIC_NAME_CORRUPT);
		//
		reqBytes = new RateMeterImpl(clock, MetricsConstants.METRIC_NAME_BYTE);
	}

	@Override
	public final void start() {
		super.start();
		throughputSuccess.resetStartTime();
		throughputFail.resetStartTime();
		throughputCorrupt.resetStartTime();
		reqBytes.resetStartTime();
	}

	@Override
	public final void markSucc(final long bytes, final long duration, final long latency) {
		markSucc(bytes, duration, latency, 0L);
	}

	@Override
	public final void markSucc(final long bytes, final long duration, final long latency, final long ttfb) {
		throughputSuccess.update(1);
		reqBytes.update(bytes);
		updateTimings(latency, duration, ttfb);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markSucc(bytes, duration, latency, ttfb);
		}
	}

	@Override
	public final void markPartSucc(final long bytes, final long duration, final long latency) {
		markPartSucc(bytes, duration, latency, 0L);
	}

	@Override
	public final void markPartSucc(final long bytes, final long duration, final long latency, final long ttfb) {
		reqBytes.update(bytes);
		updateTimings(latency, duration, ttfb);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markPartSucc(bytes, duration, latency, ttfb);
		}
	}

	@Override
	public final void markSucc(
					final long count, final long bytes, final long durationValues[], final long latencyValues[]) {
		markSucc(count, bytes, durationValues, latencyValues, new long[0]);
	}

	@Override
	public final void markSucc(
					final long count,
					final long bytes,
					final long durationValues[],
					final long latencyValues[],
					final long ttfbValues[]) {
		throughputSuccess.update(count);
		reqBytes.update(bytes);
		final var timingsLen = Math.min(durationValues.length, latencyValues.length);
		long duration, latency;
		for (var i = 0; i < timingsLen; ++i) {
			duration = durationValues[i];
			latency = latencyValues[i];
			updateTimings(latency, duration, i < ttfbValues.length ? ttfbValues[i] : 0L);
		}
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markSucc(count, bytes, durationValues, latencyValues, ttfbValues);
		}
	}

	@Override
	public final void markPartSucc(
					final long bytes, final long durationValues[], final long latencyValues[]) {
		markPartSucc(bytes, durationValues, latencyValues, new long[0]);
	}

	@Override
	public final void markPartSucc(
					final long bytes,
					final long durationValues[],
					final long latencyValues[],
					final long ttfbValues[]) {
		reqBytes.update(bytes);
		final var timingsLen = Math.min(durationValues.length, latencyValues.length);
		long duration, latency;
		for (var i = 0; i < timingsLen; ++i) {
			duration = durationValues[i];
			latency = latencyValues[i];
			updateTimings(latency, duration, i < ttfbValues.length ? ttfbValues[i] : 0L);
		}
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markPartSucc(bytes, durationValues, latencyValues, ttfbValues);
		}
	}

	private void updateTimings(final long latencyMicros, final long durationMicros, final long ttfbMicros) {
		if (durationMicros > 0) {
			reqDuration.update(durationMicros);
			if (latencyMicros > 0) {
				respLatency.update(latencyMicros);
			}
			if (ttfbMicros > 0 && ttfbMicros <= durationMicros) {
				timeToFirstByte.update(ttfbMicros);
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
	public final void markFail(final long duration, final long latency) {
		throughputFail.update(1);
		updateTimings(latency, duration, 0);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markFail(duration, latency);
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
	public final void markCorrupt() {
		throughputFail.update(1);
		throughputCorrupt.update(1);
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.markCorrupt();
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
		refreshLastSnapshot(false);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void refreshLastSnapshot(final boolean force) {
		final var currentTimeMillis = System.currentTimeMillis();
		if (force || currentTimeMillis - lastSnapshotsUpdateTs > DEFAULT_SNAPSHOT_UPDATE_PERIOD_MILLIS) {
			lastSnapshotsUpdateTs = currentTimeMillis;
			updateTimingSnapshots();
			actualConcurrency.update(actualConcurrencyGauge.getAsInt());
			actualConcurrencySnapshot = actualConcurrency.snapshot();
		}
		lastSnapshot = (S) new AllMetricsSnapshotImpl(
						reqDurSnapshot,
						respLatSnapshot,
						ttfbSnapshot,
						actualConcurrencySnapshot,
						throughputFail.snapshot(),
						throughputCorrupt.snapshot(),
						throughputSuccess.snapshot(),
						reqBytes.snapshot(),
						elapsedTimeMillis(),
						deleteMetricsSnapshot());
		super.refreshLastSnapshot(force);
	}

	@SuppressWarnings("unchecked")
	private DeleteMetricsSnapshot deleteMetricsSnapshot() {
		final Object value = metadata.get(MetricsConstants.METADATA_DELETE_METRICS);
		if (value instanceof Supplier<?> supplier) {
			final Object snapshot = supplier.get();
			return snapshot instanceof DeleteMetricsSnapshot deleteSnapshot ? deleteSnapshot : null;
		}
		return value instanceof DeleteMetricsSnapshot deleteSnapshot ? deleteSnapshot : null;
	}

	private void updateTimingSnapshots() {
		reqDurSnapshot = reqDuration.snapshot();
		respLatSnapshot = respLatency.snapshot();
		ttfbSnapshot = timeToFirstByte.snapshot();
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
