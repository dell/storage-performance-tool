package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;

import java.util.Map;

import static com.dell.spt.base.metrics.MetricsConstants.*;

public abstract class MetricsContextBase<S extends AllMetricsSnapshot>
				implements MetricsContext<S> {

	protected final Map<String, Object> metadata;
	protected final long ts;
	protected final int concurrencyThreshold;
	protected final boolean stdOutColorFlag;
	protected final long outputPeriodMillis;
	private volatile long tsStart = -1;
	private volatile long lastOutputTs = 0;
	private volatile boolean thresholdStateExitedFlag = false;
	protected volatile MetricsContextBase thresholdMetricsCtx = null;
	protected volatile S lastSnapshot = null;

	protected MetricsContextBase(
					final Map metadata,
					final int concurrencyThreshold,
					final boolean stdOutColorFlag,
					final long outputPeriodMillis) {
		ts = System.nanoTime();
		this.metadata = metadata;
		this.concurrencyThreshold = concurrencyThreshold > 0 ? concurrencyThreshold : Integer.MAX_VALUE;
		this.stdOutColorFlag = stdOutColorFlag;
		this.outputPeriodMillis = outputPeriodMillis;
	}

	@Override
	public void start() {
		tsStart = System.currentTimeMillis();
		metadata.put(METADATA_START_TIME, tsStart);
	}

	@Override
	public final boolean isStarted() {
		return tsStart > -1;
	}

	@Override
	public final long startTimeStamp() {
		return tsStart;
	}

	@Override
	public final Map metadata() {
		return metadata;
	}

	@Override
	public final String loadStepId() {
		return (String) metadata.get(METADATA_STEP_ID);
	}

	@Override
	public final long runId() {
		return (long) metadata.get(METADATA_RUN_ID);
	}

	@Override
	public final OpType opType() {
		return (OpType) metadata.get(METADATA_OP_TYPE);
	}

	@Override
	public final int concurrencyLimit() {
		return (int) metadata.get(METADATA_LIMIT_CONC);
	}

	@Override
	public final int concurrencyThreshold() {
		return concurrencyThreshold;
	}

	@Override
	public final SizeInBytes itemDataSize() {
		return (SizeInBytes) metadata.get(METADATA_ITEM_DATA_SIZE);
	}

	@Override
	public final boolean stdOutColorEnabled() {
		return stdOutColorFlag;
	}

	@Override
	public final long outputPeriodMillis() {
		return outputPeriodMillis;
	}

	@Override
	public final long lastOutputTs() {
		return lastOutputTs;
	}

	@Override
	public final void lastOutputTs(final long ts) {
		lastOutputTs = ts;
	}

	@Override
	public final S lastSnapshot() {
		if (lastSnapshot == null) {
			refreshLastSnapshot();
		}
		return lastSnapshot;
	}

	@Override
	public void refreshLastSnapshot() {
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.refreshLastSnapshot();
		}
	}

	@Override
	public void refreshLastSnapshot(final boolean force) {
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.refreshLastSnapshot(force);
		}
	}

	@Override
	public final MetricsContext thresholdMetrics() throws IllegalStateException {
		if (thresholdMetricsCtx == null) {
			throw new IllegalStateException("Nested metrics context is not exist");
		}
		return thresholdMetricsCtx;
	}

	@Override
	public final void enterThresholdState() throws IllegalStateException {
		if (thresholdMetricsCtx != null) {
			throw new IllegalStateException("Nested metrics context already exists");
		}
		thresholdMetricsCtx = newThresholdMetricsContext();
		thresholdMetricsCtx.start();
	}

	protected abstract MetricsContextBase<S> newThresholdMetricsContext();

	@Override
	public final boolean thresholdStateEntered() {
		return thresholdMetricsCtx != null && thresholdMetricsCtx.isStarted();
	}

	@Override
	public final void exitThresholdState() throws IllegalStateException {
		if (thresholdMetricsCtx == null) {
			throw new IllegalStateException("Threshold state was not entered");
		}
		thresholdMetricsCtx.close();
		thresholdStateExitedFlag = true;
	}

	@Override
	public final boolean thresholdStateExited() {
		return thresholdStateExitedFlag;
	}

	@Override
	public final int hashCode() {
		return (int) ts;
	}

	@Override
	public final int compareTo(final MetricsContext<S> other) {
		return Long.compare(hashCode(), other.hashCode());
	}

	@Override
	public void close() {
		tsStart = -1;
		lastSnapshot = null;
		if (thresholdMetricsCtx != null) {
			thresholdMetricsCtx.close();
			thresholdMetricsCtx = null;
		}
	}

	@Override
	public long elapsedTimeMillis() {
		return (System.currentTimeMillis() - tsStart);
	}

	@Override
	public String comment() {
		return (String) this.metadata.get(METADATA_COMMENT);
	}
}
