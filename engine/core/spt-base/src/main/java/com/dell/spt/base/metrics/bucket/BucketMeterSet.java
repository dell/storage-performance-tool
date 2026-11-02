package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.util.concurrent.atomic.LongAdder;

/**
 * The meters of one bucket and operation type. It has the aggregate's method set and records what
 * the aggregate records for each call, with no EWMA, clock read, concurrency meter or last-interval
 * rate. See {@link com.dell.spt.base.metrics.context.MetricsContextImpl}.
 */
public final class BucketMeterSet {

	private final LongAdder succeeded = new LongAdder();
	private final LongAdder failed = new LongAdder();
	private final LongAdder corrupt = new LongAdder();
	private final LongAdder bytes = new LongAdder();
	private final TimingMeterImpl duration = new TimingMeterImpl(MetricsConstants.METRIC_NAME_DUR);
	private final TimingMeterImpl latency = new TimingMeterImpl(MetricsConstants.METRIC_NAME_LAT);
	private final TimingMeterImpl timeToFirstByte = new TimingMeterImpl(MetricsConstants.METRIC_NAME_TTFB);

	public void markSucc(final long byteCount, final long durationMicros, final long latencyMicros, final long ttfbMicros) {
		succeeded.increment();
		bytes.add(byteCount);
		updateTimings(latencyMicros, durationMicros, ttfbMicros);
	}

	public void markPartSucc(
					final long byteCount, final long durationMicros, final long latencyMicros, final long ttfbMicros) {
		bytes.add(byteCount);
		updateTimings(latencyMicros, durationMicros, ttfbMicros);
	}

	public void markSucc(
					final long count,
					final long byteCount,
					final long[] durationValues,
					final long[] latencyValues,
					final long[] ttfbValues) {
		succeeded.add(count);
		bytes.add(byteCount);
		final int timingsLength = Math.min(durationValues.length, latencyValues.length);
		for (int i = 0; i < timingsLength; i++) {
			updateTimings(latencyValues[i], durationValues[i], i < ttfbValues.length ? ttfbValues[i] : 0L);
		}
	}

	public void markFail() {
		failed.increment();
	}

	public void markFail(final long durationMicros, final long latencyMicros) {
		failed.increment();
		updateTimings(latencyMicros, durationMicros, 0L);
	}

	public void markCorrupt() {
		failed.increment();
		corrupt.increment();
	}

	private void updateTimings(final long latencyMicros, final long durationMicros, final long ttfbMicros) {
		TimingMeterImpl.updateRequestTimings(
						duration, latency, timeToFirstByte, latencyMicros, durationMicros, ttfbMicros);
	}

	BucketTerminalRecord terminalRecord(final OpType opType, final BucketKind kind, final String bucket) {
		return new BucketTerminalRecord(
						opType,
						kind,
						bucket,
						succeeded.sum(),
						failed.sum(),
						corrupt.sum(),
						bytes.sum(),
						duration.snapshot(),
						latency.snapshot(),
						timeToFirstByte.snapshot());
	}
}
