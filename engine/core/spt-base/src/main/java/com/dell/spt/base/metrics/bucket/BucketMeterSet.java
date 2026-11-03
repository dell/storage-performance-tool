package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

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
	private final BucketTimingMeter duration = new BucketTimingMeter(MetricsConstants.METRIC_NAME_DUR);
	private final BucketTimingMeter latency = new BucketTimingMeter(MetricsConstants.METRIC_NAME_LAT);
	private final BucketTimingMeter timeToFirstByte = new BucketTimingMeter(MetricsConstants.METRIC_NAME_TTFB);

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

	/** Copies this set's readings at this instant. */
	BucketCut cut(final LongSupplier clock) {
		final long time = clock.getAsLong();
		return new BucketCut(
						time,
						succeeded.sum(),
						failed.sum(),
						corrupt.sum(),
						bytes.sum(),
						duration.histogramCopy(),
						latency.histogramCopy(),
						timeToFirstByte.histogramCopy());
	}

	/**
	 * The values between two cuts of this set, with each timing's population encoded so that the
	 * record stays mergeable once the cuts are released. The end cut is differenced in place, so
	 * the pass holds no third copy.
	 */
	BucketTerminalRecord windowRecord(
					final OpType opType,
					final BucketKind kind,
					final String bucket,
					final BucketCut start,
					final BucketCut end,
					final IntFunction<ByteBuffer> encodeBuffer) {
		return new BucketTerminalRecord(
						opType,
						kind,
						bucket,
						end.succeeded - start.succeeded,
						end.failed - start.failed,
						end.corrupt - start.corrupt,
						end.bytes - start.bytes,
						TimingMetricSnapshotImpl.ofWindow(
										MetricsConstants.METRIC_NAME_DUR, end.duration, start.duration, encodeBuffer),
						TimingMetricSnapshotImpl.ofWindow(
										MetricsConstants.METRIC_NAME_LAT, end.latency, start.latency, encodeBuffer),
						TimingMetricSnapshotImpl.ofWindow(
										MetricsConstants.METRIC_NAME_TTFB, end.timeToFirstByte, start.timeToFirstByte, encodeBuffer));
	}

	BucketTerminalRecord terminalRecord(final OpType opType, final BucketKind kind, final String bucket) {
		return terminalRecord(opType, kind, bucket, capacity -> ByteBuffer.allocate(capacity));
	}

	BucketTerminalRecord terminalRecord(
					final OpType opType,
					final BucketKind kind,
					final String bucket,
					final IntFunction<ByteBuffer> encodeBuffer) {
		return new BucketTerminalRecord(
						opType,
						kind,
						bucket,
						succeeded.sum(),
						failed.sum(),
						corrupt.sum(),
						bytes.sum(),
						duration.snapshot(encodeBuffer),
						latency.snapshot(encodeBuffer),
						timeToFirstByte.snapshot(encodeBuffer));
	}
}
