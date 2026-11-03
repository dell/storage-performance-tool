package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import java.io.Serial;
import java.io.Serializable;

/**
 * The values of one bucket and operation type over a measured interval: the counters and, for each
 * timing, its population in mergeable form, so that an entry node merges populations and not row
 * statistics.
 */
public final class BucketTerminalRecord implements Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	private final OpType opType;
	private final BucketKind kind;
	private final String bucket;
	private final long succeeded;
	private final long failed;
	private final long corrupt;
	private final long bytes;
	private final TimingMetricSnapshot duration;
	private final TimingMetricSnapshot latency;
	private final TimingMetricSnapshot timeToFirstByte;

	public BucketTerminalRecord(
					final OpType opType,
					final BucketKind kind,
					final String bucket,
					final long succeeded,
					final long failed,
					final long corrupt,
					final long bytes,
					final TimingMetricSnapshot duration,
					final TimingMetricSnapshot latency,
					final TimingMetricSnapshot timeToFirstByte) {
		this.opType = opType;
		this.kind = kind;
		this.bucket = bucket;
		this.succeeded = succeeded;
		this.failed = failed;
		this.corrupt = corrupt;
		this.bytes = bytes;
		this.duration = duration;
		this.latency = latency;
		this.timeToFirstByte = timeToFirstByte;
	}

	public OpType opType() {
		return opType;
	}

	public BucketKind kind() {
		return kind;
	}

	/** The member's name, or null for the catch-all. */
	public String bucket() {
		return bucket;
	}

	public long succeeded() {
		return succeeded;
	}

	public long failed() {
		return failed;
	}

	public long corrupt() {
		return corrupt;
	}

	public long bytes() {
		return bytes;
	}

	public TimingMetricSnapshot duration() {
		return duration;
	}

	public TimingMetricSnapshot latency() {
		return latency;
	}

	public TimingMetricSnapshot timeToFirstByte() {
		return timeToFirstByte;
	}
}
