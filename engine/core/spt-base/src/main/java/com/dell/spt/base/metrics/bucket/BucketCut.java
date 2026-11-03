package com.dell.spt.base.metrics.bucket;

import org.HdrHistogram.Histogram;

/** One meter set's readings at one instant, held between a window's start and end passes. */
final class BucketCut {

	final long timeNanos;
	final long succeeded;
	final long failed;
	final long corrupt;
	final long bytes;
	final Histogram duration;
	final Histogram latency;
	final Histogram timeToFirstByte;

	BucketCut(
					final long timeNanos,
					final long succeeded,
					final long failed,
					final long corrupt,
					final long bytes,
					final Histogram duration,
					final Histogram latency,
					final Histogram timeToFirstByte) {
		this.timeNanos = timeNanos;
		this.succeeded = succeeded;
		this.failed = failed;
		this.corrupt = corrupt;
		this.bytes = bytes;
		this.duration = duration;
		this.latency = latency;
		this.timeToFirstByte = timeToFirstByte;
	}
}
