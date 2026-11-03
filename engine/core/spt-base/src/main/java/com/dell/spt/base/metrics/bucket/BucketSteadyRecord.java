package com.dell.spt.base.metrics.bucket;

import java.io.Serial;
import java.io.Serializable;

/**
 * The values of one bucket and operation type over its own window, between its start and end cuts.
 * That interval is shifted later than the aggregate's by the two delays: it is not nested in it.
 */
public final class BucketSteadyRecord implements Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	private final BucketTerminalRecord values;
	private final long windowNanos;
	private final long startDelayNanos;
	private final long endDelayNanos;
	private final long passNanos;

	BucketSteadyRecord(
					final BucketTerminalRecord values,
					final long windowNanos,
					final long startDelayNanos,
					final long endDelayNanos,
					final long passNanos) {
		this.values = values;
		this.windowNanos = windowNanos;
		this.startDelayNanos = startDelayNanos;
		this.endDelayNanos = endDelayNanos;
		this.passNanos = passNanos;
	}

	public BucketTerminalRecord values() {
		return values;
	}

	/** The length of this record's own window. */
	public long windowNanos() {
		return windowNanos;
	}

	/** This record's start cut after the aggregate's. */
	public long startDelayNanos() {
		return startDelayNanos;
	}

	/** This record's end cut after the aggregate's, queueing included. */
	public long endDelayNanos() {
		return endDelayNanos;
	}

	/** The cut thread's time for this record's end pass. */
	public long passNanos() {
		return passNanos;
	}
}
