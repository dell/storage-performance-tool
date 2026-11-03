package com.dell.spt.base.metrics.bucket;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A worker's terminal bucket data, which it holds until its step service closes. */
public final class BucketMetricsReply implements Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	private final String stepId;
	private final String instanceId;
	private final ArrayList<BucketContextReply> contexts;

	public BucketMetricsReply(final String stepId, final String instanceId, final List<BucketContextReply> contexts) {
		this.stepId = stepId;
		this.instanceId = instanceId;
		this.contexts = new ArrayList<>(contexts);
	}

	public String stepId() {
		return stepId;
	}

	/** Differs for each worker's step, so that an entry node can tell a duplicate reply. */
	public String instanceId() {
		return instanceId;
	}

	public List<BucketContextReply> contexts() {
		return List.copyOf(contexts);
	}

	/** The part of this reply within a range. */
	public BucketMetricsReply within(final BucketMetricsRange range) {
		final List<BucketContextReply> selected = new ArrayList<>();
		for (final BucketContextReply context : contexts) {
			if (range.covers(context.opType())) {
				selected.add(context.within(range.fromSlot(), range.toSlot()));
			}
		}
		return new BucketMetricsReply(stepId, instanceId, selected);
	}
}
