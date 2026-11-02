package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** One worker's terminal bucket data for one operation type. */
public final class BucketContextReply implements Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	private final OpType opType;
	private final ArrayList<String> members;
	private final ArrayList<BucketTerminalRecord> wholeStep;
	private final BucketSteadyOutcome steady;

	public BucketContextReply(
					final OpType opType,
					final List<String> members,
					final List<BucketTerminalRecord> wholeStep,
					final BucketSteadyOutcome steady) {
		this.opType = opType;
		this.members = new ArrayList<>(members);
		this.wholeStep = new ArrayList<>(wholeStep);
		this.steady = steady;
	}

	public OpType opType() {
		return opType;
	}

	/** The worker's Target Bucket Set, in set order. */
	public List<String> members() {
		return List.copyOf(members);
	}

	/** The whole-step records, the members in set order and then the catch-all. */
	public List<BucketTerminalRecord> wholeStep() {
		return List.copyOf(wholeStep);
	}

	BucketContextReply within(final int fromSlot, final int toSlot) {
		final int to = Math.min(toSlot, wholeStep.size());
		final int from = Math.min(fromSlot, to);
		final BucketSteadyOutcome steadyWithin = steady == null || steady.kind() != BucketSteadyOutcome.Kind.MEASURED
						? steady
						: BucketSteadyOutcome.measured(steady.records().subList(from, to));
		return new BucketContextReply(opType, members, wholeStep.subList(from, to), steadyWithin);
	}

	/** The bucket window's outcome, or null when this worker's aggregate measures no window. */
	public BucketSteadyOutcome steady() {
		return steady;
	}
}
