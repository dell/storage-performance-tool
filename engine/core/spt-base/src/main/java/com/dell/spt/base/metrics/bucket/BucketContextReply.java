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

	private final int contextIndex;
	private final OpType opType;
	private final long elapsedMillis;
	private final long aggregateStartOffsetMillis;
	private final ArrayList<String> members;
	private final ArrayList<BucketTerminalRecord> wholeStep;
	private final BucketSteadyOutcome steady;

	/**
	 * @param contextIndex the metrics context's place in its step, which an entry node's contexts share
	 * @param elapsedMillis this worker's own elapsed time for the context, as its aggregate rates use
	 * @param aggregateStartOffsetMillis when this worker's aggregate window started after step start,
	 *     or {@link com.dell.spt.base.metrics.steady.SteadyResult#NONE}
	 */
	public BucketContextReply(
					final int contextIndex,
					final OpType opType,
					final long elapsedMillis,
					final long aggregateStartOffsetMillis,
					final List<String> members,
					final List<BucketTerminalRecord> wholeStep,
					final BucketSteadyOutcome steady) {
		this.contextIndex = contextIndex;
		this.opType = opType;
		this.elapsedMillis = elapsedMillis;
		this.aggregateStartOffsetMillis = aggregateStartOffsetMillis;
		this.members = new ArrayList<>(members);
		this.wholeStep = new ArrayList<>(wholeStep);
		this.steady = steady;
	}

	public int contextIndex() {
		return contextIndex;
	}

	public OpType opType() {
		return opType;
	}

	public long elapsedMillis() {
		return elapsedMillis;
	}

	public long aggregateStartOffsetMillis() {
		return aggregateStartOffsetMillis;
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
		return new BucketContextReply(
						contextIndex,
						opType,
						elapsedMillis,
						aggregateStartOffsetMillis,
						members,
						wholeStep.subList(from, to),
						steadyWithin);
	}

	/** The bucket window's outcome, or null when this worker's aggregate measures no window. */
	public BucketSteadyOutcome steady() {
		return steady;
	}
}
