package com.dell.spt.base.metrics.bucket;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** What one metrics context's bucket window came to. */
public final class BucketSteadyOutcome implements Serializable {

	/** The three ends of a bucket window. */
	public enum Kind {
		/** The aggregate window never started, so no cut was taken. This is not a failure. */
		NOT_REACHED,
		/** Every bucket has a record over its own window. */
		MEASURED,
		/** The aggregate window started, but a trustworthy bucket window could not be obtained. */
		UNMEASURED
	}

	@Serial
	private static final long serialVersionUID = 1L;

	private final Kind kind;
	private final String reason;
	private final ArrayList<BucketSteadyRecord> records;

	private BucketSteadyOutcome(final Kind kind, final String reason, final List<BucketSteadyRecord> records) {
		this.kind = kind;
		this.reason = reason;
		this.records = new ArrayList<>(records);
	}

	static BucketSteadyOutcome notReached() {
		return new BucketSteadyOutcome(Kind.NOT_REACHED, null, List.of());
	}

	static BucketSteadyOutcome measured(final List<BucketSteadyRecord> records) {
		return new BucketSteadyOutcome(Kind.MEASURED, null, records);
	}

	public static BucketSteadyOutcome unmeasured(final String reason) {
		return new BucketSteadyOutcome(Kind.UNMEASURED, reason, List.of());
	}

	public Kind kind() {
		return kind;
	}

	/** Why the window could not be obtained, or null. */
	public String reason() {
		return reason;
	}

	/** The members in set order and then the catch-all, when {@link Kind#MEASURED}. */
	public List<BucketSteadyRecord> records() {
		return List.copyOf(records);
	}
}
