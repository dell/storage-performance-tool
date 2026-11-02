package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import java.util.List;
import java.util.Map;

/**
 * Finds the meter set for a result, on the per-operation result path. It routes between operation
 * types the way the step's metrics contexts do, so that a bucket row and the aggregate it sums to
 * always receive the same calls. It neither allocates nor locks.
 */
public final class BucketRecorder {

	private static final OpType[] OP_TYPES = OpType.values();

	private final BucketSlotTable table;
	private final BucketMeterSet[] primary;
	private final BucketMeterSet[][] byOpType;

	/**
	 * @param members the step's Target Bucket Set, by name
	 * @param primary the meters of the step context's primary metrics context
	 * @param byOpType the meters of its per-type metrics contexts, or null when it has none
	 */
	public BucketRecorder(
					final List<String> members, final BucketMeters primary, final Map<OpType, BucketMeters> byOpType) {
		table = new BucketSlotTable(members);
		this.primary = primary.sets();
		if (byOpType == null || byOpType.isEmpty()) {
			this.byOpType = null;
		} else {
			this.byOpType = new BucketMeterSet[OP_TYPES.length][];
			byOpType.forEach((type, meters) -> this.byOpType[type.ordinal()] = meters.sets());
		}
	}

	/** The set the operation's result belongs to, for a site that routes through its type. */
	public BucketMeterSet meterSet(final Operation<?> op) {
		return setsOf(op.type())[table.slotOfPath(objectPath(op))];
	}

	/** The set of a result whose bucket is known by its path or name, not from its operation. */
	public BucketMeterSet meterSet(final OpType type, final String bucketPath) {
		return setsOf(type)[table.slotOfPath(bucketPath)];
	}

	/** The set the operation's result belongs to, for a site that updates the primary context. */
	public BucketMeterSet primaryMeterSet(final Operation<?> op) {
		return primary[table.slotOfPath(objectPath(op))];
	}

	private BucketMeterSet[] setsOf(final OpType type) {
		if (byOpType != null) {
			final BucketMeterSet[] sets = byOpType[type.ordinal()];
			if (sets != null) {
				return sets;
			}
		}
		return primary;
	}

	/** A result's object path: the destination for writes, the source for everything else. */
	private static String objectPath(final Operation<?> op) {
		final OpType type = op.type();
		final boolean writes = OpType.CREATE.equals(type) || OpType.UPDATE.equals(type);
		final String path = writes ? op.dstPath() : op.srcPath();
		if (path != null) {
			return path;
		}
		return writes ? op.srcPath() : op.dstPath();
	}
}
