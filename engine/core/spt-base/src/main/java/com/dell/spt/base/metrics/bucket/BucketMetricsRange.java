package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import java.io.Serial;
import java.io.Serializable;

/**
 * Which of a worker's bucket records a fetch asks for. Every fetch today asks for {@link #ALL}; the
 * range lets an entry node fetch the same members of one operation type from every worker, merge and
 * release them before the next range, if whole payloads ever stop fitting its heap.
 *
 * @param opType the operation type, or null for every type
 * @param fromSlot the first slot, the members in set order and then the catch-all
 * @param toSlot one past the last slot
 */
public record BucketMetricsRange(OpType opType, int fromSlot, int toSlot) implements Serializable {

	public static final BucketMetricsRange ALL = new BucketMetricsRange(null, 0, Integer.MAX_VALUE);

	@Serial
	private static final long serialVersionUID = 1L;

	boolean covers(final OpType type) {
		return opType == null || opType == type;
	}
}
