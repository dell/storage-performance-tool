package com.dell.spt.base.load.lifecycle;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.List;

/**
 * Immutable operation-lifecycle counters and recoverable terminal identities.
 *
 * @param generatorBuffered operations currently owned by the generator
 * @param driverQueued operations admitted to a driver queue but not dispatched
 * @param dispatched total operations which crossed actual dispatch
 * @param inFlight dispatched operations still executing or publishing a result
 * @param terminal total retained terminal results
 * @param unattempted total recovered operations which never reached dispatch
 * @param unresolved total dispatched operations which outlived the bounded drain
 * @param unattemptedOperations identities recovered before dispatch
 * @param unresolvedOperations identities unresolved after the drain bound
 */
public record OperationLifecycleSnapshot<O extends Operation<? extends Item>>(
				long generatorBuffered,
				long driverQueued,
				long dispatched,
				long inFlight,
				long terminal,
				long unattempted,
				long unresolved,
				List<O> unattemptedOperations,
				List<O> unresolvedOperations) {

	/** Defensively copies identity lists supplied by lifecycle owners. */
	public OperationLifecycleSnapshot {
		unattemptedOperations = List.copyOf(unattemptedOperations);
		unresolvedOperations = List.copyOf(unresolvedOperations);
	}
}
