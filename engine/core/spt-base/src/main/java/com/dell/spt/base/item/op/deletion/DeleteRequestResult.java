package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.op.Operation.Status;
import java.util.List;

/** Immutable request and object reconciliation snapshot. */
public record DeleteRequestResult(
			DeleteRequestOutcome outcome,
			DeleteFailureClassification failureClassification,
			Status operationStatus,
			List<DeleteTargetResult> targetResults,
			String errorMessage) {

	public DeleteRequestResult {
		targetResults = List.copyOf(targetResults);
	}

	/** Returns the number of targets accepted by the storage service. */
	public long acceptedObjectCount() {
		return targetResults.stream()
						.filter(result -> result.outcome() == DeleteTargetOutcome.ACCEPTED)
						.count();
	}

	/** Returns the number of operationally or conservatively failed targets. */
	public long failedObjectCount() {
		return targetResults.size() - acceptedObjectCount();
	}
}
