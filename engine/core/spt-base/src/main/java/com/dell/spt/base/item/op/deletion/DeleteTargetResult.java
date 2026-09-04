package com.dell.spt.base.item.op.deletion;

/** Ordered reconciliation result for one requested identity. */
public record DeleteTargetResult(
			DeleteTarget target,
			DeleteTargetOutcome outcome,
			DeleteFailureClassification failureClassification,
			String errorMessage) {}
