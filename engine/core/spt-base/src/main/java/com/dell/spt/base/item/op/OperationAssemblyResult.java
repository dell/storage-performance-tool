package com.dell.spt.base.item.op;

/**
 * Cardinalities produced by assembling a batch of input identities.
 *
 * @param consumedIdentityCount number of input identities accepted by the assembler
 * @param emittedOperationCount number of logical operations appended to the supplied output
 *                              buffer
 */
public record OperationAssemblyResult(int consumedIdentityCount, int emittedOperationCount) {

	public OperationAssemblyResult {
		if (consumedIdentityCount < 0) {
			throw new IllegalArgumentException("Consumed identity count must not be negative");
		}
		if (emittedOperationCount < 0) {
			throw new IllegalArgumentException("Emitted operation count must not be negative");
		}
	}
}
