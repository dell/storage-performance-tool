package com.dell.spt.base.item.op.deletion;

/**
 * Immutable object-identity accounting for one standalone DELETE step.
 *
 * <p>{@code selected = accepted + failed + unattempted + unresolved} and {@code attempted
 * = accepted + failed + unresolved} must both hold once the step reaches its bounded terminal
 * state. {@link #reconciled()} exposes that invariant explicitly so callers fail closed instead
 * of treating incomplete accounting as a successful run.
 */
public record DeleteObjectLifecycleSnapshot(
			long selected,
			long attempted,
			long accepted,
			long failed,
			long unattempted,
			long unresolved,
			long protocolFailed,
			boolean reconciled) {

	/** Returns the reconciled zero-work compatibility snapshot. */
	public static DeleteObjectLifecycleSnapshot empty() {
		return new DeleteObjectLifecycleSnapshot(0, 0, 0, 0, 0, 0, 0, true);
	}
}
