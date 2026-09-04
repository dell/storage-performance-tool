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
			long fullSuccessfulRequests,
			long preValidationFailed,
			long verificationCorrectnessFailed,
			long verificationInconclusive,
			boolean reconciled) implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/** Compatibility constructor retained for extension code compiled against the lifecycle v1 shape. */
	public DeleteObjectLifecycleSnapshot(
				final long selected,
				final long attempted,
				final long accepted,
				final long failed,
				final long unattempted,
				final long unresolved,
				final long protocolFailed,
				final long fullSuccessfulRequests,
				final boolean reconciled) {
		this(
				selected, attempted, accepted, failed, unattempted, unresolved, protocolFailed,
				fullSuccessfulRequests, 0, 0, 0, reconciled);
	}

	/** Compatibility constructor retained for the original eight-counter shape. */
	public DeleteObjectLifecycleSnapshot(
			final long selected,
			final long attempted,
			final long accepted,
			final long failed,
			final long unattempted,
			final long unresolved,
			final long protocolFailed,
			final boolean reconciled) {
		this(selected, attempted, accepted, failed, unattempted, unresolved, protocolFailed, 0, reconciled);
	}

	/** Returns the reconciled zero-work compatibility snapshot. */
	public static DeleteObjectLifecycleSnapshot empty() {
		return new DeleteObjectLifecycleSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true);
	}
}
