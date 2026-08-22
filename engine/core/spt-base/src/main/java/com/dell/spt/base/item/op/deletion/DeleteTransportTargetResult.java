package com.dell.spt.base.item.op.deletion;

/** Neutral per-target response emitted by a storage transport adapter. */
public record DeleteTransportTargetResult(
			String key, String versionId, boolean succeeded, String errorMessage) {

	public DeleteTransportTargetResult {
		versionId = DeleteTarget.emptyToNull(versionId);
	}

	/** Creates a successful neutral response for one target identity. */
	public static DeleteTransportTargetResult succeeded(final DeleteTarget target) {
		return new DeleteTransportTargetResult(target.key(), target.versionId(), true, null);
	}

	/** Creates an operational failure response for one target identity. */
	public static DeleteTransportTargetResult failed(
			final DeleteTarget target, final String errorMessage) {
		return new DeleteTransportTargetResult(
					target.key(), target.versionId(), false, errorMessage);
	}

	DeleteTargetIdentity identity() {
		return new DeleteTargetIdentity(key, versionId);
	}
}
