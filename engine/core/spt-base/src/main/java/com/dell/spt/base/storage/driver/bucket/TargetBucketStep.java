package com.dell.spt.base.storage.driver.bucket;

/**
 * What target-bucket setup must know about a step that its configuration does not state.
 *
 * <p>A mixed step inherits {@code load.op.type=create} and keeps its GET/STAT pool in
 * {@code item.input.file}, and provisioning runs before it builds its per-operation contexts, so
 * setup cannot tell from the operation type whether it writes new objects or which of its files
 * are inputs for existing objects.
 *
 * @param mixed true for a mixed step
 * @param writesNewObjects for a mixed step, whether its PUT weight is positive
 */
public record TargetBucketStep(boolean mixed, boolean writesNewObjects) {

	/** A step whose operation type tells whether it writes new objects. */
	public static final TargetBucketStep ORDINARY = new TargetBucketStep(false, false);

	/** A mixed step, which writes new objects only when its PUT weight is positive. */
	public static TargetBucketStep mixed(final boolean writesNewObjects) {
		return new TargetBucketStep(true, writesNewObjects);
	}
}
