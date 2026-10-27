package com.dell.spt.base.storage.driver.bucket;

/**
 * What target-bucket setup must know about a step that its configuration does not state.
 *
 * <p>A mixed step inherits {@code load.op.type=create} and keeps its GET/STAT pool in
 * {@code item.input.file}, and provisioning runs before it builds its per-operation contexts, so
 * setup cannot tell from the operation type whether it writes new objects or which of its files
 * are inputs for existing objects.
 *
 * <p>A pipeline step feeds every sub-step after the first with the items the previous one
 * completed, through memory rather than an item input file, so setup cannot see that input either.
 *
 * @param mixed true for a mixed step
 * @param writesNewObjects for a mixed step, whether its PUT weight is positive
 * @param pipeline true for a pipeline step, whose contexts after the first take their items from the
 *     previous context
 */
public record TargetBucketStep(boolean mixed, boolean writesNewObjects, boolean pipeline) {

	/** A step whose operation type tells whether it writes new objects. */
	public static final TargetBucketStep ORDINARY = new TargetBucketStep(false, false, false);

	/** A pipeline step: each context after the first takes its items from the previous one. */
	public static final TargetBucketStep PIPELINE = new TargetBucketStep(false, false, true);

	/** A mixed step, which writes new objects only when its PUT weight is positive. */
	public static TargetBucketStep mixed(final boolean writesNewObjects) {
		return new TargetBucketStep(true, writesNewObjects, false);
	}
}
