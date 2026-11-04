package com.dell.spt.base.metrics.bucket;

/** What a row of bucket metrics describes. A real bucket may have any valid name, so this tells them apart. */
public enum BucketKind {
	/** A member of the step's Target Bucket Set. */
	MEMBER,
	/** Every result whose bucket is not a member. */
	CATCH_ALL
}
