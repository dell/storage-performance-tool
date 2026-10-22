package com.dell.spt.base.storage.driver.bucket;

import com.dell.spt.base.storage.Credential;

/**
 * Bucket control-plane requests for target-bucket provisioning.
 *
 * <p>Each request is bounded by the driver's timeout and interruptible. A response classifies the
 * outcome; only a recognized not-found response may report {@link TargetBucketResponse.Kind#NOT_FOUND}.
 * Authorization, redirect, transport, timeout, malformed and service failures report
 * {@link TargetBucketResponse.Kind#FAILED}. The control client never mutates a bucket beyond
 * creating it.
 */
public interface TargetBucketControl extends AutoCloseable {

	/**
	 * Checks that the bucket exists and is accessible.
	 *
	 * @param credential the per-path credential for the bucket, or null for the driver's default
	 */
	TargetBucketResponse check(String bucket, Credential credential) throws InterruptedException;

	/**
	 * Creates the bucket.
	 *
	 * @param credential the per-path credential for the bucket, or null for the driver's default
	 */
	TargetBucketResponse create(String bucket, Credential credential) throws InterruptedException;

	@Override
	void close();
}
