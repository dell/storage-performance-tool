package com.dell.spt.base.integrity;

/** Result and worker-time telemetry for one full logical-object digest pass. */
public record DigestResult(IntegrityMetadata metadata, long workerNanos) {

	public DigestResult {
		if (metadata == null) {
			throw new IllegalArgumentException("metadata must not be null");
		}
		if (workerNanos < 0) {
			throw new IllegalArgumentException("workerNanos must be nonnegative");
		}
	}
}
