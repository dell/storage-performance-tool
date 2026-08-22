package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.storage.Credential;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** One immutable standalone DELETE transport request. */
public record DeleteRequest(String bucket, Credential credential, List<DeleteTarget> targets) {

	/** S3 protocol target limit for one multi-object DELETE request. */
	public static final int MAX_TARGET_COUNT = 1000;

	public DeleteRequest {
		if (bucket == null || bucket.isEmpty()) {
			throw new IllegalArgumentException("DELETE request bucket must not be empty");
		}
		Objects.requireNonNull(credential, "DELETE request credential");
		targets = List.copyOf(targets);
		if (targets.isEmpty() || targets.size() > MAX_TARGET_COUNT) {
			throw new IllegalArgumentException(
						"DELETE request target count must be between 1 and " + MAX_TARGET_COUNT);
		}
		final var identities = new HashSet<DeleteTargetIdentity>(targets.size());
		for (final var target : targets) {
			if (!bucket.equals(target.bucket())) {
				throw new IllegalArgumentException("Every DELETE request target must use bucket " + bucket);
			}
			if (!identities.add(target.identity())) {
				throw new IllegalArgumentException(
								"Duplicate DELETE target identity: " + target.key());
			}
		}
	}
}
