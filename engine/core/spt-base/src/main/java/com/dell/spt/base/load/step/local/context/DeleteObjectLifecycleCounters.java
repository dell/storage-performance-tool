package com.dell.spt.base.load.step.local.context;

import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteRequestResult;
import com.dell.spt.base.item.op.deletion.DeleteTargetOutcome;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Linearizable publication point for the object outcomes of terminal DELETE requests. */
final class DeleteObjectLifecycleCounters {
	private long accepted;
	private long failed;
	private long protocolFailed;
	private long fullSuccessfulRequests;
	private long attemptedRequests;
	private long attemptedObjects;
	private long partialRequests;
	private long failedRequests;
	private long fullBatchRequests;
	private long partialBatchRequests;
	private long currentKeyTargets;
	private long exactVersionTargets;
	private final Map<String, long[]> buckets = new HashMap<>();
	private final boolean selectionMappingAvailable;
	private final Set<String> selectedBuckets;

	DeleteObjectLifecycleCounters() {
		this(Set.of());
	}

	DeleteObjectLifecycleCounters(final Set<String> selectedBuckets) {
		selectionMappingAvailable = !selectedBuckets.isEmpty();
		this.selectedBuckets = selectedBuckets.stream()
						.filter(name -> !DeleteMetricsSnapshot.OVERFLOW_BUCKET.equals(name))
						.sorted()
						.limit(DeleteMetricsSnapshot.MAX_BUCKET_METRICS)
						.collect(Collectors.toUnmodifiableSet());
	}

	synchronized void recordDispatch(final DeleteRequest request, final int configuredBatchSize) {
		attemptedRequests = Math.addExact(attemptedRequests, 1);
		attemptedObjects = Math.addExact(attemptedObjects, request.targets().size());
		if (request.targets().size() == configuredBatchSize) {
			fullBatchRequests = Math.addExact(fullBatchRequests, 1);
		} else {
			partialBatchRequests = Math.addExact(partialBatchRequests, 1);
		}
		for (final var target : request.targets()) {
			if (target.versionId() == null) {
				currentKeyTargets = Math.addExact(currentKeyTargets, 1);
			} else {
				exactVersionTargets = Math.addExact(exactVersionTargets, 1);
			}
			bucket(target.bucket())[0] = Math.addExact(bucket(target.bucket())[0], 1);
		}
	}

	synchronized void recordTerminal(final DeleteRequestResult result) {
		final long acceptedDelta = result.acceptedObjectCount();
		final long failedDelta = result.failedObjectCount();
		recordTerminal(
						acceptedDelta,
						failedDelta,
						result.failureClassification() == DeleteFailureClassification.PROTOCOL ? failedDelta : 0,
						result.outcome() == DeleteRequestOutcome.FULL_SUCCESS ? 1 : 0);
		if (result.outcome() == DeleteRequestOutcome.PARTIAL) {
			partialRequests = Math.addExact(partialRequests, 1);
		} else if (result.outcome() == DeleteRequestOutcome.FAILED) {
			failedRequests = Math.addExact(failedRequests, 1);
		}
		for (final var targetResult : result.targetResults()) {
			final long[] bucket = bucket(targetResult.target().bucket());
			final int index = targetResult.outcome() == DeleteTargetOutcome.ACCEPTED ? 1 : 2;
			bucket[index] = Math.addExact(bucket[index], 1);
		}
	}

	synchronized void recordTerminal(
					final long acceptedDelta,
					final long failedDelta,
					final long protocolFailedDelta,
					final long fullSuccessfulRequestDelta) {
		final long nextAccepted = Math.addExact(accepted, acceptedDelta);
		final long nextFailed = Math.addExact(failed, failedDelta);
		final long nextProtocolFailed = Math.addExact(protocolFailed, protocolFailedDelta);
		final long nextFullSuccessfulRequests = Math.addExact(
						fullSuccessfulRequests, fullSuccessfulRequestDelta);
		accepted = nextAccepted;
		failed = nextFailed;
		protocolFailed = nextProtocolFailed;
		fullSuccessfulRequests = nextFullSuccessfulRequests;
	}

	synchronized Snapshot snapshot() {
		final var bucketSnapshot = new HashMap<String, BucketCounters>(buckets.size());
		buckets.forEach((name, counts) -> bucketSnapshot.put(
						name, new BucketCounters(counts[0], counts[1], counts[2])));
		return new Snapshot(
						accepted,
						failed,
						protocolFailed,
						fullSuccessfulRequests,
						attemptedRequests,
						attemptedObjects,
						partialRequests,
						failedRequests,
						fullBatchRequests,
						partialBatchRequests,
						currentKeyTargets,
						exactVersionTargets,
						Map.copyOf(bucketSnapshot));
	}

	private long[] bucket(final String requestedName) {
		if (selectionMappingAvailable) {
			final String selectedName = selectedBuckets.contains(requestedName)
							? requestedName
							: DeleteMetricsSnapshot.OVERFLOW_BUCKET;
			return buckets.computeIfAbsent(selectedName, ignored -> new long[3]);
		}
		String name = requestedName;
		if (!buckets.containsKey(name)
						&& buckets.size() >= DeleteMetricsSnapshot.MAX_BUCKET_METRICS) {
			name = DeleteMetricsSnapshot.OVERFLOW_BUCKET;
		}
		return buckets.computeIfAbsent(name, ignored -> new long[3]);
	}

	record BucketCounters(long attempted, long accepted, long failed) {}

	record Snapshot(
					long accepted,
					long failed,
					long protocolFailed,
					long fullSuccessfulRequests,
					long attemptedRequests,
					long attemptedObjects,
					long partialRequests,
					long failedRequests,
					long fullBatchRequests,
					long partialBatchRequests,
					long currentKeyTargets,
					long exactVersionTargets,
					Map<String, BucketCounters> buckets) {}
}
