package com.dell.spt.base.metrics.snapshot;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_FIXED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_SINGLE;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.deletion.DeleteVerificationSummary;
import java.util.List;
import org.junit.jupiter.api.Test;

final class DeleteMetricsSnapshotTest {

	@Test
	void defaultsUseTheSharedDeleteIdentityAndPolicyVocabulary() {
		final var snapshot = DeleteMetricsSnapshot.builder(1).build();

		assertEquals(DELETE_IDENTITY_MODE_SINGLE, snapshot.mode());
		assertEquals(DELETE_SELECTION_ORDER_CANONICAL, snapshot.selectionOrder());
		assertEquals(DELETE_FAILURE_POLICY_MODE_FIXED, snapshot.failurePolicyMode());
	}

	@Test
	void aggregatesExplicitRequestAndObjectUnitsWithoutDerivingRequestsFromObjects() {
		final var nodeA = snapshot("batch", 100, 2, 150, 1, 1, "bucket-a")
						.toBuilder()
						.failurePolicy("fixed", 100_000, 0, 30, 4, 1)
						.build();
		final var nodeB = snapshot("batch", 100, 1, 25, 1, 0, "bucket-b");

		final var aggregate = DeleteMetricsSnapshot.aggregate(List.of(nodeA, nodeB));

		assertEquals(3, aggregate.requestAttempted());
		assertEquals(2, aggregate.requestFullSuccess());
		assertEquals(1, aggregate.requestPartial());
		assertEquals(0, aggregate.requestFailed());
		assertEquals(175, aggregate.objectAttempted());
		assertEquals(170, aggregate.objectAccepted());
		assertEquals(5, aggregate.objectFailed());
		assertEquals(3, aggregate.actualRequestCount());
		assertEquals(175, aggregate.actualObjectCount());
		assertEquals(1, aggregate.fullBatchCount());
		assertEquals(2, aggregate.partialBatchCount());
		assertEquals(2, aggregate.buckets().size());
		assertEquals(4, aggregate.operationalFailedObjects());
		assertEquals(1, aggregate.excludedFailedObjects());
		assertEquals(4.0 / 174.0 * 100.0, aggregate.observedFailurePercent(), 0.000_001);
		assertTrue(aggregate.reconciled());
	}

	@Test
	void rejectsAggregationAcrossDifferentDeleteResultIdentities() {
		final var batch = snapshot("batch", 100, 1, 100, 1, 0, "bucket-a");
		final var single = snapshot("single", 1, 1, 1, 1, 0, "bucket-a");

		assertThrows(
						IllegalArgumentException.class,
						() -> DeleteMetricsSnapshot.aggregate(List.of(batch, single)));
	}

	@Test
	void rejectsAggregationAcrossDifferentDeleteFailurePolicies() {
		final var fixed = snapshot("batch", 100, 1, 100, 1, 0, "bucket-a");
		final var percentage = DeleteMetricsSnapshot.builder(100)
						.identity("batch", "canonical")
						.failurePolicy("percentage", 100_000, 1.0, 30, 0)
						.build();

		assertThrows(
						IllegalArgumentException.class,
						() -> DeleteMetricsSnapshot.aggregate(List.of(fixed, percentage)));
	}

	@Test
	void boundsBucketCardinalityWithOneDeterministicOverflowEntry() {
		final var builder = DeleteMetricsSnapshot.builder(1);
		for (int i = 0; i < 105; i++) {
			builder.bucket("bucket-" + i, 1, 1, 1, 0);
		}
		final var snapshot = builder.build();

		assertEquals(DeleteMetricsSnapshot.MAX_BUCKET_METRICS + 1, snapshot.buckets().size());
		assertEquals(DeleteMetricsSnapshot.OVERFLOW_BUCKET,
						snapshot.buckets().get(snapshot.buckets().size() - 1).bucket());
		assertEquals(5, snapshot.buckets().get(snapshot.buckets().size() - 1).selected());
	}

	@Test
	void aggregatePreservesUnavailableLivePhasesAndFailureBudgetOutcome() {
		final var live = snapshot("single", 1, 1, 1, 1, 0, "bucket-a")
						.toBuilder()
						.phases(-1, -1, -1)
						.failureOutcome("running")
						.build();

		final var aggregate = DeleteMetricsSnapshot.aggregate(List.of(live, live));

		assertEquals(-1, aggregate.scheduledDeleteNanos());
		assertEquals(-1, aggregate.drainNanos());
		assertEquals(-1, aggregate.totalWallNanos());
		assertEquals("running", aggregate.failureOutcome());
	}

	@Test
	void aggregatesTheFullVerificationClassificationMatrix() {
		final var nodeA = snapshot("single", 1, 1, 1, 1, 0, "bucket-a")
						.toBuilder()
						.verification(new DeleteVerificationSummary(
										true, true, true, true, false, 30_000, 0, 1, 0, 0,
										1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
						.build();
		final var nodeB = snapshot("single", 1, 1, 1, 1, 0, "bucket-b")
						.toBuilder()
						.verification(new DeleteVerificationSummary(
										true, true, true, true, false, 30_000, 0, 0, 1, 1,
										0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
						.build();

		final var verification = DeleteMetricsSnapshot.aggregate(List.of(nodeA, nodeB)).verification();

		assertEquals(1, verification.verifiedAbsent());
		assertEquals(1, verification.stillPresent());
		assertEquals(1, verification.unresolved());
		assertEquals(2, verification.correctnessFailures());
		assertEquals(1, verification.inconclusiveFailures());
		assertEquals(2, verification.residualCount());
	}

	private static DeleteMetricsSnapshot snapshot(
					final String mode,
					final int configuredBatchSize,
					final long requests,
					final long objects,
					final long fullSuccessRequests,
					final long partialRequests,
					final String bucket) {
		return DeleteMetricsSnapshot.builder(configuredBatchSize)
						.identity(mode, "canonical")
						.requests(requests, fullSuccessRequests, partialRequests, 0, 0, requests * 2.0)
						.objects(objects, objects, objects - partialRequests * 5, partialRequests * 5, 0, 0, objects * 2.0)
						.batches(
										requests,
										objects,
										objects / configuredBatchSize,
										objects % configuredBatchSize == 0 ? 0 : 1)
						.versions(objects, 0)
						.bucket(bucket, objects, objects, objects - partialRequests * 5, partialRequests * 5)
						.phases(1_000_000_000L, 100_000_000L, 1_100_000_000L)
						.failurePolicy("fixed", 100_000, 0, 30, partialRequests * 5.0 / objects * 100.0)
						.reconciled(true)
						.build();
	}
}
