package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.storage.Credential;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeleteArtifactsTest {

	@Test
	void versionedRowsKeepRequestAndObjectUnitsSeparateAndLinkEveryTarget() {
		final var first = new DeleteTarget(
						new IntegrityManifestDataItem("bucket", "a,key", 11, null), 0);
		final var second = new DeleteTarget(
						new IntegrityManifestDataItem("bucket", "b", 12, "v-2"), 1);
		final var request = new DeleteRequest("bucket", Credential.NONE, List.of(first, second));
		final String requestId = DeleteArtifacts.requestId(request);

		assertEquals(requestId, DeleteArtifacts.requestId(request));
		assertFalse(requestId.contains("a,key"));
		assertEquals("1", DeleteArtifacts.SCHEMA_VERSION);
		assertEquals("logical_api_requests", DeleteArtifacts.REQUEST_UNIT);
		assertEquals("object_identities", DeleteArtifacts.OBJECT_UNIT);
		assertTrue(DeleteArtifacts.REQUESTS_HEADER.contains("request_id"));
		assertTrue(DeleteArtifacts.OBJECTS_HEADER.contains("error_classification"));
		assertFalse(DeleteArtifacts.OBJECTS_HEADER.contains("duration_us"));
		assertFalse(DeleteArtifacts.OBJECTS_HEADER.contains("latency_us"));
		assertEquals(0, first.selectionIndex());
		assertEquals(1, second.selectionIndex());
	}

	@Test
	void residualPolicyExcludesAcceptedTargetsAndKeepsEveryUncertainOutcome() {
		assertFalse(DeleteArtifacts.isResidual(DeleteTargetOutcome.ACCEPTED));
		assertTrue(DeleteArtifacts.isResidual(DeleteTargetOutcome.FAILED));
		assertTrue(DeleteArtifacts.isResidual(DeleteTargetOutcome.UNATTEMPTED));
		assertTrue(DeleteArtifacts.isResidual(DeleteTargetOutcome.UNRESOLVED));
	}

	@Test
	void artifactVocabularyHasOneProducerAndValidatorMapping() {
		assertEquals(MetricsConstants.DELETE_REQUEST_UNIT, DeleteArtifacts.REQUEST_UNIT);
		assertEquals(MetricsConstants.DELETE_OBJECT_UNIT, DeleteArtifacts.OBJECT_UNIT);
		assertEquals("full_success", DeleteArtifacts.requestOutcome(DeleteRequestOutcome.FULL_SUCCESS));
		assertEquals("partial", DeleteArtifacts.requestOutcome(DeleteRequestOutcome.PARTIAL));
		assertEquals("failed", DeleteArtifacts.requestOutcome(DeleteRequestOutcome.FAILED));
		assertEquals("unresolved", DeleteArtifacts.REQUEST_OUTCOME_UNRESOLVED);
		assertEquals(0, DeleteArtifacts.requestOutcomeIndex(DeleteArtifacts.REQUEST_OUTCOME_FULL_SUCCESS));
		assertEquals(1, DeleteArtifacts.requestOutcomeIndex(DeleteArtifacts.REQUEST_OUTCOME_PARTIAL));
		assertEquals(2, DeleteArtifacts.requestOutcomeIndex(DeleteArtifacts.REQUEST_OUTCOME_FAILED));
		assertEquals(3, DeleteArtifacts.requestOutcomeIndex(DeleteArtifacts.REQUEST_OUTCOME_UNRESOLVED));

		assertEquals("accepted", DeleteArtifacts.targetOutcome(DeleteTargetOutcome.ACCEPTED));
		assertEquals("failed", DeleteArtifacts.targetOutcome(DeleteTargetOutcome.FAILED));
		assertEquals("unattempted", DeleteArtifacts.targetOutcome(DeleteTargetOutcome.UNATTEMPTED));
		assertEquals("unresolved", DeleteArtifacts.targetOutcome(DeleteTargetOutcome.UNRESOLVED));
		assertEquals(0, DeleteArtifacts.targetOutcomeIndex(DeleteArtifacts.TARGET_OUTCOME_ACCEPTED));
		assertEquals(1, DeleteArtifacts.targetOutcomeIndex(DeleteArtifacts.TARGET_OUTCOME_FAILED));
		assertEquals(2, DeleteArtifacts.targetOutcomeIndex(DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED));
		assertEquals(3, DeleteArtifacts.targetOutcomeIndex(DeleteArtifacts.TARGET_OUTCOME_UNRESOLVED));
		assertEquals(-1, DeleteArtifacts.requestOutcomeIndex("unknown"));
		assertEquals(-1, DeleteArtifacts.targetOutcomeIndex("unknown"));

		assertEquals("none", DeleteArtifacts.failureClassification(DeleteFailureClassification.NONE));
		assertEquals(
						"operational",
						DeleteArtifacts.failureClassification(DeleteFailureClassification.OPERATIONAL));
		assertEquals("protocol", DeleteArtifacts.failureClassification(DeleteFailureClassification.PROTOCOL));
	}
}
