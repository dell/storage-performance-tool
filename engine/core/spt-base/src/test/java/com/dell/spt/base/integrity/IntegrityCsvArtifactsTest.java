package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import java.io.StringReader;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

class IntegrityCsvArtifactsTest {

	@Test
	void failureRowsAreRfc4180AndPreserveExactIdentity() throws Exception {
		final Operation<?> op = mock(Operation.class);
		final Item item = mock(Item.class);
		when(item.name()).thenReturn("key,with\nquote\"");
		when(op.item()).thenReturn(item);
		when(op.requestedVersionId()).thenReturn("v,1");
		when(op.returnedVersionId()).thenReturn("v2");
		when(op.responseRequestId()).thenReturn("request");
		when(op.opRetryCount()).thenReturn(2);
		when(op.integrityVerificationResult()).thenReturn(new IntegrityVerificationResult(
						new IntegrityMetadata("1", "sha256", "0".repeat(64), 10L),
						"f".repeat(64),
						10L,
						100L,
						IntegrityFailureReason.DIGEST_MISMATCH,
						"mismatch"));

		final String row = IntegrityCsvArtifacts.failureRecord("node", "step", "s3-aws", op);
		final var records = CSVFormat.RFC4180.parse(new StringReader(row)).getRecords();

		assertEquals(1, records.size());
		assertEquals(16, records.get(0).size());
		assertEquals("key,with\nquote\"", records.get(0).get(4));
		assertEquals("v,1", records.get(0).get(5));
		assertEquals("digest_mismatch", records.get(0).get(8));
		assertEquals("3", records.get(0).get(15));
	}

	@Test
	void performanceRowUsesWorkerTimeAndLeavesReadDelayBlank() throws Exception {
		final var snapshot = new IntegrityPerformanceAccumulator.Snapshot(
						2L, 2L * 1024 * 1024, 1_000_000_000L, -1L, 3L);

		final String row = IntegrityCsvArtifacts.performanceRecord(
						"node", "step", "s3", "read_verify", snapshot);
		final var record = CSVFormat.RFC4180.parse(new StringReader(row)).getRecords().get(0);

		assertEquals(11, record.size());
		assertEquals("1.000000000", record.get(7));
		assertEquals("2.000000000", record.get(8));
		assertEquals("", record.get(9));
		assertEquals("3", record.get(10));
	}

	@Test
	void artifactApplicabilityMatchesStepRole() {
		assertTrue(IntegrityCsvArtifacts.applicableHeaders(OpType.LIST, "s3", true).isEmpty());
		assertTrue(IntegrityCsvArtifacts.applicableHeaders(OpType.READ, "s3", false).isEmpty());
		assertEquals(
						List.of(
										IntegrityCsvArtifacts.Kind.FAILURES,
										IntegrityCsvArtifacts.Kind.PERFORMANCE),
						IntegrityCsvArtifacts.applicableHeaders(OpType.READ, "s3", true)
										.stream().map(IntegrityCsvArtifacts.Artifact::kind).toList());
		assertEquals(
						List.of(
										IntegrityCsvArtifacts.Kind.PERFORMANCE,
										IntegrityCsvArtifacts.Kind.MULTIPART_LIFECYCLE),
						IntegrityCsvArtifacts.applicableHeaders(OpType.CREATE, "s3-aws", true)
										.stream().map(IntegrityCsvArtifacts.Artifact::kind).toList());
		assertEquals(
						List.of(IntegrityCsvArtifacts.Kind.PERFORMANCE),
						IntegrityCsvArtifacts.applicableHeaders(OpType.CREATE, "s3-aws", true, false)
										.stream().map(IntegrityCsvArtifacts.Artifact::kind).toList());
	}

	@Test
	void multipartLifecycleEmissionIsClaimedExactlyOnce() {
		final var operation = new CompositeDataOperationImpl<>();

		assertTrue(IntegrityCsvArtifacts.logMultipartLifecycleOnce(
						operation, "node", "step", "s3", "bucket", "key", "upload",
						"completed", false, null, null));
		assertFalse(IntegrityCsvArtifacts.logMultipartLifecycleOnce(
						operation, "node", "step", "s3", "bucket", "key", "upload",
						"failed_orphaned", true, false, "late callback"));
	}
}
