package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.storage.Credential;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeleteRequestOperationTest {

	@Test
	void requestOwnsImmutableCanonicalTargetsAndSnapshotRetainsTheWholeRequest() {
		final var first = new IntegrityManifestDataItem("bucket", "key-a", 11, null);
		final var second = new IntegrityManifestDataItem("bucket", "key-b", 22, "version-b");
		final var source = new ArrayList<>(List.of(new DeleteTarget(first), new DeleteTarget(second)));
		final var credential = Credential.getInstance("uid", "secret");
		final var request = new DeleteRequest("bucket", credential, source);
		source.clear();

		final var operation = new DeleteRequestOperationImpl(7, request);
		operation.startRequest();
		operation.markRequestFirstByteSent();
		operation.finishRequest();
		operation.markResponseFirstByteReceived();
		operation.finishResponse();
		operation.completeDelete(DeleteTransportResult.success(request.targets()));
		final var snapshot = operation.result();

		assertEquals(2, request.targets().size());
		assertThrows(UnsupportedOperationException.class, () -> request.targets().clear());
		assertEquals("key-a", request.targets().get(0).key());
		assertEquals(11, request.targets().get(0).size());
		assertEquals("version-b", request.targets().get(1).versionId());
		assertSame(first, operation.item(), "item() remains only the first-target compatibility projection");
		assertFalse((Object) operation instanceof DataOperation, "standalone DELETE is not a data-transfer operation");
		assertNotSame(operation, snapshot);
		assertSame(request, snapshot.deleteRequest());
		assertTrue(operation.requestFirstByteTime() >= operation.reqTimeStart());
		assertEquals(operation.requestFirstByteTime(), snapshot.requestFirstByteTime());
		assertEquals(operation.respTimeStart(), operation.responseFirstByteTime());
		assertEquals(operation.responseFirstByteTime(), snapshot.responseFirstByteTime());
		assertEquals(DeleteRequestOutcome.FULL_SUCCESS, snapshot.deleteResult().outcome());
		assertEquals(List.of("key-a", "key-b"), snapshot.deleteResult().targetResults().stream()
						.map(result -> result.target().key()).toList());
		assertThrows(
						IllegalStateException.class,
						() -> operation.completeDelete(DeleteTransportResult.success(request.targets())));
		operation.reset();
		assertEquals(0, operation.requestFirstByteTime());
		assertEquals(0, operation.responseFirstByteTime());
	}

	@Test
	void nativeTransportTimingRetainsFirstSendAcrossTransparentRetries() {
		final var request = new DeleteRequest(
						"bucket",
						Credential.getInstance("uid", "secret"),
						List.of(new DeleteTarget(
										new IntegrityManifestDataItem("bucket", "key", 1, null))));
		final var operation = new DeleteRequestOperationImpl(0, request);

		operation.beginTransportAttempt();
		operation.recordTransportRequestTiming(1_000_000L, 4_000_000L);
		assertEquals(3_000L, operation.transportRequestLatency());

		operation.beginTransportAttempt();
		assertEquals(0L, operation.transportRequestLatency());
		operation.recordTransportRequestTiming(9_000_000L, 11_000_000L);
		assertEquals(10_000L, operation.transportRequestLatency(),
						"the terminal response must remain measured from the first provider send");
		assertEquals(10_000L, operation.result().transportRequestLatency());

		operation.reset();
		assertEquals(0L, operation.transportRequestLatency());
		operation.recordTransportRequestTiming(20_000_000L, 19_000_000L);
		assertEquals(0L, operation.transportRequestLatency(),
						"invalid transport-clock ordering must fail closed");
	}

	@Test
	void requestRejectsInvalidCardinalityIdentityBucketCredentialAndDuplicates() {
		final var credential = Credential.getInstance("uid", "secret");
		final var first = new DeleteTarget(new IntegrityManifestDataItem("bucket", "key", 1, null));
		final var otherBucket = new DeleteTarget(new IntegrityManifestDataItem("other", "key-2", 1, null));

		assertThrows(IllegalArgumentException.class, () -> new DeleteRequest("bucket", credential, List.of()));
		assertThrows(
						IllegalArgumentException.class,
						() -> new DeleteRequest("bucket", credential, java.util.Collections.nCopies(1001, first)));
		assertThrows(
						IllegalArgumentException.class,
						() -> new DeleteRequest("bucket", credential, List.of(first, first)));
		assertThrows(
						IllegalArgumentException.class,
						() -> new DeleteRequest("bucket", credential, List.of(first, otherBucket)));
		assertThrows(
						IllegalArgumentException.class,
						() -> DeleteTransportResult.failure(Status.INTERRUPTED, "cancelled"));
	}
}
