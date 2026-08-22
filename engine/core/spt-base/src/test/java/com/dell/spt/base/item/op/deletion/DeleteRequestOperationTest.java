package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
		assertEquals(DeleteRequestOutcome.FULL_SUCCESS, snapshot.deleteResult().outcome());
		assertEquals(List.of("key-a", "key-b"), snapshot.deleteResult().targetResults().stream()
						.map(result -> result.target().key()).toList());
		assertThrows(
						IllegalStateException.class,
						() -> operation.completeDelete(DeleteTransportResult.success(request.targets())));
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
