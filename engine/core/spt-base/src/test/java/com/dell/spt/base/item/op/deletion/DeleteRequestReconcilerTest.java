package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.storage.Credential;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeleteRequestReconcilerTest {

	private final DeleteTarget current = target("same-key", null);
	private final DeleteTarget exact = target("same-key", "v2");
	private final DeleteRequest request = new DeleteRequest("bucket", Credential.NONE, List.of(current, exact));

	@Test
	void reconcilesValidPartialResponseByKeyAndRequestedVersionInRequestOrder() {
		final var result = DeleteRequestReconciler.reconcile(
						request,
						new DeleteTransportResult(
										List.of(
														DeleteTransportTargetResult.failed(exact, "access denied"),
														DeleteTransportTargetResult.succeeded(current)),
										null,
										null));

		assertEquals(DeleteRequestOutcome.PARTIAL, result.outcome());
		assertEquals(DeleteFailureClassification.OPERATIONAL, result.failureClassification());
		assertEquals(Status.RESP_FAIL_SVC, result.operationStatus());
		assertEquals(1, result.acceptedObjectCount());
		assertEquals(1, result.failedObjectCount());
		assertEquals(List.of(current, exact), result.targetResults().stream()
						.map(DeleteTargetResult::target).toList());
		assertEquals(
						List.of(DeleteTargetOutcome.ACCEPTED, DeleteTargetOutcome.FAILED),
						result.targetResults().stream().map(DeleteTargetResult::outcome).toList());
	}

	@Test
	void transportFailureFailsEveryTargetOperationally() {
		final var result = DeleteRequestReconciler.reconcile(
						request, DeleteTransportResult.failure(Status.FAIL_TIMEOUT, "timed out"));

		assertEquals(DeleteRequestOutcome.FAILED, result.outcome());
		assertEquals(DeleteFailureClassification.OPERATIONAL, result.failureClassification());
		assertEquals(Status.FAIL_TIMEOUT, result.operationStatus());
		assertEquals(2, result.failedObjectCount());
		assertEquals(
						List.of(DeleteFailureClassification.OPERATIONAL, DeleteFailureClassification.OPERATIONAL),
						result.targetResults().stream().map(DeleteTargetResult::failureClassification).toList());
	}

	@Test
	void transportResultRejectsANonFailureRequestStatusRegardlessOfConstructionPath() {
		assertThrows(
						IllegalArgumentException.class,
						() -> new DeleteTransportResult(List.of(), Status.INTERRUPTED, "stopped"));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("protocolDefects")
	void protocolDefectFailsEveryTargetConservatively(
					final String name, final DeleteTransportResult transportResult) {
		final var result = DeleteRequestReconciler.reconcile(request, transportResult);

		assertEquals(DeleteRequestOutcome.FAILED, result.outcome(), name);
		assertEquals(DeleteFailureClassification.PROTOCOL, result.failureClassification(), name);
		assertEquals(Status.RESP_FAIL_CORRUPT, result.operationStatus(), name);
		assertEquals(0, result.acceptedObjectCount(), name);
		assertEquals(2, result.failedObjectCount(), name);
	}

	private Stream<Arguments> protocolDefects() {
		return Stream.of(
						Arguments.of(
										"missing",
										new DeleteTransportResult(
														List.of(DeleteTransportTargetResult.succeeded(current)), null, null)),
						Arguments.of(
										"duplicate",
										new DeleteTransportResult(
														List.of(
																		DeleteTransportTargetResult.succeeded(current),
																		DeleteTransportTargetResult.succeeded(current)),
														null,
														null)),
						Arguments.of(
										"malformed",
										new DeleteTransportResult(
														List.of(
																		new DeleteTransportTargetResult("", null, true, null),
																		DeleteTransportTargetResult.succeeded(exact)),
														null,
														null)),
						Arguments.of(
										"unexpected",
										new DeleteTransportResult(
														List.of(
																		DeleteTransportTargetResult.succeeded(current),
																		new DeleteTransportTargetResult("other", null, true, null)),
														null,
														null)),
						Arguments.of("null result", null));
	}

	private static DeleteTarget target(final String key, final String version) {
		return new DeleteTarget(new IntegrityManifestDataItem("bucket", key, 7, version));
	}
}
