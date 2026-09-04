package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.op.Operation.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/** Validates neutral transport identities and reconciles them in request order. */
public final class DeleteRequestReconciler {

	private DeleteRequestReconciler() {}

	/**
	 * Validates transport response identities and returns a result ordered like the request.
	 * Protocol defects conservatively fail every target.
	 */
	public static DeleteRequestResult reconcile(
					final DeleteRequest request, final DeleteTransportResult transportResult) {
		if (transportResult == null) {
			return protocolFailure(request, "DELETE transport returned no reconciliation result");
		}
		if (transportResult.failureStatus() != null) {
			return operationalFailure(
							request, transportResult.failureStatus(), transportResult.failureMessage());
		}
		final var responses = transportResult.targetResults();
		if (responses == null) {
			final String message = transportResult.failureMessage();
			return protocolFailure(
							request,
							message == null || message.isBlank()
											? "DELETE transport target reconciliation is missing"
											: message);
		}
		final var requestedIdentities = new HashSet<DeleteTargetIdentity>(request.targets().size());
		for (final var target : request.targets()) {
			requestedIdentities.add(target.identity());
		}
		final var responsesByIdentity = new HashMap<DeleteTargetIdentity, DeleteTransportTargetResult>(responses.size());
		for (final var response : responses) {
			if (response == null || response.key() == null || response.key().isEmpty()) {
				return protocolFailure(request, "DELETE response contains a malformed identity");
			}
			final var identity = response.identity();
			if (!requestedIdentities.contains(identity)) {
				return protocolFailure(request, "DELETE response contains an unexpected identity");
			}
			if (responsesByIdentity.putIfAbsent(identity, response) != null) {
				return protocolFailure(request, "DELETE response contains a duplicate identity");
			}
		}
		if (responsesByIdentity.size() != request.targets().size()) {
			return protocolFailure(request, "DELETE response is missing requested identities");
		}
		final var targetResults = new ArrayList<DeleteTargetResult>(request.targets().size());
		var accepted = 0;
		for (final var target : request.targets()) {
			final var response = responsesByIdentity.get(target.identity());
			if (response.succeeded()) {
				accepted++;
				targetResults.add(new DeleteTargetResult(
								target, DeleteTargetOutcome.ACCEPTED, DeleteFailureClassification.NONE, null));
			} else {
				targetResults.add(new DeleteTargetResult(
								target,
								DeleteTargetOutcome.FAILED,
								DeleteFailureClassification.OPERATIONAL,
								response.errorMessage()));
			}
		}
		if (accepted == targetResults.size()) {
			return new DeleteRequestResult(
							DeleteRequestOutcome.FULL_SUCCESS,
							DeleteFailureClassification.NONE,
							Status.SUCC,
							targetResults,
							null);
		}
		return new DeleteRequestResult(
						accepted == 0 ? DeleteRequestOutcome.FAILED : DeleteRequestOutcome.PARTIAL,
						DeleteFailureClassification.OPERATIONAL,
						Status.RESP_FAIL_SVC,
						targetResults,
						null);
	}

	private static DeleteRequestResult operationalFailure(
					final DeleteRequest request, final Status status, final String message) {
		return new DeleteRequestResult(
						DeleteRequestOutcome.FAILED,
						DeleteFailureClassification.OPERATIONAL,
						status,
						request.targets().stream()
										.map(target -> new DeleteTargetResult(
														target,
														DeleteTargetOutcome.FAILED,
														DeleteFailureClassification.OPERATIONAL,
														message))
										.toList(),
						message);
	}

	private static DeleteRequestResult protocolFailure(
					final DeleteRequest request, final String message) {
		return new DeleteRequestResult(
						DeleteRequestOutcome.FAILED,
						DeleteFailureClassification.PROTOCOL,
						Status.RESP_FAIL_CORRUPT,
						request.targets().stream()
										.map(target -> new DeleteTargetResult(
														target,
														DeleteTargetOutcome.FAILED,
														DeleteFailureClassification.PROTOCOL,
														message))
										.toList(),
						message);
	}
}
