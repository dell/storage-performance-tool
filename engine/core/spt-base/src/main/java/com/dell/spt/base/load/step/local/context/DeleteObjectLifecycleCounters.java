package com.dell.spt.base.load.step.local.context;

/** Linearizable publication point for the object outcomes of terminal DELETE requests. */
final class DeleteObjectLifecycleCounters {
	private long accepted;
	private long failed;
	private long protocolFailed;
	private long fullSuccessfulRequests;

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
		return new Snapshot(accepted, failed, protocolFailed, fullSuccessfulRequests);
	}

	record Snapshot(
					long accepted,
					long failed,
					long protocolFailed,
					long fullSuccessfulRequests) {}
}
