package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.util.concurrent.atomic.LongAdder;

/**
 * Per-driver counts of which data path each CREATE/READ payload request actually used.
 *
 * <p>Selecting the RDMA driver does not prove that payloads moved over RDMA: operations can be
 * ineligible by size, preparation can fall back to HTTP, and the server can decline. These counts
 * make the effective path visible in the step summary.
 *
 * <p>Every request counter counts requests sent: submissions the parent driver accepted. A
 * submission it refuses is counted when a later attempt is accepted. The counters of registrations
 * and of failure causes count events, one for each attempt they happen in.
 */
final class RdmaPathStats {

	/** RDMA-proposed requests sent. Each ends in one of the six outcomes below, or in none. */
	final LongAdder rdmaRequestsSent = new LongAdder();
	/** Outcome: server reported the payload moved over RDMA. */
	final LongAdder rdmaTransferred = new LongAdder();
	/** RDMA success reported without bytes-transferred; requested size counted. */
	final LongAdder rdmaBytesAssumed = new LongAdder();
	/** Outcome: server declined or ignored RDMA on a 2xx response to a GET and sent the body over HTTP. */
	final LongAdder rdmaDeclinedServedOverHttp = new LongAdder();
	/** Outcome: server declined or ignored RDMA and the operation failed: a PUT, or a GET without fallback. */
	final LongAdder rdmaDeclinedFailed = new LongAdder();
	/** Outcome: the request ended with a non-success status, such as an HTTP error or a lost connection. */
	final LongAdder rdmaHttpError = new LongAdder();
	/** Outcome: RDMA reply contract violated. */
	final LongAdder rdmaProtocolError = new LongAdder();
	/** Outcome: ended by the client-side timeout reaper. */
	final LongAdder rdmaTimedOut = new LongAdder();
	/** RDMA disabled, or its initialization failed with fallback enabled: sent over HTTP. */
	final LongAdder httpRdmaUnavailable = new LongAdder();
	/** Below {@code thresholdBytes}: sent over HTTP by design. */
	final LongAdder httpBelowThreshold = new LongAdder();
	/** Operation kind not proposed for RDMA (ranged part reads): sent over HTTP. */
	final LongAdder httpIneligible = new LongAdder();
	/** Larger than one RDMA buffer: sent over HTTP. */
	final LongAdder httpOversize = new LongAdder();
	/** RDMA preparation failed and fallback sent the operation over HTTP. */
	final LongAdder httpFallback = new LongAdder();
	/** RDMA preparation failed with fallback disabled: operation failed. */
	final LongAdder prepareFailed = new LongAdder();

	/** A per-operation buffer was registered: the pool was disabled or gave none. */
	final LongAdder registrationsOnDemand = new LongAdder();
	/** The transport refused to register a per-operation buffer. */
	final LongAdder registrationFailedOnDemand = new LongAdder();
	/** Direct memory for a per-operation buffer could not be allocated. */
	final LongAdder allocationFailedOnDemand = new LongAdder();
	/** The transport gave no token for a registered buffer. */
	final LongAdder tokenGenerationFailed = new LongAdder();

	String summary() {
		return "rdmaTransferred=" + rdmaTransferred.sum()
						+ " (bytesAssumed=" + rdmaBytesAssumed.sum() + ")"
						+ ", rdmaDeclined=" + (rdmaDeclinedServedOverHttp.sum() + rdmaDeclinedFailed.sum())
						+ ", rdmaHttpError=" + rdmaHttpError.sum()
						+ ", rdmaProtocolError=" + rdmaProtocolError.sum()
						+ ", rdmaTimedOut=" + rdmaTimedOut.sum()
						+ ", httpRdmaUnavailable=" + httpRdmaUnavailable.sum()
						+ ", httpBelowThreshold=" + httpBelowThreshold.sum()
						+ ", httpIneligible=" + httpIneligible.sum()
						+ ", httpOversize=" + httpOversize.sum()
						+ ", httpFallback=" + httpFallback.sum()
						+ ", prepareFailed=" + prepareFailed.sum();
	}
}
