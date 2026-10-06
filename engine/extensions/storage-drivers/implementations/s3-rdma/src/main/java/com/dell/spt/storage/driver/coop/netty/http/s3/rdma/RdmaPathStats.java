package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.util.concurrent.atomic.LongAdder;

/**
 * Per-driver counts of which data path each CREATE/READ data operation actually used.
 *
 * <p>Selecting the RDMA driver does not prove that payloads moved over RDMA: operations can be
 * ineligible by size, preparation can fall back to HTTP, and the server can decline. These counts
 * make the effective path visible in the step summary.
 */
final class RdmaPathStats {

	/** Server reported the payload moved over RDMA. */
	final LongAdder rdmaTransferred = new LongAdder();
	/** RDMA success reported without bytes-transferred; requested size counted. */
	final LongAdder rdmaBytesAssumed = new LongAdder();
	/** Server declined or ignored RDMA on a 2xx response. */
	final LongAdder rdmaDeclined = new LongAdder();
	/** RDMA-proposed request answered with an HTTP error status. */
	final LongAdder rdmaHttpError = new LongAdder();
	/** RDMA reply contract violated. */
	final LongAdder rdmaProtocolError = new LongAdder();
	/** RDMA-proposed request ended by the client-side timeout reaper. */
	final LongAdder rdmaTimedOut = new LongAdder();
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

	String summary() {
		return "rdmaTransferred=" + rdmaTransferred.sum()
						+ " (bytesAssumed=" + rdmaBytesAssumed.sum() + ")"
						+ ", rdmaDeclined=" + rdmaDeclined.sum()
						+ ", rdmaHttpError=" + rdmaHttpError.sum()
						+ ", rdmaProtocolError=" + rdmaProtocolError.sum()
						+ ", rdmaTimedOut=" + rdmaTimedOut.sum()
						+ ", httpBelowThreshold=" + httpBelowThreshold.sum()
						+ ", httpIneligible=" + httpIneligible.sum()
						+ ", httpOversize=" + httpOversize.sum()
						+ ", httpFallback=" + httpFallback.sum()
						+ ", prepareFailed=" + prepareFailed.sum();
	}
}
