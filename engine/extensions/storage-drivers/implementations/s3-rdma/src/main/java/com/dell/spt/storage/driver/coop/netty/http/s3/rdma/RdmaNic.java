package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One NIC's RDMA resources: its context, its pool, its copier and its data-path counts. A driver
 * without a NIC set has one, over the 5.16 objects. A driver with a NIC set has one per NIC, and an
 * attempt's {@code RdmaContext} carries its NIC's, so its release, the reaper and its completion
 * reach the right pool and context.
 */
final class RdmaNic {

	final int index;
	final String name;
	final RdmaTransport transport;
	/** {@code null} when {@code storage.rdma.bufferPool} is disabled. */
	final RdmaBufferPool pool;
	final RdmaPayloadCopier copier;
	final RdmaPathStats stats;
	private final AtomicBoolean closed = new AtomicBoolean();

	RdmaNic(final int index, final String name, final RdmaTransport transport, final RdmaBufferPool pool,
					final RdmaPayloadCopier copier, final RdmaPathStats stats) {
		this.index = index;
		this.name = name;
		this.transport = transport;
		this.pool = pool;
		this.copier = copier;
		this.stats = stats;
	}

	/**
	 * Ends the copier, then closes the pool and then the context, once. Nothing may be running on this
	 * NIC's lane: whoever saw it end calls this.
	 */
	void close() {
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		if (!copier.shutdown()) {
			Loggers.MSG.warn("NIC {}: the RDMA payload copy did not stop within {} ms", name, RdmaPayloadCopier.CLOSE_WAIT_MILLIS);
		}
		if (pool != null) {
			pool.close();
		}
		transport.close();
	}

	/** Whether {@link #close} ran. */
	boolean closed() {
		return closed.get();
	}

	/** The pool part of this NIC's data-path summary. */
	String poolSummary() {
		return pool == null ? ", bufferPool=disabled" : ", " + pool.summary();
	}
}
