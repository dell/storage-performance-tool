package com.dell.spt.storage.driver.coop.aws.s3;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.crt.CRT;

/**
 * A reference to the AWS CRT that keeps it serving Java once JVM shutdown has begun, held by a
 * driver from when its clients are built until they are closed.
 *
 * <p>The CRT registers a JVM shutdown hook of its own. On SIGINT or SIGTERM that hook runs at
 * once, beside the engine's hook that stops the step, and while no reference is held it stops the
 * CRT from calling Java at all: a response the target has sent never reaches its request, and no
 * new request can be signed. With a reference held, the CRT stays up until the last one is
 * released, so the stop drains the requests in flight as the {@code s3} driver does.
 *
 * <p>A reference is released at most once. Once the CRT's shutdown has begun no reference can be
 * taken; the driver is then built without one, as before.
 */
final class CrtShutdownRef {

	private static final Logger LOG = LoggerFactory.getLogger(CrtShutdownRef.class);

	/** The count the references are taken from: the CRT's own, or a stand-in in tests. */
	interface Counter {
		void acquire();

		void release();
	}

	/** The CRT's JVM-shutdown reference count, global to the process. */
	static final Counter CRT_COUNTER = new Counter() {
		@Override
		public void acquire() {
			CRT.acquireShutdownRef();
		}

		@Override
		public void release() {
			CRT.releaseShutdownRef();
		}
	};

	/** No reference: what a driver holds when it was built without a CRT client of its own. */
	static final CrtShutdownRef NONE = new CrtShutdownRef(null);

	private final Counter counter;
	private final AtomicBoolean held;

	private CrtShutdownRef(final Counter counter) {
		this.counter = counter;
		this.held = new AtomicBoolean(counter != null);
	}

	/**
	 * Takes a reference from the counter. If the counter refuses, as the CRT does once its
	 * shutdown has begun, the result holds nothing and releases nothing.
	 */
	static CrtShutdownRef acquire(final Counter counter) {
		Objects.requireNonNull(counter);
		try {
			counter.acquire();
		} catch (final RuntimeException refused) {
			LOG.debug("No CRT shutdown reference, so the CRT may stop serving requests at a JVM shutdown: {}",
							refused.toString());
			return NONE;
		}
		return new CrtShutdownRef(counter);
	}

	/** Whether this reference is still held. */
	boolean held() {
		return held.get();
	}

	/** Releases the reference if it is still held; later calls do nothing. Never throws. */
	void release() {
		if (!held.compareAndSet(true, false)) {
			return;
		}
		try {
			counter.release();
		} catch (final RuntimeException failure) {
			LOG.debug("Could not release the CRT shutdown reference: {}", failure.toString());
		}
	}
}
