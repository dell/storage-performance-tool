package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fake RDMA transport for integration testing without RDMA hardware.
 *
 * <p>Overrides all public methods of {@link RdmaTransport} to simulate
 * successful RDMA operations. Maintains internal state for verifying
 * resource management (buffer registration/deregistration) and supports
 * configurable failure injection.
 *
 * <p>Usage:
 * <pre>{@code
 * var fake = new FakeRdmaTransport(config);
 * fake.init("http://10.0.0.1:9020", "key", "secret");
 * long handle = fake.registerBuffer(buf, size);
 * String token = fake.generateToken(handle, size);
 * fake.deregisterBuffer(buf, handle);
 * assertTrue(fake.areAllDeregistered());
 * }</pre>
 */
public class FakeRdmaTransport extends RdmaTransport {

	private volatile boolean available;
	private final AtomicLong handleCounter = new AtomicLong(1000);
	private final ConcurrentMap<Long, ByteBuffer> activeRegistrations = new ConcurrentHashMap<>();
	private final AtomicLong registerCount = new AtomicLong();
	private final AtomicLong deregisterCount = new AtomicLong();

	/** Tracks handles that have been deregistered — detects double-deregister bugs. */
	private final Set<Long> deregisteredHandles = Collections.newSetFromMap(new ConcurrentHashMap<>());

	/** If true, deregisterBuffer() throws on double-deregister. */
	private volatile boolean strictDeregister = true;

	/** When >= 0, registerBuffer() returns 0 (failure) after this many successful registrations. */
	private volatile int failAfterNRegistrations = -1;

	/** When true, generateToken() returns null. */
	private volatile boolean failTokenGeneration;

	public FakeRdmaTransport(final RdmaConfig config) {
		super(config);
	}

	@Override
	public boolean init(final String endpoint, final String accessKey, final String secretKey) {
		if (endpoint == null || endpoint.isEmpty() || accessKey == null || secretKey == null) {
			available = false;
			return false;
		}
		available = true;
		return true;
	}

	@Override
	public boolean isAvailable() {
		return available;
	}

	@Override
	public long registerBuffer(final ByteBuffer buffer, final int size) {
		if (!available || buffer == null) {
			return 0;
		}
		final long count = registerCount.incrementAndGet();
		if (failAfterNRegistrations >= 0 && count > failAfterNRegistrations) {
			return 0; // simulate registration failure
		}
		final long handle = handleCounter.incrementAndGet();
		activeRegistrations.put(handle, buffer);
		return handle;
	}

	@Override
	public String generateToken(final long mrHandle, final int size) {
		if (!available || mrHandle == 0 || failTokenGeneration) {
			return null;
		}
		// Return a valid-format token: addr:size:rkey:lid:dctn:g:gid
		return String.format("%016x:%08x:%08x:%04x:%06x:%d:%032x",
						0x7fffc3200000L + mrHandle, size, mrHandle & 0xFFFFFFFFL,
						0, 0x00133f, 1, 0xfe80000000000000L);
	}

	@Override
	public void deregisterBuffer(final ByteBuffer buffer, final long mrHandle) {
		if (mrHandle != 0 && !deregisteredHandles.add(mrHandle)) {
			// Handle was already deregistered — this is a double-free bug
			if (strictDeregister) {
				throw new IllegalStateException(
								"Double deregister detected for handle " + mrHandle);
			}
		}
		activeRegistrations.remove(mrHandle);
		deregisterCount.incrementAndGet();
	}

	@Override
	public void close() {
		available = false;
		activeRegistrations.clear();
	}

	// ==================== Test verification helpers ====================

	/** Total number of registerBuffer() calls (including failed ones). */
	public long getRegisterCount() {
		return registerCount.get();
	}

	/** Total number of deregisterBuffer() calls. */
	public long getDeregisterCount() {
		return deregisterCount.get();
	}

	/** True if every registered buffer has been deregistered. */
	public boolean areAllDeregistered() {
		return activeRegistrations.isEmpty();
	}

	/** Number of buffers currently registered (not yet deregistered). */
	public int getActiveRegistrationCount() {
		return activeRegistrations.size();
	}

	/** Get the buffer associated with a registration handle (for content verification). */
	public ByteBuffer getRegisteredBuffer(final long handle) {
		return activeRegistrations.get(handle);
	}

	// ==================== Failure injection ====================

	/** After n successful registrations, subsequent calls return 0 (failure). Set -1 to disable. */
	public void setFailAfterNRegistrations(final int n) {
		this.failAfterNRegistrations = n;
	}

	/** When true, generateToken() always returns null. */
	public void setFailTokenGeneration(final boolean fail) {
		this.failTokenGeneration = fail;
	}

	/** Manually control availability (simulates transport going down). */
	public void setAvailable(final boolean available) {
		this.available = available;
	}

	/** When false, double-deregister logs but doesn't throw (default: true). */
	public void setStrictDeregister(final boolean strict) {
		this.strictDeregister = strict;
	}

	/** Returns true if the given handle was already deregistered. */
	public boolean wasDeregistered(final long handle) {
		return deregisteredHandles.contains(handle);
	}
}
