package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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

	/** How many of the next registerBuffer() calls still return 0 (failure) before they succeed again. */
	private final AtomicInteger failNextRegistrations = new AtomicInteger();

	/** When true, generateToken() returns null. */
	private volatile boolean failTokenGeneration;

	/** When true, init() fails and the transport stays unavailable. */
	private volatile boolean failInit;

	/** Buffers still registered when the transport was closed; negative until then. */
	private volatile int activeRegistrationsAtClose = -1;

	/** The DC target number tokens carry; a NIC's fake sets its own. */
	private volatile int dctn = 0x00133f;

	/** Runs inside registerBuffer(), generateToken() and deregisterBuffer(), to hold a caller there. */
	private volatile Runnable beforeRegister = () -> {};
	private volatile Runnable beforeToken = () -> {};
	private volatile Runnable beforeDeregister = () -> {};
	private final AtomicInteger callsInFlight = new AtomicInteger();
	private final AtomicInteger closes = new AtomicInteger();
	private volatile boolean closedDuringCall;
	private volatile String strictDevice;
	private volatile String strictAddress;

	public FakeRdmaTransport(final RdmaConfig config) {
		super(config);
	}

	@Override
	public boolean init(final String endpoint, final String accessKey, final String secretKey) {
		if (failInit || endpoint == null || endpoint.isEmpty() || accessKey == null || secretKey == null) {
			available = false;
			return false;
		}
		available = true;
		return true;
	}

	@Override
	public void initStrict(final String device, final String localIp) {
		if (failInit) {
			throw new RdmaException("injected open failure of " + device + " at " + localIp, 5, "EIO");
		}
		strictDevice = device;
		strictAddress = localIp;
		available = true;
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
		callsInFlight.incrementAndGet();
		try {
			beforeRegister.run();
			final long count = registerCount.incrementAndGet();
			if (failAfterNRegistrations >= 0 && count > failAfterNRegistrations) {
				return 0; // simulate registration failure
			}
			if (failNextRegistrations.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
				return 0; // simulate a registration failure that does not last
			}
			final long handle = handleCounter.incrementAndGet();
			activeRegistrations.put(handle, buffer);
			return handle;
		} finally {
			callsInFlight.decrementAndGet();
		}
	}

	@Override
	public String generateToken(final long mrHandle, final int size) {
		if (!available || mrHandle == 0 || failTokenGeneration) {
			return null;
		}
		callsInFlight.incrementAndGet();
		try {
			beforeToken.run();
			// Return a valid-format token: addr:size:rkey:lid:dctn:g:gid
			return String.format("%016x:%08x:%08x:%04x:%06x:%d:%032x",
							0x7fffc3200000L + mrHandle, size, mrHandle & 0xFFFFFFFFL,
							0, dctn, 1, 0xfe80000000000000L);
		} finally {
			callsInFlight.decrementAndGet();
		}
	}

	@Override
	public void deregisterBuffer(final ByteBuffer buffer, final long mrHandle) {
		callsInFlight.incrementAndGet();
		try {
			beforeDeregister.run();
			if (mrHandle != 0 && !deregisteredHandles.add(mrHandle)) {
				// Handle was already deregistered — this is a double-free bug
				if (strictDeregister) {
					throw new IllegalStateException(
									"Double deregister detected for handle " + mrHandle);
				}
			}
			activeRegistrations.remove(mrHandle);
			deregisterCount.incrementAndGet();
		} finally {
			callsInFlight.decrementAndGet();
		}
	}

	@Override
	public void close() {
		closes.incrementAndGet();
		if (callsInFlight.get() > 0) {
			closedDuringCall = true;
		}
		available = false;
		if (activeRegistrationsAtClose < 0) {
			activeRegistrationsAtClose = activeRegistrations.size();
		}
		activeRegistrations.clear();
	}

	// ==================== Test verification helpers ====================

	/** How many times close() ran. */
	public int getCloseCount() {
		return closes.get();
	}

	/** True if close() ran while registerBuffer() or generateToken() was running. */
	public boolean wasClosedDuringACall() {
		return closedDuringCall;
	}

	/** The device of the strict open; null without one. */
	public String getStrictDevice() {
		return strictDevice;
	}

	/** The address of the strict open; null without one. */
	public String getStrictAddress() {
		return strictAddress;
	}

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

	/** Buffers nobody had deregistered when the transport was closed; negative before the close. */
	public int getActiveRegistrationsAtClose() {
		return activeRegistrationsAtClose;
	}

	/** Get the buffer associated with a registration handle (for content verification). */
	public ByteBuffer getRegisteredBuffer(final long handle) {
		return activeRegistrations.get(handle);
	}

	// ==================== Failure injection ====================

	/** Tokens carry this DC target number from now on. */
	public void setDctn(final int dctn) {
		this.dctn = dctn;
	}

	/** Runs in each registerBuffer() call, before it registers. */
	public void setBeforeRegister(final Runnable action) {
		this.beforeRegister = action;
	}

	/** Runs in each generateToken() call, before it makes the token. */
	public void setBeforeDeregister(final Runnable action) {
		this.beforeDeregister = action;
	}

	public void setBeforeToken(final Runnable action) {
		this.beforeToken = action;
	}

	/** After n successful registrations, subsequent calls return 0 (failure). Set -1 to disable. */
	public void setFailAfterNRegistrations(final int n) {
		this.failAfterNRegistrations = n;
	}

	/** The next n registerBuffer() calls return 0 (failure); the calls after them succeed. */
	public void setFailNextRegistrations(final int n) {
		this.failNextRegistrations.set(n);
	}

	/** When true, generateToken() always returns null. */
	public void setFailTokenGeneration(final boolean fail) {
		this.failTokenGeneration = fail;
	}

	/** When true, init() returns false (simulates a host without a usable RDMA device). */
	public void setFailInit(final boolean fail) {
		this.failInit = fail;
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
