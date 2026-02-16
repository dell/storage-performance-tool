package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * RDMA Transport - Direct libibverbs implementation.
 *
 * <p>This class provides a JNI bridge to libibverbs/libmlx5 for RDMA token
 * generation. The native layer handles memory registration and token generation;
 * the actual HTTP requests are sent through the parent S3StorageDriver's Netty
 * pipeline with the RDMA token added as an HTTP header.
 *
 * <p>See RDMA_ARCHITECTURE_V3.md for design details and implementation notes.
 *
 * <p>Token format: {@code addr:size:rkey:lid:dctn:g:gid}
 *
 * <p>The S3-RDMA protocol works as follows:
 * <ul>
 *   <li>PUT: Client sends HTTP with token header, server performs RDMA READ from client</li>
 *   <li>GET: Client sends HTTP with token header, server performs RDMA WRITE to client</li>
 * </ul>
 */
public class RdmaTransport implements AutoCloseable {

	/** Whether the native library was successfully loaded. */
	private static final boolean NATIVE_AVAILABLE;

	/**
	 * Cached reference to {@code sun.misc.Unsafe.invokeCleaner(ByteBuffer)} for explicit
	 * direct buffer deallocation (JDK 9+). Null if reflective access is unavailable.
	 */
	private static final Object UNSAFE_INSTANCE;
	private static final Method INVOKE_CLEANER;

	static {
		boolean loaded = false;
		try {
			NativeLibraryLoader.load("spt_rdma");
			loaded = true;
			Loggers.MSG.info("S3-RDMA native library loaded successfully");
		} catch (final UnsatisfiedLinkError e) {
			Loggers.MSG.info("S3-RDMA native library not available: {}", e.getMessage());
		} catch (final Exception e) {
			Loggers.MSG.warn("Failed to load S3-RDMA native library: {}", e.getMessage());
		}
		NATIVE_AVAILABLE = loaded;

		// Cache Unsafe.invokeCleaner for explicit direct buffer deallocation
		Object unsafe = null;
		Method cleaner = null;
		try {
			final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
			final var theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			unsafe = theUnsafe.get(null);
			cleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
		} catch (final Exception e) {
			Loggers.MSG.debug("Unsafe.invokeCleaner not available, direct buffers will rely on GC: {}",
							e.getMessage());
		}
		UNSAFE_INSTANCE = unsafe;
		INVOKE_CLEANER = cleaner;
	}

	/**
	 * JNI callback: native code calls this to route warnings through the Java logging framework.
	 * Must be static so the native layer can call it without an instance reference.
	 */
	@SuppressWarnings("unused")  // called from native code via JNI
	private static void nativeLogWarn(final String msg) {
		Loggers.MSG.warn("S3-RDMA native: {}", msg);
	}

	// Native methods - only called when NATIVE_AVAILABLE is true
	private native long nativeInit(String deviceName, String localIp);

	private native void nativeClose(long handle);

	private native long nativeRegisterBuffer(long handle, ByteBuffer buffer, int size);

	private native void nativeDeregisterBuffer(long handle, long mrHandle);

	private native String nativeGenerateToken(long handle, long mrHandle, int size);

	private native int nativeIsRdmaAvailable();

	private final RdmaConfig config;
	private volatile boolean initialized;
	private volatile long nativeHandle;

	/**
	 * Identity-based key for tracking ByteBuffers without calling their content-based hashCode().
	 */
	private static final class IdentityKey {
		private final ByteBuffer buffer;

		IdentityKey(final ByteBuffer buffer) {
			this.buffer = buffer;
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(buffer);
		}

		@Override
		public boolean equals(final Object obj) {
			return obj instanceof IdentityKey && ((IdentityKey) obj).buffer == this.buffer;
		}
	}

	/**
	 * Track memory registration handles for ByteBuffers.
	 */
	private final ConcurrentMap<IdentityKey, Long> mrHandles = new ConcurrentHashMap<>();

	public RdmaTransport(final RdmaConfig config) {
		this.config = config;
	}

	/**
	 * Initialize the RDMA transport.
	 *
	 * <p>Initialization only requires the device name (or "auto" for auto-detection).
	 * S3 credentials are not needed for RDMA setup, but are validated for consistency
	 * since HTTP authentication is handled by the parent driver's Netty pipeline.
	 *
	 * @param endpoint  S3 endpoint URL (validated but unused for RDMA setup)
	 * @param accessKey S3 access key (validated but unused for RDMA setup)
	 * @param secretKey S3 secret key (validated but unused for RDMA setup)
	 * @return true if RDMA is available and initialized, false otherwise
	 */
	public boolean init(final String endpoint, final String accessKey, final String secretKey) {
		if (!NATIVE_AVAILABLE) {
			Loggers.MSG.info("S3-RDMA transport: native library not available (stub mode)");
			initialized = false;
			return false;
		}

		// Validate parameters (fail fast on null/empty endpoint for consistency)
		if (endpoint == null || endpoint.isEmpty()) {
			Loggers.MSG.warn("S3-RDMA transport: invalid endpoint");
			initialized = false;
			return false;
		}
		if (accessKey == null || secretKey == null) {
			Loggers.MSG.warn("S3-RDMA transport: invalid credentials");
			initialized = false;
			return false;
		}

		try {
			// Only device name and local IP needed - credentials are handled by HTTP layer
			final String deviceName = config.getDevice();
			final String localIp = config.getLocalIp();
			nativeHandle = nativeInit(deviceName, localIp);

			if (nativeHandle != 0) {
				initialized = true;
				Loggers.MSG.info("S3-RDMA transport initialized: device={}, localIp={}",
								deviceName, localIp.isEmpty() ? "auto" : localIp);

				return true;
			} else {
				Loggers.MSG.warn("S3-RDMA transport initialization failed");
				initialized = false;
				return false;
			}
		} catch (final Exception e) {
			Loggers.MSG.warn("S3-RDMA transport init exception: {}", e.getMessage());
			initialized = false;
			return false;
		}
	}

	/**
	 * Register a buffer for RDMA operations.
	 *
	 * @param buffer direct ByteBuffer to register
	 * @param size   buffer size
	 * @return memory registration handle, or 0 if registration failed
	 */
	public long registerBuffer(final ByteBuffer buffer, final int size) {
		if (!initialized || nativeHandle == 0 || buffer == null) {
			return 0;
		}

		try {
			final long mrHandle = nativeRegisterBuffer(nativeHandle, buffer, size);
			if (mrHandle != 0) {
				mrHandles.put(new IdentityKey(buffer), mrHandle);
			}
			return mrHandle;
		} catch (final Exception e) {
			Loggers.MSG.warn("RDMA buffer registration failed: {}", e.getMessage());
			return 0;
		}
	}

	/**
	 * Deregister a buffer previously registered with {@link #registerBuffer}.
	 *
	 * @param buffer   the buffer to deregister
	 * @param mrHandle the memory registration handle
	 */
	public void deregisterBuffer(final ByteBuffer buffer, final long mrHandle) {
		if (buffer != null) {
			mrHandles.remove(new IdentityKey(buffer));
		}
		if (mrHandle != 0 && NATIVE_AVAILABLE && nativeHandle != 0) {
			try {
				nativeDeregisterBuffer(nativeHandle, mrHandle);
			} catch (final Exception e) {
				Loggers.MSG.debug("RDMA buffer deregistration failed: {}", e.getMessage());
			}
		}
	}

	/**
	 * Generate an RDMA token for the given registered buffer.
	 *
	 * <p>Token format: {@code addr:size:rkey:lid:dctn:g:gid}
	 *
	 * @param mrHandle memory registration handle from {@link #registerBuffer}
	 * @param size     size of data to transfer
	 * @return RDMA token string, or null if generation failed
	 */
	public String generateToken(final long mrHandle, final int size) {
		if (!initialized || nativeHandle == 0 || mrHandle == 0) {
			return null;
		}

		try {
			return nativeGenerateToken(nativeHandle, mrHandle, size);
		} catch (final Exception e) {
			Loggers.MSG.warn("RDMA token generation failed: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Explicitly deallocate a direct ByteBuffer's native memory via
	 * {@code sun.misc.Unsafe.invokeCleaner()} (JDK 9+). Falls back to GC
	 * if Unsafe is unavailable (e.g., restricted module access).
	 */
	private static void cleanDirectBuffer(final ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect()) {
			return;
		}
		if (INVOKE_CLEANER != null && UNSAFE_INSTANCE != null) {
			try {
				INVOKE_CLEANER.invoke(UNSAFE_INSTANCE, buffer);
			} catch (final Exception e) {
				Loggers.MSG.trace("Direct buffer cleanup failed, relying on GC: {}", e.getMessage());
			}
		}
	}

	/**
	 * @return true if native RDMA library is loaded (may still need init)
	 */
	public static boolean isNativeAvailable() {
		return NATIVE_AVAILABLE;
	}

	/**
	 * @return true if RDMA transport is initialized and available for operations
	 */
	public boolean isAvailable() {
		return initialized && nativeHandle != 0;
	}

	/**
	 * Check if RDMA hardware capabilities are available on this system.
	 *
	 * @return true if RDMA hardware appears available
	 */
	public boolean isRdmaHardwareAvailable() {
		if (!NATIVE_AVAILABLE) {
			return false;
		}
		try {
			return nativeIsRdmaAvailable() != 0;
		} catch (final Exception e) {
			Loggers.MSG.debug("RDMA hardware check failed: {}", e.getMessage());
			return false;
		}
	}

	@Override
	public void close() {
		// Set initialized=false FIRST — volatile write creates happens-before with
		// the volatile reads in registerBuffer/deregisterBuffer/generateToken, so any
		// concurrent call that checks initialized after this point will see false and
		// bail out before touching the native handle.
		initialized = false;

		// Deregister all remaining buffers
		final long handle = nativeHandle;
		for (final var entry : mrHandles.entrySet()) {
			if (NATIVE_AVAILABLE && handle != 0) {
				try {
					nativeDeregisterBuffer(handle, entry.getValue());
				} catch (final Exception ignored) {}
			}
		}
		mrHandles.clear();

		if (handle != 0 && NATIVE_AVAILABLE) {
			try {
				nativeClose(handle);
			} catch (final Exception e) {
				Loggers.MSG.debug("Native close failed: {}", e.getMessage());
			}
		}
		nativeHandle = 0;
	}
}
