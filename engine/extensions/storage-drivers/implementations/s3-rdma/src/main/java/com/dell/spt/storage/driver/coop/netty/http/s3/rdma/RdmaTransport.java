package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstraction over native RDMA operations via libobjclient.
 *
 * This class provides a JNI bridge to the rdma-object-client library for
 * RDMA-accelerated S3 PUT/GET operations. When the native library is not
 * available, all methods fail gracefully to allow fallback to HTTP.
 */
public class RdmaTransport implements AutoCloseable {

	/** Whether the native library was successfully loaded. */
	private static final boolean NATIVE_AVAILABLE;

	static {
		boolean loaded = false;
		try {
			NativeLibraryLoader.load("spt_rdma_native");
			loaded = true;
			Loggers.MSG.info("RDMA native library loaded successfully");
		} catch (final UnsatisfiedLinkError e) {
			Loggers.MSG.info("RDMA native library not available: {}", e.getMessage());
		} catch (final Exception e) {
			Loggers.MSG.warn("Failed to load RDMA native library: {}", e.getMessage());
		}
		NATIVE_AVAILABLE = loaded;
	}

	// Native methods - only called when NATIVE_AVAILABLE is true
	private native long nativeInit(String endpoint, String accessKey, String secretKey,
					String localIp, String logLevel, int concurrency);

	private native void nativeClose(long handle);

	private native int nativePutObject(long handle, String bucket, String key,
					ByteBuffer buffer, int size);

	private native int nativeGetObject(long handle, String bucket, String key,
					ByteBuffer buffer, int maxSize);

	private native long nativeAllocateBuffer(int size);

	private native void nativeFreeBuffer(long bufferPtr);

	private native ByteBuffer nativeWrapBuffer(long bufferPtr, int size);

	private final RdmaConfig config;
	private volatile boolean initialized;
	private volatile long nativeHandle;

	/**
	 * Track native buffer pointers for ByteBuffers allocated via {@link #allocateBuffer}.
	 * This allows us to free them properly in {@link #freeBuffer}.
	 */
	private final ConcurrentMap<ByteBuffer, Long> nativeBufferPointers = new ConcurrentHashMap<>();

	public RdmaTransport(final RdmaConfig config) {
		this.config = config;
	}

	/**
	 * Initialize the native RDMA client.
	 *
	 * @param endpoint  S3 endpoint URL (e.g. "http://ecs-node:9020")
	 * @param accessKey S3 access key
	 * @param secretKey S3 secret key
	 * @return true if RDMA is available and initialized, false otherwise
	 */
	public boolean init(final String endpoint, final String accessKey, final String secretKey) {
		if (!NATIVE_AVAILABLE) {
			Loggers.MSG.info("RDMA transport: native library not available (stub mode)");
			initialized = false;
			return false;
		}

		try {
			nativeHandle = nativeInit(
							endpoint,
							accessKey,
							secretKey,
							config.getLocalIp(),
							config.getLogLevel(),
							0  // Use default concurrency
			);

			if (nativeHandle != 0) {
				initialized = true;
				Loggers.MSG.info("RDMA transport initialized: endpoint={}", endpoint);
				return true;
			} else {
				Loggers.MSG.warn("RDMA transport initialization failed");
				initialized = false;
				return false;
			}
		} catch (final Exception e) {
			Loggers.MSG.warn("RDMA transport init exception: {}", e.getMessage());
			initialized = false;
			return false;
		}
	}

	/**
	 * PUT an object via RDMA.
	 *
	 * @param bucket S3 bucket name
	 * @param key    S3 object key
	 * @param buffer direct ByteBuffer containing the object data
	 * @param size   number of bytes to write from the buffer
	 * @return HTTP status code on success, or -1 on failure
	 */
	public int putObject(final String bucket, final String key,
					final ByteBuffer buffer, final int size) {
		if (!initialized || nativeHandle == 0) {
			return -1;
		}

		try {
			return nativePutObject(nativeHandle, bucket, key, buffer, size);
		} catch (final Exception e) {
			Loggers.MSG.warn("RDMA putObject exception: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * GET an object via RDMA.
	 *
	 * @param bucket  S3 bucket name
	 * @param key     S3 object key
	 * @param buffer  direct ByteBuffer to receive the object data
	 * @param maxSize maximum number of bytes to read
	 * @return HTTP status code on success, or -1 on failure
	 */
	public int getObject(final String bucket, final String key,
					final ByteBuffer buffer, final int maxSize) {
		if (!initialized || nativeHandle == 0) {
			return -1;
		}

		try {
			return nativeGetObject(nativeHandle, bucket, key, buffer, maxSize);
		} catch (final Exception e) {
			Loggers.MSG.warn("RDMA getObject exception: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * Allocate a buffer suitable for RDMA operations.
	 *
	 * When native RDMA is available, this allocates RDMA-registered memory for
	 * optimal zero-copy transfers. Otherwise, it returns a standard direct ByteBuffer.
	 *
	 * @param size buffer size in bytes
	 * @return a direct ByteBuffer
	 */
	public ByteBuffer allocateBuffer(final int size) {
		if (!NATIVE_AVAILABLE) {
			// Fallback: use standard direct buffer
			return ByteBuffer.allocateDirect(size);
		}

		try {
			final long ptr = nativeAllocateBuffer(size);
			if (ptr != 0) {
				final ByteBuffer buffer = nativeWrapBuffer(ptr, size);
				if (buffer != null) {
					nativeBufferPointers.put(buffer, ptr);
					return buffer;
				}
				// Failed to wrap, free the native memory
				nativeFreeBuffer(ptr);
			}
		} catch (final Exception e) {
			Loggers.MSG.debug("Native buffer allocation failed, using fallback: {}", e.getMessage());
		}

		// Fallback: use standard direct buffer
		return ByteBuffer.allocateDirect(size);
	}

	/**
	 * Free a buffer previously allocated by {@link #allocateBuffer}.
	 *
	 * For buffers allocated with native RDMA memory, this deregisters and frees
	 * the memory. For standard direct ByteBuffers, this is a no-op.
	 *
	 * @param buffer the buffer to free
	 */
	public void freeBuffer(final ByteBuffer buffer) {
		if (buffer == null) {
			return;
		}

		// Check if this is a native buffer we allocated
		final Long ptr = nativeBufferPointers.remove(buffer);
		if (ptr != null && NATIVE_AVAILABLE) {
			try {
				nativeFreeBuffer(ptr);
			} catch (final Exception e) {
				Loggers.MSG.debug("Native buffer free failed: {}", e.getMessage());
			}
		}
		// Standard direct ByteBuffers are handled by GC
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

	@Override
	public void close() {
		if (nativeHandle != 0 && NATIVE_AVAILABLE) {
			try {
				nativeClose(nativeHandle);
			} catch (final Exception e) {
				Loggers.MSG.debug("Native close failed: {}", e.getMessage());
			}
		}
		nativeHandle = 0;
		initialized = false;

		// Free any remaining native buffers
		for (final var entry : nativeBufferPointers.entrySet()) {
			if (NATIVE_AVAILABLE) {
				try {
					nativeFreeBuffer(entry.getValue());
				} catch (final Exception ignored) {}
			}
		}
		nativeBufferPointers.clear();
	}
}
