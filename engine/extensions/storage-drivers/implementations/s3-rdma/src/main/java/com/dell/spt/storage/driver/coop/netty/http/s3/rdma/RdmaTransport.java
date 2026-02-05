package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstraction over native RDMA operations via NVIDIA cuObject.
 *
 * <p>This class provides a JNI bridge to the NVIDIA cuObject client library
 * (from CUDA 13.1.1+) for RDMA-accelerated S3 PUT/GET operations. When the
 * native library is not available, all methods fail gracefully to allow
 * transparent fallback to HTTP.
 *
 * <p>V2 Architecture uses the hybrid HTTP control + RDMA data transfer model:
 * <ul>
 *   <li>HTTP sends/receives metadata and control (x-amz-rdma-token header)</li>
 *   <li>RDMA transfers bulk data directly between registered memory regions</li>
 *   <li>Server responds with x-amz-rdma-reply header (200=accepted, 501=declined)</li>
 * </ul>
 *
 * @see <a href="https://docs.nvidia.com/gpudirect-storage/cuobject/index.html">cuObject Documentation</a>
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

	private native int nativeIsRdmaAvailable();

	private final RdmaConfig config;
	private volatile boolean initialized;
	private volatile long nativeHandle;
	private volatile RdmaBufferPool bufferPool;

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
	 * Track native buffer pointers for ByteBuffers allocated via {@link #allocateNative}.
	 * This allows us to free them properly in {@link #freeNative}.
	 */
	private final ConcurrentMap<IdentityKey, Long> nativeBufferPointers = new ConcurrentHashMap<>();

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

				// Initialize buffer pool if configured
				if (config.getPoolSize() > 0) {
					bufferPool = new RdmaBufferPool(this, config.getBufferSize(), config.getPoolSize());
					bufferPool.init();
				}

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
	 * When native RDMA is available, this attempts to acquire a pre-registered buffer
	 * from the pool. If the requested size exceeds the pool's buffer size or the pool
	 * is empty, it falls back to a slow on-demand allocation and registration.
	 *
	 * @param size buffer size in bytes
	 * @return a direct ByteBuffer
	 */
	public ByteBuffer allocateBuffer(final int size) {
		if (!NATIVE_AVAILABLE) {
			// Fallback: use standard direct buffer
			return ByteBuffer.allocateDirect(size);
		}

		// Try to use the buffer pool first
		final RdmaBufferPool pool = bufferPool;
		if (pool != null && size <= pool.getBufferSize()) {
			try {
				final ByteBuffer pooledBuf = pool.acquire(0, java.util.concurrent.TimeUnit.MILLISECONDS);
				if (pooledBuf != null) {
					return pooledBuf;
				}
				Loggers.MSG.trace("RDMA buffer pool empty, falling back to slow allocation");
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		// Fallback to slow path: allocate and register on-demand
		final ByteBuffer buf = allocateNative(size);
		if (buf != null) {
			return buf;
		}

		// Absolute fallback: use standard direct buffer (not RDMA capable)
		return ByteBuffer.allocateDirect(size);
	}

	/**
	 * Free a buffer previously allocated by {@link #allocateBuffer}.
	 *
	 * Returns pooled buffers to the pool, or deregisters and frees on-demand buffers.
	 *
	 * @param buffer the buffer to free
	 */
	public void freeBuffer(final ByteBuffer buffer) {
		if (buffer == null) {
			return;
		}

		final RdmaBufferPool pool = bufferPool;
		if (pool != null && buffer.capacity() == pool.getBufferSize()) {
			pool.release(buffer);
			return;
		}

		freeNative(buffer);
	}

	/**
	 * Internal: Allocate a native RDMA-registered buffer.
	 */
	public ByteBuffer allocateNative(final int size) {
		if (!NATIVE_AVAILABLE)
			return null;
		try {
			final long ptr = nativeAllocateBuffer(size);
			if (ptr != 0) {
				final ByteBuffer buffer = nativeWrapBuffer(ptr, size);
				if (buffer != null) {
					nativeBufferPointers.put(new IdentityKey(buffer), ptr);
					return buffer;
				}
				nativeFreeBuffer(ptr);
			}
		} catch (final Exception e) {
			Loggers.MSG.debug("Native buffer allocation failed: {}", e.getMessage());
		}
		return null;
	}

	/**
	 * Internal: Free a native RDMA-registered buffer.
	 */
	public void freeNative(final ByteBuffer buffer) {
		if (buffer == null)
			return;
		final Long ptr = nativeBufferPointers.remove(new IdentityKey(buffer));
		if (ptr != null && NATIVE_AVAILABLE) {
			try {
				nativeFreeBuffer(ptr);
			} catch (final Exception e) {
				Loggers.MSG.debug("Native buffer free failed: {}", e.getMessage());
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
	 * This checks for CUDA runtime and RDMA device availability.
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
		if (bufferPool != null) {
			try {
				bufferPool.close();
			} catch (final Exception e) {
				Loggers.MSG.debug("Native pool close failed: {}", e.getMessage());
			}
			bufferPool = null;
		}

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
