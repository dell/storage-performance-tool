package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class RdmaBufferPoolTest {

	private RdmaTransport mockTransport;
	private RdmaBufferPool pool;
	private static final int BUFFER_SIZE = 1024;
	private static final int POOL_COUNT = 5;

	@BeforeEach
	void setUp() {
		mockTransport = mock(RdmaTransport.class);
		pool = new RdmaBufferPool(mockTransport, BUFFER_SIZE, POOL_COUNT);

		// Setup mock allocation to return unique direct buffers
		when(mockTransport.allocateNative(anyInt())).thenAnswer(invocation -> {
			int size = invocation.getArgument(0);
			return ByteBuffer.allocateDirect(size);
		});
	}

	@Test
	void testPoolInitialization() throws InterruptedException {
		pool.init();
		verify(mockTransport, times(POOL_COUNT)).allocateNative(BUFFER_SIZE);
		for (int i = 0; i < POOL_COUNT; i++) {
			assertNotNull(pool.acquire(0, TimeUnit.MILLISECONDS));
		}
		assertNull(pool.acquire(0, TimeUnit.MILLISECONDS));
	}

	@Test
	void testAcquireReleaseLifecycle() throws InterruptedException {
		pool.init();
		ByteBuffer buf1 = pool.acquire(0, TimeUnit.MILLISECONDS);
		pool.release(buf1);
		// Drain the others
		for (int i = 0; i < POOL_COUNT - 1; i++) {
			pool.acquire(0, TimeUnit.MILLISECONDS);
		}
		// The last one should be buf1
		ByteBuffer buf2 = pool.acquire(0, TimeUnit.MILLISECONDS);
		assertSame(buf1, buf2);
	}

	@Test
	void testReleaseMismatchedBuffer() {
		pool.init();
		ByteBuffer oddBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE * 2);
		pool.release(oddBuffer);
		verify(mockTransport).freeNative(oddBuffer);
	}

	// ---------- P1.2: identity-based pool ownership ----------

	@Test
	void testRelease_rejectsSameSizeNonPoolBuffer() {
		// A non-pool buffer with the SAME capacity must NOT enter the pool
		pool.init();
		ByteBuffer impostor = ByteBuffer.allocateDirect(BUFFER_SIZE);
		assertFalse(pool.isPoolBuffer(impostor));
		pool.release(impostor);
		// Should be freed, not returned to pool
		verify(mockTransport).freeNative(impostor);
	}

	@Test
	void testIsPoolBuffer_trueForPoolBuffer() throws InterruptedException {
		pool.init();
		ByteBuffer buf = pool.acquire(0, TimeUnit.MILLISECONDS);
		assertNotNull(buf);
		assertTrue(pool.isPoolBuffer(buf));
	}

	@Test
	void testIsPoolBuffer_falseForNull() {
		assertFalse(pool.isPoolBuffer(null));
	}

	@Test
	void testIsPoolBuffer_falseAfterClose() throws InterruptedException {
		pool.init();
		ByteBuffer buf = pool.acquire(0, TimeUnit.MILLISECONDS);
		pool.release(buf);
		pool.close();
		// After close, owned set is cleared
		assertFalse(pool.isPoolBuffer(buf));
	}

	@Test
	void testCloseFreesBuffers() {
		pool.init();
		pool.close();
		// All 5 should be freed
		verify(mockTransport, times(POOL_COUNT)).freeNative(any(ByteBuffer.class));
	}
}
