package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class RdmaTransportTest {

	private RdmaConfig config;
	private RdmaTransport transport;

	@BeforeEach
	void setUp() {
		config = new RdmaConfig(
						true, 1_048_576L, true, 0, 0, "auto", "", "WARN");
		transport = new RdmaTransport(config);
	}

	@AfterEach
	void tearDown() {
		transport.close();
	}

	@Test
	void testInitReturnsFalseInStubMode() {
		final boolean result = transport.init("http://localhost:9020", "access", "secret");
		assertFalse(result, "Stub init should return false (no native lib)");
	}

	@Test
	void testIsAvailableFalseAfterFailedInit() {
		transport.init("http://localhost:9020", "access", "secret");
		assertFalse(transport.isAvailable());
	}

	@Test
	void testIsAvailableFalseBeforeInit() {
		assertFalse(transport.isAvailable());
	}

	@Test
	void testAllocateBufferReturnsDirect() {
		final ByteBuffer buf = transport.allocateBuffer(4096);
		assertNotNull(buf);
		assertTrue(buf.isDirect(), "Buffer should be direct ByteBuffer");
		assertEquals(4096, buf.capacity());
	}

	@Test
	void testAllocateBufferSmallSize() {
		final ByteBuffer buf = transport.allocateBuffer(1);
		assertNotNull(buf);
		assertEquals(1, buf.capacity());
	}

	@Test
	void testAllocateBufferLargeSize() {
		final int tenMb = 10 * 1024 * 1024;
		final ByteBuffer buf = transport.allocateBuffer(tenMb);
		assertNotNull(buf);
		assertEquals(tenMb, buf.capacity());
	}

	@Test
	void testFreeBufferDoesNotThrow() {
		final ByteBuffer buf = transport.allocateBuffer(4096);
		assertDoesNotThrow(() -> transport.freeBuffer(buf));
	}

	@Test
	void testFreeBufferNull() {
		assertDoesNotThrow(() -> transport.freeBuffer(null));
	}

	@Test
	void testRegisterBufferReturnsZeroInStubMode() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		// In stub mode (no native library), registerBuffer should return 0
		final long mrHandle = transport.registerBuffer(buf, 1024);
		assertEquals(0, mrHandle);
	}

	@Test
	void testDeregisterBufferDoesNotThrow() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		// Even with mrHandle=0, this should not throw
		assertDoesNotThrow(() -> transport.deregisterBuffer(buf, 0));
	}

	@Test
	void testGenerateTokenReturnsNullInStubMode() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		// In stub mode, generateToken should return null
		final String token = transport.generateToken(0, 1024);
		assertNull(token);
	}

	@Test
	void testCloseIsIdempotent() {
		transport.close();
		assertDoesNotThrow(() -> transport.close());
	}

	@Test
	void testCloseAfterInit() {
		transport.init("http://localhost:9020", "access", "secret");
		assertDoesNotThrow(() -> transport.close());
		assertFalse(transport.isAvailable());
	}

	@Test
	void testIsRdmaHardwareAvailableFalseInStubMode() {
		// Without the native library, this should return false
		assertFalse(transport.isRdmaHardwareAvailable());
	}

	@Test
	void testIsNativeAvailable() {
		// Static method check - will be false without native library
		assertFalse(RdmaTransport.isNativeAvailable());
	}

	@Test
	void testRegisterBufferWithZeroSize() {
		final ByteBuffer buf = transport.allocateBuffer(0);
		// registerBuffer with 0 size should return 0 (not initialized)
		final long mrHandle = transport.registerBuffer(buf, 0);
		assertEquals(0, mrHandle);
	}

	@Test
	void testGenerateTokenWithZeroMrHandle() {
		// generateToken with 0 mrHandle should return null
		final String token = transport.generateToken(0, 1024);
		assertNull(token);
	}

	@Test
	void testMultipleAllocateFree() {
		// Test multiple allocations and frees
		final ByteBuffer buf1 = transport.allocateBuffer(1024);
		final ByteBuffer buf2 = transport.allocateBuffer(2048);
		final ByteBuffer buf3 = transport.allocateBuffer(4096);

		assertNotNull(buf1);
		assertNotNull(buf2);
		assertNotNull(buf3);

		assertDoesNotThrow(() -> transport.freeBuffer(buf1));
		assertDoesNotThrow(() -> transport.freeBuffer(buf2));
		assertDoesNotThrow(() -> transport.freeBuffer(buf3));
	}

	@Test
	void testFreeBufferTwice() {
		final ByteBuffer buf = transport.allocateBuffer(4096);
		transport.freeBuffer(buf);
		// Second free should not throw
		assertDoesNotThrow(() -> transport.freeBuffer(buf));
	}

	@Test
	void testInitWithNullEndpoint() {
		final boolean result = transport.init(null, "access", "secret");
		assertFalse(result);
	}

	@Test
	void testInitWithEmptyEndpoint() {
		final boolean result = transport.init("", "access", "secret");
		assertFalse(result);
	}

	@Test
	void testInitWithNullCredentials() {
		final boolean result = transport.init("http://localhost:9020", null, null);
		assertFalse(result);
	}

	@Test
	void testGetMrHandleReturnsZeroForUnregistered() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		// Buffer was never registered, should return 0
		final long mrHandle = transport.getMrHandle(buf);
		assertEquals(0, mrHandle);
	}

	@Test
	void testGetMrHandleNull() {
		assertEquals(0, transport.getMrHandle(null));
	}

	@Test
	void testAllocateNativeReturnsNullInStubMode() {
		// In stub mode (not initialized), allocateNative should return null
		assertNull(transport.allocateNative(1024));
	}

	@Test
	void testFreeNativeDoesNotThrowForNull() {
		assertDoesNotThrow(() -> transport.freeNative(null));
	}
}
