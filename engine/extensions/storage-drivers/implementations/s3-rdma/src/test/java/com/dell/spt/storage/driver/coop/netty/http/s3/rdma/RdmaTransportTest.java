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
						true, 1_048_576L, true, "auto", "", "WARN");
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
	void testPutObjectReturnsNegativeInStubMode() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		final int status = transport.putObject("bucket", "key", buf, 1024);
		assertEquals(-1, status);
	}

	@Test
	void testGetObjectReturnsNegativeInStubMode() {
		final ByteBuffer buf = transport.allocateBuffer(1024);
		final int status = transport.getObject("bucket", "key", buf, 1024);
		assertEquals(-1, status);
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
}
