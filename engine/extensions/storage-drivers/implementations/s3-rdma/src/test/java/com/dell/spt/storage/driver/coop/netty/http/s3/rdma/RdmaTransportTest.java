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
	void testRegisterBufferReturnsZeroInStubMode() {
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		// In stub mode (no native library), registerBuffer should return 0
		final long mrHandle = transport.registerBuffer(buf, 1024);
		assertEquals(0, mrHandle);
	}

	@Test
	void testDeregisterBufferDoesNotThrow() {
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		// Even with mrHandle=0, this should not throw
		assertDoesNotThrow(() -> transport.deregisterBuffer(buf, 0));
	}

	@Test
	void testGenerateTokenReturnsNullInStubMode() {
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
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
		final ByteBuffer buf = ByteBuffer.allocateDirect(0);
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

}
