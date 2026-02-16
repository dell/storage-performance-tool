package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RdmaExceptionTest {

	@Test
	void testConstructorAndGetters() {
		final var ex = new RdmaException("RDMA READ failed", 12, "ENOMEM");
		assertEquals(12, ex.getErrno());
		assertEquals("ENOMEM", ex.getErrorName());
	}

	@Test
	void testMessageFormat() {
		final var ex = new RdmaException("ibv_reg_mr failed", 22, "EINVAL");
		assertEquals("ibv_reg_mr failed (errno=22: EINVAL)", ex.getMessage());
	}

	@Test
	void testIsRuntimeException() {
		final var ex = new RdmaException("test", 0, "SUCCESS");
		assertInstanceOf(RuntimeException.class, ex);
	}

	@Test
	void testZeroErrno() {
		final var ex = new RdmaException("Success", 0, "SUCCESS");
		assertEquals(0, ex.getErrno());
		assertEquals("SUCCESS", ex.getErrorName());
		assertTrue(ex.getMessage().contains("errno=0"));
	}

	@Test
	void testNegativeErrno() {
		final var ex = new RdmaException("Unknown error", -1, "UNKNOWN");
		assertEquals(-1, ex.getErrno());
		assertTrue(ex.getMessage().contains("errno=-1"));
	}

	@Test
	void testNullErrorName() {
		final var ex = new RdmaException("error", 5, null);
		assertEquals(5, ex.getErrno());
		assertNull(ex.getErrorName());
		assertTrue(ex.getMessage().contains("errno=5: null"));
	}

	@Test
	void testEmptyMessage() {
		final var ex = new RdmaException("", 19, "ENODEV");
		assertEquals("", ex.getMessage().substring(0, 0));
		assertTrue(ex.getMessage().contains("errno=19: ENODEV"));
	}

	@Test
	void testCommonRdmaErrors() {
		// ENODEV - No such device (errno 19) — common when RDMA hardware missing
		final var enodev = new RdmaException("Failed to open RDMA device", 19, "ENODEV");
		assertEquals(19, enodev.getErrno());
		assertEquals("ENODEV", enodev.getErrorName());

		// ETIMEDOUT - Connection timed out (errno 110)
		final var etimedout = new RdmaException("RDMA operation timed out", 110, "ETIMEDOUT");
		assertEquals(110, etimedout.getErrno());

		// EPERM - Operation not permitted (errno 1)
		final var eperm = new RdmaException("Memory registration denied", 1, "EPERM");
		assertEquals(1, eperm.getErrno());
	}
}
