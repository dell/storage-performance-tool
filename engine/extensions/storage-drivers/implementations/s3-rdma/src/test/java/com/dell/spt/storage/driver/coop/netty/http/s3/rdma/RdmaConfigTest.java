package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RdmaConfigTest {

	@Test
	void testConstructorWithAllFields() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, 128, 33554432, "mlx5_0", "10.0.0.1", "INFO");
		assertTrue(config.isEnabled());
		assertEquals(1_048_576L, config.getThresholdBytes());
		assertTrue(config.isFallbackEnabled());
		assertEquals(128, config.getPoolSize());
		assertEquals(33554432, config.getBufferSize());
		assertEquals("mlx5_0", config.getDevice());
		assertEquals("10.0.0.1", config.getLocalIp());
		assertEquals("INFO", config.getLogLevel());
	}

	@Test
	void testDisabledConfig() {
		final var config = new RdmaConfig(
						false, 0L, false, 0, 0, "auto", "", "WARN");
		assertFalse(config.isEnabled());
		assertEquals(0L, config.getThresholdBytes());
		assertFalse(config.isFallbackEnabled());
	}

	@Test
	void testThresholdBoundary() {
		final var config = new RdmaConfig(
						true, 0L, true, 128, 33554432, "auto", "", "WARN");
		assertEquals(0L, config.getThresholdBytes());
	}

	@Test
	void testLargeThreshold() {
		final long oneGb = 1_073_741_824L;
		final var config = new RdmaConfig(
						true, oneGb, true, 128, 33554432, "auto", "", "WARN");
		assertEquals(oneGb, config.getThresholdBytes());
	}

	@Test
	void testToString() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, 128, 33554432, "auto", "", "WARN");
		final String str = config.toString();
		assertNotNull(str);
		assertTrue(str.contains("enabled=true"));
		assertTrue(str.contains("thresholdBytes=1048576"));
		assertTrue(str.contains("fallbackEnabled=true"));
		assertTrue(str.contains("poolSize=128"));
	}

	@Test
	void testEmptyLocalIp() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, 128, 33554432, "auto", "", "WARN");
		assertEquals("", config.getLocalIp());
	}

	@Test
	void testAutoDevice() {
		final var config = new RdmaConfig(
						true, 1_048_576L, true, 128, 33554432, "auto", "", "WARN");
		assertEquals("auto", config.getDevice());
	}
}
