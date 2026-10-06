package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class RdmaLocalAddressTest {

	@Test
	void loopbackRouteIsNotUsedAsRdmaAddress() {
		assertEquals("", S3RdmaStorageDriver.routedLocalAddress("127.0.0.1", 9020));
	}

	@Test
	void unresolvableHostYieldsNoAddress() {
		assertEquals("", S3RdmaStorageDriver.routedLocalAddress("no-such-host.invalid", 9020));
	}

	@Test
	void routedAddressIsEmptyOrALocalNonLoopbackIpv4() throws Exception {
		// TEST-NET-1: the result depends on the host's routes, but it is never a loopback address.
		final String address = S3RdmaStorageDriver.routedLocalAddress("192.0.2.1", 9020);
		if (!address.isEmpty()) {
			final InetAddress parsed = InetAddress.getByName(address);
			assertFalse(parsed.isLoopbackAddress());
			assertTrue(address.matches("\\d+\\.\\d+\\.\\d+\\.\\d+"));
		}
	}

	@Test
	void withLocalIpReplacesOnlyTheAddress() {
		final var config = new RdmaConfig(true, 1024, false, "mlx5_bond_0", "", "WARN", 15_000);
		final var routed = config.withLocalIp("10.0.0.5");
		assertEquals("10.0.0.5", routed.getLocalIp());
		assertEquals("", config.getLocalIp());
		assertEquals(config.isEnabled(), routed.isEnabled());
		assertEquals(config.getThresholdBytes(), routed.getThresholdBytes());
		assertEquals(config.isFallbackEnabled(), routed.isFallbackEnabled());
		assertEquals(config.getDevice(), routed.getDevice());
		assertEquals(config.getLogLevel(), routed.getLogLevel());
		assertEquals(config.getTimeoutMs(), routed.getTimeoutMs());
	}
}
