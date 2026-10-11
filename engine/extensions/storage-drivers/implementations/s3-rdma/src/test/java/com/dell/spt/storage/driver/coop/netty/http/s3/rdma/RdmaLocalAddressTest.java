package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RdmaLocalAddressTest {

	/** TEST-NET-1: never contacted; constructing the driver only performs a route lookup. */
	private static final String TEST_NET_ENDPOINT = "192.0.2.1";
	private static final int PORT = 9020;

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
	void autoDeviceUsesTheAddressRoutedToTheEndpoint() throws Exception {
		final RdmaConfig effective = effectiveConfig("auto", "");
		assertEquals(S3RdmaStorageDriver.routedLocalAddress(TEST_NET_ENDPOINT, PORT), effective.getLocalIp());
	}

	@Test
	void explicitDeviceIsNotOverriddenByAnInferredAddress() throws Exception {
		// The native layer binds by address first; an inferred address could select another NIC.
		assertEquals("", effectiveConfig("mlx5_1", "").getLocalIp());
	}

	@Test
	void explicitLocalAddressIsKept() throws Exception {
		assertEquals("10.1.2.3", effectiveConfig("auto", "10.1.2.3").getLocalIp());
		assertEquals("10.1.2.3", effectiveConfig("mlx5_1", "10.1.2.3").getLocalIp());
	}

	@Test
	void explicitDeviceDetection() {
		assertTrue(new RdmaConfig(true, 0, false, "mlx5_0", "", "WARN", 1).hasExplicitDevice());
		assertFalse(new RdmaConfig(true, 0, false, "auto", "", "WARN", 1).hasExplicitDevice());
		assertFalse(new RdmaConfig(true, 0, false, "", "", "WARN", 1).hasExplicitDevice());
	}

	private static RdmaConfig effectiveConfig(final String device, final String localIp) throws Exception {
		final var requested = new RdmaConfig(true, 1024, false, device, localIp, "WARN", 15_000);
		final AtomicReference<RdmaConfig> seen = new AtomicReference<>();
		try {
			S3RdmaStorageDriverTestSupport.newDriver(requested, effective -> {
				seen.set(effective);
				return new FakeRdmaTransport(effective);
			}, "/bucket", List.of(TEST_NET_ENDPOINT), PORT);
			return seen.get();
		} finally {
			S3RdmaStorageDriverTestSupport.closeCreatedDrivers();
		}
	}

	@Test
	void withLocalIpReplacesOnlyTheAddress() {
		final var config = new RdmaConfig(true, 1024, false, "mlx5_bond_0", "", "WARN", 15_000, true);
		final var routed = config.withLocalIp("10.0.0.5");
		assertTrue(routed.isAllowMissingBytesHeader());
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
