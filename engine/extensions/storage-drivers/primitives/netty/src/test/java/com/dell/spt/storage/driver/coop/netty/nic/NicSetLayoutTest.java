package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.nic.NicCode;
import com.dell.spt.base.nic.NicPlan;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

class NicSetLayoutTest {

	private static final String HOSTNAME_ENDPOINT = "s3-a.example:9000";

	@Test
	void layoutKeepsTheNicSetOrderAndFreezesEachDestination() {
		final var layout = NicSetLayout.from(plan(List.of()));

		assertEquals(List.of("ens2np1", "ens2np0"), layout.nics().stream().map(NicSetLayout.Nic::name).toList(),
						"the list order, not the inventory order");
		final var first = layout.nics().get(0);
		assertEquals("198.51.100.10", first.address().getHostAddress());
		assertEquals(List.of(new NicSetLayout.PathSpec("198.51.100.20:9000", new InetSocketAddress("198.51.100.20", 9000))),
						first.paths());

		final var second = layout.nics().get(1);
		assertEquals("192.0.2.10", second.address().getHostAddress());
		assertEquals(List.of("192.0.2.20:9000", HOSTNAME_ENDPOINT),
						second.paths().stream().map(NicSetLayout.PathSpec::endpoint).toList());
		final var hostname = second.paths().get(1).destination();
		assertEquals("192.0.2.21", hostname.getAddress().getHostAddress(), "resolved once, at plan time");
		assertEquals(9000, hostname.getPort());
		assertFalse(hostname.isUnresolved());
	}

	@Test
	void aFailedPlanHasNoLayout() {
		final var failed = plan(List.of(
						new NicPlan.Finding(NicCode.NIC_DOWN, "NIC ens2np0 is down", "ens2np0", null, "bring it up"),
						new NicPlan.Finding(NicCode.ENDPOINT_UNSERVED, "no chosen NIC serves 192.0.2.30:9000", null,
										"192.0.2.30:9000", null)));

		assertEquals("NIC ens2np0 is down (bring it up); no chosen NIC serves 192.0.2.30:9000",
						NicSetLayout.failureMessage(failed));
		final var thrown = assertThrows(IllegalArgumentException.class, () -> NicSetLayout.from(failed));
		assertTrue(thrown.getMessage().contains("NIC ens2np0 is down"), thrown.getMessage());
	}

	@Test
	void anEndpointWithoutAnAddressOrPortIsRejected() {
		final var noAddress = new NicPlan(List.of("ens2np0"), false, false, List.of("ens2np0"),
						List.of(nic("ens2np0", "192.0.2.10")),
						List.of(endpoint("192.0.2.20:9000", null)),
						List.of(), List.of(group("ens2np0", "192.0.2.20:9000")), List.of(), List.of());
		assertThrows(IllegalArgumentException.class, () -> NicSetLayout.from(noAddress));

		final var noPort = new NicPlan(List.of("ens2np0"), false, false, List.of("ens2np0"),
						List.of(nic("ens2np0", "192.0.2.10")),
						List.of(endpoint("192.0.2.20", "192.0.2.20")),
						List.of(), List.of(group("ens2np0", "192.0.2.20")), List.of(), List.of());
		assertThrows(IllegalArgumentException.class, () -> NicSetLayout.from(noPort));
	}

	private static NicPlan plan(final List<NicPlan.Finding> failures) {
		final var nics = List.of(
						nic("ens2np0", "192.0.2.10"),
						nic("ens2np1", "198.51.100.10"),
						new NicPlan.PlannedNic("eth0", false, null, null, List.of(),
										new NicPlan.Reason(NicCode.NOT_LISTED, null), 0));
		final var endpoints = List.of(
						endpoint("192.0.2.20:9000", "192.0.2.20"),
						endpoint(HOSTNAME_ENDPOINT, "192.0.2.21"),
						endpoint("198.51.100.20:9000", "198.51.100.20"));
		final var groups = List.of(
						group("ens2np1", "198.51.100.20:9000"),
						new NicPlan.PairingGroup(List.of("ens2np0"), List.of("192.0.2.20:9000", HOSTNAME_ENDPOINT),
										List.of(new NicPlan.Path("ens2np0", "192.0.2.20:9000"),
														new NicPlan.Path("ens2np0", HOSTNAME_ENDPOINT))));
		return new NicPlan(List.of("ens2np1", "ens2np0"), false, false, List.of("ens2np1", "ens2np0"), nics, endpoints,
						List.of(), groups, failures, List.of());
	}

	private static NicPlan.PlannedNic nic(final String name, final String address) {
		return new NicPlan.PlannedNic(name, true, address, null, List.of(), null, 1);
	}

	private static NicPlan.PlannedEndpoint endpoint(final String endpoint, final String address) {
		return new NicPlan.PlannedEndpoint(endpoint, address, List.of(), null, List.of(), List.of());
	}

	private static NicPlan.PairingGroup group(final String nic, final String endpoint) {
		return new NicPlan.PairingGroup(List.of(nic), List.of(endpoint), List.of(new NicPlan.Path(nic, endpoint)));
	}
}
