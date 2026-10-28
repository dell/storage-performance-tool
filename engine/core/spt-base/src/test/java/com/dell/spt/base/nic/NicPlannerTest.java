package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.nic.NicPlan.Finding;
import com.dell.spt.base.nic.NicPlan.PlannedEndpoint;
import com.dell.spt.base.nic.NicPlan.PlannedNic;
import com.dell.spt.base.nic.NicPlan.Reason;
import com.dell.spt.base.nic.NicPlan.Reduction;
import com.dell.spt.base.nic.NicPlan.SharedPort;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The planner over fixture hosts: items 2, 4, 5, 6 to 6b and 7a. Pairing is in {@link NicPairingTest}. */
class NicPlannerTest {

	private static final String E1 = "192.0.2.101:9000";
	private static final String E2 = "192.0.2.102:9000";
	private static final String B1 = "198.51.100.101:9000";
	private static final List<String> AUTO = List.of(NicPlanner.AUTO);

	/** Literal IPv4 hosts resolve to themselves; these names to fixed answers. */
	private static final Map<String, String> NAMES = Map.of("s3a.example", "192.0.2.101", "s3b.example", "192.0.2.102");

	static final EndpointResolver RESOLVER = host -> {
		final var literal = Ipv4.parse(host);
		return literal != null ? literal : NAMES.containsKey(host) ? Ipv4.parse(NAMES.get(host)) : null;
	};

	@TempDir
	Path root;

	private FakeHost host;

	@BeforeEach
	void createHost() {
		host = new FakeHost(root);
	}

	// ---------------------------------------------------------------- auto eligibility (item 4)

	@Test
	void autoExclusionReasons() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("eno1", "0000:01:00.0", "192.0.2.12/24", 400000).down();
		host.virtual("docker0").address("192.0.2.200/24");
		host.physical("wlp1s0", "0000:05:00.0").address("192.0.2.13/24").wireless();
		nic("ens3f0np0", "0000:3d:00.0", null, 400000);
		nic("ens3f1np1", "0000:3d:00.1", null, 400000);
		host.bond("bond0", "ens3f0np0", "ens3f1np1").address("198.51.100.40/24");
		nic("eno2", "0000:02:00.0", "198.51.100.10/24", 1000);
		nic("ens4", "0000:41:00.0", "192.0.2.14/24", 400000).address("192.0.2.15/24");
		nic("ens6", "0000:61:00.0", "203.0.113.16/24", 400000).address("192.0.2.16/24");

		final var plan = plan(AUTO, List.of(E1, E2));

		assertEquals(List.of("ens2np0", "ens6"), plan.nicSet());
		assertEquals("192.0.2.16", planned(plan, "ens6").address(), "only the address that reaches an endpoint counts");
		assertReason(plan, "eno1", NicCode.INTERFACE_DOWN);
		assertReason(plan, "docker0", NicCode.VIRTUAL);
		assertReason(plan, "wlp1s0", NicCode.NOT_ETHERNET);
		assertReason(plan, "ens3f0np0", NicCode.BOND_MEMBER);
		assertReason(plan, "bond0", NicCode.NO_ENDPOINT_ON_SUBNET);
		assertReason(plan, "eno2", NicCode.NO_ENDPOINT_ON_SUBNET);
		assertReason(plan, "ens4", NicCode.ADDRESS_AMBIGUOUS);
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	@Test
	void autoRdmaExclusionReasons() {
		connectX("ens2np0", "0000:17:00.0", "mlx5_0", "192.0.2.11");
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);
		// A non-mlx5 device at the top speed tier: excluded for DC, not hidden by speed.
		nic("ens4np0", "0000:19:00.0", "192.0.2.13/24", 400000);
		host.rdma("irdma0", "0000:19:00.0", "ice").roceV2(3, "192.0.2.13", "ens4np0").probe(false, 4096);
		nic("ens5np0", "0000:1a:00.0", "192.0.2.14/24", 400000);
		host.rdma("mlx5_3", "0000:1a:00.0", "mlx5_core").roceV2(3, "192.0.2.14", "ens5np0").probe(false, 4096);
		nic("ens6np0", "0000:1b:00.0", "192.0.2.15/24", 400000);
		host.rdma("mlx5_4", "0000:1b:00.0", "mlx5_core").port(1, "DOWN", "Ethernet").roceV2(3, "192.0.2.15", "ens6np0");
		nic("ens7np0", "0000:1c:00.0", "192.0.2.16/24", 400000);
		host.rdma("mlx5_5", "0000:1c:00.0", "mlx5_core").gid(1, 2, "192.0.2.16", "IB/RoCE v1", "ens7np0");
		nic("ens8np0", "0000:1d:00.0", "192.0.2.17/24", 400000);
		host.rdma("mlx5_6", "0000:1d:00.0", "mlx5_core").port(2, "ACTIVE", "Ethernet").gid(2, 3, "192.0.2.17", "RoCE v2", "ens8np0");
		nic("ens9np0", "0000:1e:00.0", "192.0.2.18/24", 400000);
		host.rdma("mlx5_7", "0000:1e:00.0", "mlx5_core").port(1, "ACTIVE", "InfiniBand").roceV2(3, "192.0.2.18", "ens9np0");

		final var plan = plan(AUTO, List.of(E1), true);

		assertEquals(List.of("ens2np0"), plan.nicSet());
		assertEquals(3, planned(plan, "ens2np0").gidIndex());
		assertReason(plan, "ens3np0", NicCode.NO_RDMA_DEVICE);
		assertReason(plan, "ens4np0", NicCode.RDMA_NO_DC);
		assertReason(plan, "ens5np0", NicCode.RDMA_NO_DC);
		assertReason(plan, "ens6np0", NicCode.RDMA_PORT_NOT_ACTIVE);
		assertReason(plan, "ens7np0", NicCode.NO_ROCEV2_GID);
		assertReason(plan, "ens8np0", NicCode.ROCEV2_GID_NOT_PORT_1);
		assertReason(plan, "ens9np0", NicCode.RDMA_PORT_NOT_ACTIVE);
		assertTrue(plan.warnings().isEmpty(), plan.warnings()::toString);
	}

	@Test
	void withoutRdmaTheRdmaChecksDoNotApply() {
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);
		nic("ens4np0", "0000:19:00.0", "192.0.2.13/24", 400000);
		host.rdma("bnxt_re0", "0000:19:00.0", "bnxt_en").probe(false, 1024);

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("ens3np0", "ens4np0"), plan.nicSet());
		assertNull(planned(plan, "ens4np0").gidIndex());
		assertTrue(plan.warnings().stream().noneMatch(finding -> finding.code() == NicCode.ROCE_MTU_LOW));
	}

	@Test
	void unknownDcAndLowMtuWarn() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core").roceV2(3, "192.0.2.11", "ens2np0").probe(null, 1024);

		final var plan = plan(AUTO, List.of(E1), true);

		assertEquals(List.of("ens2np0"), plan.nicSet(), "an unknown DC answer does not exclude an mlx5 device");
		assertEquals(List.of(NicCode.ROCE_MTU_LOW, NicCode.RDMA_DC_UNKNOWN), codes(plan.warnings()));
		assertEquals(NicPlanner.ROCE_MTU_FIX, plan.warnings().get(0).fix());
		assertTrue(plan.ok());
	}

	@Test
	void roceLagBondIsEligibleForRdma() {
		nic("ens3f0np0", "0000:3d:00.0", null, 200000);
		nic("ens3f1np1", "0000:3d:00.1", null, 200000);
		host.bond("bond0", "ens3f0np0", "ens3f1np1").address("192.0.2.40/24").speed(400000);
		host.rdma("mlx5_bond_0", "0000:3d:00.0", "mlx5_core").roceV2(3, "192.0.2.40", "bond0").probe(true, 4096);

		final var auto = plan(AUTO, List.of(E1), true);
		final var listed = plan(List.of("mlx5_bond_0"), List.of(E1), true);

		assertEquals(List.of("bond0"), auto.nicSet());
		assertReason(auto, "ens3f0np0", NicCode.BOND_MEMBER);
		assertEquals(List.of("bond0"), listed.nicSet(), "the LAG device names the bond, not its first member");
		assertTrue(listed.ok(), listed.failures()::toString);
	}

	// ---------------------------------------------------------------- auto ordering and counts

	@Test
	void autoOrdersByNameNaturallyAndAListKeepsItsOrder() {
		nic("ens10np0", "0000:3d:00.0", "192.0.2.12/24", 400000);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		assertEquals(List.of("ens2np0", "ens10np0"), plan(AUTO, List.of(E1)).nicSet());
		assertEquals(List.of("ens10np0", "ens2np0"), plan(List.of("ens10np0", "ens2np0"), List.of(E1)).nicSet());
		assertEquals(List.of("ens2np0", "ens10np0"), plan(List.of("Auto"), List.of(E1)).nicSet(), "auto in any case");
	}

	@Test
	void autoWithNoEligibleNic() {
		nic("eno2", "0000:02:00.0", "198.51.100.10/24", 1000);
		host.loopback();

		final var plan = plan(AUTO, List.of(E1, E2));

		assertTrue(plan.nicSet().isEmpty());
		assertEquals(List.of(NicCode.AUTO_NONE_ELIGIBLE), codes(plan.failures()), "coverage is not reported on top");
		assertTrue(plan.groups().isEmpty());
		assertFalse(plan.ok());
	}

	@Test
	void autoWithNoResolvedEndpointSaysSo() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(AUTO, List.of("nohost.example:9000"));

		assertEquals(List.of(NicCode.ENDPOINT_NO_IPV4, NicCode.AUTO_NONE_ELIGIBLE), codes(plan.failures()));
		assertTrue(plan.failures().get(1).text().contains("no endpoint has an IPv4 address"));
	}

	@Test
	void autoWithOneEligibleNic() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("eno2", "0000:02:00.0", "198.51.100.10/24", 1000);

		final var plan = plan(AUTO, List.of(E1, E2));

		assertEquals(List.of("ens2np0"), plan.nicSet());
		assertEquals(List.of(E1, E2), planned(plan, "ens2np0").serves());
		assertEquals(2, planned(plan, "ens2np0").paths());
		assertTrue(plan.ok());
	}

	@Test
	void autoMixedWithNames() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(List.of("auto", "ens2np0"), List.of(E1));

		assertTrue(plan.auto());
		assertEquals(List.of(NicCode.AUTO_MIXED), codes(plan.failures()));
		assertTrue(plan.nicSet().isEmpty());
		assertEquals(List.of(E1), plan.endpoints().stream().map(PlannedEndpoint::endpoint).toList());
	}

	@Test
	void anEmptySettingIsNotAPlan() {
		assertThrows(IllegalArgumentException.class, () -> plan(List.of(), List.of(E1)));
	}

	// ---------------------------------------------------------------- speed tiers (D22, D28)

	@Test
	void slowerAndUnknownSpeedsAreDroppedBesideAKnownTier() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 200000);
		host.virtio("ens18", "0000:00:12.0").address("192.0.2.13/24");

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("ens2np0"), plan.nicSet());
		assertReason(plan, "ens3np0", NicCode.SLOWER_SPEED_TIER);
		assertReason(plan, "ens18", NicCode.SPEED_UNKNOWN_BESIDE_KNOWN);
		final var endpoint = plan.endpoints().get(0);
		assertEquals(400000, endpoint.tierSpeedMbps());
		assertEquals(List.of("ens2np0", "ens3np0", "ens18"), endpoint.candidates());
		assertEquals(List.of("ens2np0"), endpoint.kept());
		assertEquals(
						List.of(new Reduction("ens3np0", NicCode.SLOWER_SPEED_TIER, null), new Reduction("ens18", NicCode.SPEED_UNKNOWN_BESIDE_KNOWN, null)),
						endpoint.reductions());
	}

	@Test
	void everyCandidateOfUnknownSpeedIsOneTier() {
		host.virtio("ens18", "0000:00:12.0").address("192.0.2.13/24");
		host.virtio("ens19", "0000:00:13.0").address("192.0.2.14/24");

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("ens18", "ens19"), plan.nicSet());
		assertNull(plan.endpoints().get(0).tierSpeedMbps());
		assertTrue(plan.endpoints().get(0).reductions().isEmpty());
	}

	@Test
	void eachEndpointHasItsOwnTier() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "198.51.100.11/24", 200000);

		final var plan = plan(AUTO, List.of(E1, B1));

		assertEquals(List.of("ens2np0", "ens3np0"), plan.nicSet());
		assertEquals(400000, plan.endpoints().get(0).tierSpeedMbps());
		assertEquals(200000, plan.endpoints().get(1).tierSpeedMbps());
		assertEquals(List.of(B1), planned(plan, "ens3np0").serves());
		assertTrue(plan.ok());
	}

	@Test
	void aListWithMixedSpeedsWarns() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 200000);
		host.virtio("ens18", "0000:00:12.0").address("192.0.2.13/24");

		final var plan = plan(List.of("ens2np0", "ens3np0", "ens18"), List.of(E1));

		assertEquals(List.of("ens2np0", "ens3np0", "ens18"), plan.nicSet());
		assertEquals(List.of(NicCode.MIXED_SPEEDS), codes(plan.warnings()));
		assertNull(plan.endpoints().get(0).tierSpeedMbps());
		assertTrue(plan.ok());
	}

	// ---------------------------------------------------------------- shared ports (item 5, D39)

	@Test
	void autoKeepsTwoVlansOnOnePortThatReachDifferentEndpoints() {
		host.physical("ens2np0", "0000:17:00.0").speed(400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24").speed(400000);
		host.vlan("ens2np0.200", "ens2np0").address("198.51.100.21/24").speed(400000);

		final var plan = plan(AUTO, List.of(E1, B1));

		assertEquals(List.of("ens2np0.100", "ens2np0.200"), plan.nicSet());
		assertEquals(List.of(E1), planned(plan, "ens2np0.100").serves());
		assertEquals(List.of(B1), planned(plan, "ens2np0.200").serves());
		assertReason(plan, "ens2np0", NicCode.NO_ENDPOINT_ON_SUBNET);
		assertEquals(List.of(new SharedPort("0000:17:00.0/0", List.of("ens2np0", "ens2np0.100", "ens2np0.200"))), plan.ports());
		assertEquals(List.of(NicCode.SHARED_PORT_CHOSEN), codes(plan.warnings()));
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	@Test
	void autoKeepsTheFirstOfTwoVlansOnOnePortThatReachTheSameEndpoint() {
		host.physical("ens2np0", "0000:17:00.0").speed(400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24").speed(400000);
		host.vlan("ens2np0.200", "ens2np0").address("192.0.2.22/24").speed(400000);

		final var plan = plan(AUTO, List.of(E1, E2));

		assertEquals(List.of("ens2np0.100"), plan.nicSet());
		assertEquals(new Reason(NicCode.SHARES_PORT, "ens2np0.100"), planned(plan, "ens2np0.200").reason());
		assertEquals(List.of(E1, E2), planned(plan, "ens2np0.100").serves(), "every endpoint stays served");
		assertEquals(List.of(new Reduction("ens2np0.200", NicCode.SHARES_PORT, "ens2np0.100")), plan.endpoints().get(1).reductions());
		assertTrue(plan.ok());
	}

	@Test
	void aListFailsForTwoVlansOnOnePortThatReachTheSameEndpoint() {
		host.physical("ens2np0", "0000:17:00.0").speed(400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24");
		host.vlan("ens2np0.200", "ens2np0").address("192.0.2.22/24");

		final var plan = plan(List.of("ens2np0.100", "ens2np0.200"), List.of(E1));

		assertEquals(List.of(NicCode.SHARED_PORT_COMMON_ENDPOINT), codes(plan.failures()));
		assertEquals(E1, plan.failures().get(0).endpoint());
		assertEquals("ens2np0.200", plan.failures().get(0).nic());
	}

	@Test
	void aListMayHoldTwoVlansOnOnePortThatReachDifferentEndpoints() {
		host.physical("ens2np0", "0000:17:00.0");
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24");
		host.vlan("ens2np0.200", "ens2np0").address("198.51.100.21/24");

		final var plan = plan(List.of("ens2np0.100", "ens2np0.200"), List.of(E1, B1));

		assertTrue(plan.ok(), plan.failures()::toString);
		assertEquals(List.of(NicCode.SHARED_PORT_CHOSEN), codes(plan.warnings()));
	}

	@Test
	void autoPrefersAParentOverItsVlanOnACommonEndpoint() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24").speed(400000);

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("ens2np0"), plan.nicSet());
		assertEquals(new Reason(NicCode.SHARES_PORT, "ens2np0"), planned(plan, "ens2np0.100").reason());
	}

	@Test
	void sharingAPortOutranksSpeedInTheReason() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		host.vlan("ens2np0.100", "ens2np0").address("192.0.2.21/24").speed(400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);
		host.vlan("ens3np0.100", "ens3np0").address("192.0.2.22/24").speed(200000);

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("ens2np0", "ens3np0"), plan.nicSet());
		assertEquals(NicCode.SHARES_PORT, planned(plan, "ens3np0.100").reason().code());
	}

	// ---------------------------------------------------------------- explicit lists (item 2)

	@Test
	void entriesResolveByNameAddressAndRdmaDevice() {
		connectX("ens2np0", "0000:17:00.0", "mlx5_0", "192.0.2.11");
		host.vlan("ens2np0.100", "ens2np0").address("198.51.100.11/24");
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);
		nic("ens4np0", "0000:19:00.0", "192.0.2.13/24", 400000);

		final var plan = plan(List.of("mlx5_0", "192.0.2.12", " ens4np0 "), List.of(E1));

		assertEquals(List.of("ens2np0", "ens3np0", "ens4np0"), plan.nicSet());
		assertEquals(List.of("mlx5_0", "192.0.2.12", "ens4np0"), plan.interfaces());
		assertEquals(new Reason(NicCode.NOT_LISTED, null), planned(plan, "ens2np0.100").reason());
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	@Test
	void multiAddressInterfaces() {
		nic("ens2np0", "0000:17:00.0", "203.0.113.11/24", 400000).address("192.0.2.11/24");
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000).address("192.0.2.13/24");
		nic("ens4np0", "0000:19:00.0", "203.0.113.14/24", 400000).address("203.0.113.15/24");

		final var byName = plan(List.of("ens2np0"), List.of(E1));
		final var byAddress = plan(List.of("192.0.2.13"), List.of(E1));
		final var twoOnSubnet = plan(List.of("ens3np0"), List.of(E1));
		final var noneOnSubnet = plan(List.of("ens4np0"), List.of(E1));

		assertEquals("192.0.2.11", planned(byName, "ens2np0").address(), "the one address on an endpoint's subnet");
		assertEquals("192.0.2.13", planned(byAddress, "ens3np0").address());
		assertEquals(List.of(NicCode.ENTRY_AMBIGUOUS, NicCode.ENDPOINT_UNSERVED), codes(twoOnSubnet.failures()));
		assertTrue(twoOnSubnet.failures().get(0).text().contains("more than one"));
		assertEquals(NicCode.ENTRY_AMBIGUOUS, noneOnSubnet.failures().get(0).code());
		assertTrue(noneOnSubnet.failures().get(0).text().contains("none"));
	}

	@Test
	void duplicatesAfterResolution() {
		connectX("ens2np0", "0000:17:00.0", "mlx5_0", "192.0.2.11");

		final var plan = plan(List.of("ens2np0", "192.0.2.11", "mlx5_0"), List.of(E1));

		assertEquals(List.of(NicCode.ENTRY_DUPLICATE, NicCode.ENTRY_DUPLICATE), codes(plan.failures()));
		assertEquals(List.of("ens2np0"), plan.nicSet());
	}

	@Test
	void ambiguousEntries() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.11/24", 400000);
		host.physical("ens5f0np0", "0000:31:00.0").address("192.0.2.21/24");
		host.physical("ens5f1np1", "0000:31:00.0").devPort(1).address("192.0.2.22/24");
		host.rdma("bnxt_re0", "0000:31:00.0", "bnxt_en");

		final var byAddress = plan(List.of("192.0.2.11"), List.of(E1));
		final var byDevice = plan(List.of("bnxt_re0"), List.of(E1));

		assertEquals(List.of(NicCode.ENTRY_AMBIGUOUS), codes(byAddress.failures()));
		assertTrue(byAddress.failures().get(0).text().contains("ens2np0, ens3np0"));
		assertEquals(List.of(NicCode.ENTRY_AMBIGUOUS), codes(byDevice.failures()));
	}

	@Test
	void ipv6UnknownAndEmptyEntries() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(List.of("fe80::1", "eth9", "203.0.113.9", "", "ens2np0"), List.of(E1));

		assertEquals(
						List.of(NicCode.ENTRY_IPV6, NicCode.ENTRY_UNRESOLVED, NicCode.ENTRY_UNRESOLVED, NicCode.ENTRY_UNRESOLVED),
						codes(plan.failures()));
		assertEquals(List.of("ens2np0"), plan.nicSet());
	}

	@Test
	void listedNicsThatCannotServe() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000).down();
		host.physical("ens3np0", "0000:18:00.0");
		nic("eno2", "0000:02:00.0", "198.51.100.10/24", 1000);

		final var plan = plan(List.of("ens2np0", "ens3np0", "eno2"), List.of(E1));

		assertEquals(List.of(NicCode.NIC_NO_IPV4, NicCode.NIC_DOWN, NicCode.NIC_SERVES_NO_ENDPOINT), codes(plan.failures()));
		assertEquals(List.of("ens2np0", "ens3np0", "eno2"), plan.nicSet());
	}

	@Test
	void aListedNicReachesByARouteThroughItsDevice() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "203.0.113.11/24", 400000);
		host.route("ens3np0", "198.51.100.0/24", "203.0.113.1").route("ens2np0", "198.51.100.0/25", null);

		final var plan = plan(List.of("ens2np0", "ens3np0"), List.of(E1, B1));

		assertEquals(List.of(E1), planned(plan, "ens2np0").serves(), "an on-link route of another subnet is not a gateway route");
		assertEquals(List.of(B1), planned(plan, "ens3np0").serves());
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	@Test
	void anEndpointNoListedNicServesFails() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(List.of("ens2np0"), List.of(E1, B1));

		assertEquals(List.of(NicCode.ENDPOINT_UNSERVED), codes(plan.failures()));
		assertEquals(B1, plan.failures().get(0).endpoint());
	}

	@Test
	void listedRdmaFailures() {
		nic("ens5f0np0", "0000:31:00.0", "192.0.2.21/24", 200000);
		host.rdma("bnxt_re0", "0000:31:00.0", "bnxt_en").roceV2(3, "192.0.2.21", "ens5f0np0").probe(false, 4096);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);

		final var plan = plan(List.of("ens5f0np0", "ens3np0"), List.of(E1), true);

		assertEquals(List.of(NicCode.RDMA_NO_DC, NicCode.NO_RDMA_DEVICE), codes(plan.failures()));
		assertEquals("ens5f0np0", plan.failures().get(0).nic());
	}

	// ---------------------------------------------------------------- host checks (items 6 to 6b, D17)

	@Test
	void arpIgnoreZeroFromAllFailsForTwoChosenNicsOnOneSubnet() {
		host.sysctlAll(0, 2, 2);
		twoOnOneSubnet();

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of(NicCode.ARP_FLUX, NicCode.ARP_FLUX), codes(plan.failures()));
		assertEquals(NicPlanner.ARP_FIX, plan.failures().get(0).fix());
	}

	@Test
	void arpIgnoreZeroFromTheInterfaceFails() {
		host.sysctlAll(0, 2, 2);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000).sysctl(1, 2, 0);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000).sysctl(0, 2, 0);

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of(NicCode.ARP_FLUX, NicCode.ARP_FLUX), codes(plan.failures()));
		assertTrue(plan.failures().get(0).text().endsWith("arp_ignore is 0 on ens3np0"), plan.failures().get(0)::text);
	}

	@Test
	void arpIgnoreZeroFailsForOneChosenNicBesideAnUnchosenInterface() {
		host.sysctlAll(0, 2, 2);
		twoOnOneSubnet();

		final var plan = plan(List.of("ens2np0"), List.of(E1));

		assertEquals(List.of(NicCode.ARP_FLUX), codes(plan.failures()));
		assertTrue(plan.failures().get(0).text().contains("shares its subnet with ens3np0"));
	}

	@Test
	void anUnchosenInterfaceThatIsDownDoesNotCount() {
		host.sysctlAll(0, 0, 1);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000).down();

		assertTrue(plan(List.of("ens2np0"), List.of(E1)).failures().isEmpty());
	}

	@Test
	void arpIgnoreOnePasses() {
		host.sysctlAll(0, 2, 2);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000).sysctl(1, 2, 0);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000).sysctl(1, 2, 0);

		final var plan = plan(AUTO, List.of(E1));

		assertTrue(plan.ok(), plan.failures()::toString);
		assertTrue(plan.warnings().isEmpty(), plan.warnings()::toString);
	}

	@Test
	void arpAnnounceBelowTwoWarnsOnly() {
		host.sysctlAll(1, 1, 2);
		twoOnOneSubnet();

		final var plan = plan(AUTO, List.of(E1));

		assertTrue(plan.ok(), plan.failures()::toString);
		assertEquals(List.of(NicCode.ARP_ANNOUNCE_LOW, NicCode.ARP_ANNOUNCE_LOW), codes(plan.warnings()));
	}

	@Test
	void strictRpFilterFailsAndLoosePasses() {
		host.sysctlAll(1, 2, 0);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000).sysctl(0, 0, 1);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000).sysctl(0, 0, 2);

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of(NicCode.RP_FILTER_STRICT), codes(plan.failures()));
		assertEquals("ens2np0", plan.failures().get(0).nic());
		assertEquals(NicPlanner.RP_FILTER_FIX, plan.failures().get(0).fix());
	}

	@Test
	void strictRpFilterFromAllFails() {
		host.sysctlAll(1, 2, 1);
		twoOnOneSubnet();

		assertEquals(List.of(NicCode.RP_FILTER_STRICT, NicCode.RP_FILTER_STRICT), codes(plan(AUTO, List.of(E1)).failures()));
	}

	@Test
	void nicsOnDifferentSubnetsNeverTriggerTheRules() {
		host.sysctlAll(0, 0, 1);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "198.51.100.11/24", 400000);

		final var plan = plan(AUTO, List.of(E1, B1));

		assertEquals(List.of("ens2np0", "ens3np0"), plan.nicSet());
		assertTrue(plan.ok(), plan.failures()::toString);
		assertTrue(plan.warnings().isEmpty(), plan.warnings()::toString);
	}

	@Test
	void aSubnetIsSharedWhenEitherAddressLiesInTheOthersPrefix() {
		host.sysctlAll(0, 2, 2);
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.200/25", 400000);

		final var plan = plan(List.of("ens2np0"), List.of(E1));

		assertEquals(List.of(NicCode.ARP_FLUX), codes(plan.failures()));
	}

	@Test
	void layerTwoBondHashingWarns() {
		nic("ens3f0np0", "0000:3d:00.0", null, 200000);
		nic("ens3f1np1", "0000:3d:00.1", null, 200000);
		host.bond("bond0", "ens3f0np0", "ens3f1np1").address("192.0.2.40/24").hashPolicy("layer2+3");

		final var plan = plan(AUTO, List.of(E1));

		assertEquals(List.of("bond0"), plan.nicSet());
		assertEquals(List.of(NicCode.BOND_HASH_LAYER2), codes(plan.warnings()));
		assertEquals(NicPlanner.BOND_HASH_FIX, plan.warnings().get(0).fix());
	}

	@Test
	void effectiveSysctlIsTheLargerValue() {
		assertEquals(1, NicPlanner.effective(1, 0));
		assertEquals(2, NicPlanner.effective(0, 2));
		assertEquals(0, NicPlanner.effective(null, null));
	}

	// ---------------------------------------------------------------- endpoint identity (item 7a, D42)

	@Test
	void aRepeatedEndpointStringFailsOnce() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(AUTO, List.of(E1, E2, E1, E1));

		assertEquals(List.of(NicCode.ENDPOINT_DUPLICATE), codes(plan.failures()));
		assertEquals(E1, plan.failures().get(0).endpoint());
		assertEquals(List.of(E1, E2), plan.endpoints().stream().map(PlannedEndpoint::endpoint).toList());
	}

	@Test
	void twoNamesWithOneAddressAndPortWarnAndCountTwice() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(AUTO, List.of("s3a.example:9000", E1));

		assertTrue(plan.ok());
		assertEquals(List.of(NicCode.ENDPOINT_SAME_ADDRESS), codes(plan.warnings()));
		assertEquals(E1, plan.warnings().get(0).endpoint());
		assertEquals(List.of("s3a.example:9000", E1), planned(plan, "ens2np0").serves());
		assertEquals("192.0.2.101", plan.endpoints().get(0).address());
	}

	@Test
	void oneAddressWithTwoPortsIsTwoEndpoints() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(AUTO, List.of("192.0.2.101:9000", "192.0.2.101:9001", "s3a.example"));

		assertTrue(plan.warnings().isEmpty(), plan.warnings()::toString);
		assertEquals(3, planned(plan, "ens2np0").paths());
	}

	@Test
	void anEndpointWithoutIpv4Fails() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);

		final var plan = plan(AUTO, List.of(E1, "[2001:db8::1]:9000"));

		assertEquals(List.of(NicCode.ENDPOINT_NO_IPV4), codes(plan.failures()));
		assertNull(plan.endpoints().get(1).address());
		assertEquals(List.of(E1), planned(plan, "ens2np0").serves());
	}

	// ---------------------------------------------------------------- layouts

	@Test
	void sc26Layout() {
		final var endpoints = Layouts.sc26(host);

		final var plain = plan(AUTO, endpoints);
		final var rdma = plan(AUTO, endpoints, true);

		assertEquals(Layouts.SC26_CONNECTX, plain.nicSet());
		assertEquals(Layouts.SC26_CONNECTX, rdma.nicSet());
		for (final var name : Layouts.SC26_BROADCOM) {
			assertReason(plain, name, NicCode.SLOWER_SPEED_TIER);
			assertReason(rdma, name, NicCode.RDMA_NO_DC);
		}
		assertReason(rdma, "eno8303", NicCode.NO_ENDPOINT_ON_SUBNET);
		assertReason(rdma, "lo", NicCode.VIRTUAL);
		assertReason(rdma, "docker0", NicCode.VIRTUAL);
		for (final var name : Layouts.SC26_CONNECTX) {
			assertEquals(3, planned(rdma, name).gidIndex());
			assertEquals(1, planned(rdma, name).paths());
		}
		assertEquals(1, rdma.groups().size());
		assertTrue(plain.ok() && rdma.ok());
		assertTrue(rdma.warnings().isEmpty(), rdma.warnings()::toString);
	}

	@Test
	void sc26ManagementNicReachesByTheDefaultRouteWhenListed() {
		final var endpoints = Layouts.sc26(host);

		final var plan = plan(List.of("eno8303"), endpoints);

		assertEquals(endpoints, planned(plan, "eno8303").serves());
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	// ---------------------------------------------------------------- helpers

	private FakeHost.Iface nic(final String name, final String pci, final String cidr, final int speed) {
		final var iface = host.physical(name, pci).speed(speed);
		return cidr == null ? iface : iface.address(cidr);
	}

	private void connectX(final String name, final String pci, final String device, final String address) {
		nic(name, pci, address + "/24", 400000);
		host.rdma(device, pci, "mlx5_core").roceV2(3, address, name).probe(true, 4096);
	}

	private void twoOnOneSubnet() {
		nic("ens2np0", "0000:17:00.0", "192.0.2.11/24", 400000);
		nic("ens3np0", "0000:18:00.0", "192.0.2.12/24", 400000);
	}

	private NicPlan plan(final List<String> interfaces, final List<String> endpoints) {
		return plan(interfaces, endpoints, false);
	}

	private NicPlan plan(final List<String> interfaces, final List<String> endpoints, final boolean rdma) {
		return new NicPlanner(RESOLVER).plan(host.read(), interfaces, endpoints, rdma);
	}

	static PlannedNic planned(final NicPlan plan, final String name) {
		return plan.nics().stream().filter(nic -> nic.name().equals(name)).findFirst().orElseThrow(() -> new AssertionError("no NIC " + name));
	}

	static List<NicCode> codes(final List<Finding> findings) {
		return findings.stream().map(Finding::code).toList();
	}

	private static void assertReason(final NicPlan plan, final String name, final NicCode code) {
		final var nic = planned(plan, name);
		assertFalse(nic.used(), name + " is used");
		assertEquals(code, nic.reason() == null ? null : nic.reason().code(), name);
	}
}
