package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.nic.NicInventory.Kind;
import com.dell.spt.base.nic.NicInventory.Nic;
import com.dell.spt.base.nic.NicInventory.RdmaPort;
import com.dell.spt.base.nic.NicInventory.RoceGid;
import com.dell.spt.base.nic.NicInventory.Route;
import com.dell.spt.base.nic.NicInventory.Sysctls;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The inventory read from fixture sysfs and {@code /proc} trees (item 15). */
class SysfsInventoryReaderTest {

	@TempDir
	Path root;

	private FakeHost host;

	@BeforeEach
	void createHost() {
		host = new FakeHost(root);
	}

	@Test
	void physicalNicCarriesItsAttributes() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24").speed(400000).mtu(9000).sysctl(1, 2, 2);
		host.numaNode("0000:17:00.0", 1);

		final var nic = nic("ens2np0");

		assertEquals(Kind.PHYSICAL, nic.kind());
		assertNull(nic.exclusion());
		assertTrue(nic.up());
		assertEquals(List.of(Ipv4Prefix.parse("192.0.2.11/24")), nic.addresses());
		assertEquals(400000, nic.speedMbps());
		assertEquals(9000, nic.mtu());
		assertEquals(1, nic.numaNode());
		assertEquals("0000:17:00.0/0", nic.port());
		assertEquals(List.of(), nic.lowerDevices());
		assertNull(nic.bondMaster());
		assertNull(nic.bondHashPolicy());
		assertNull(nic.rdmaDevice());
		assertEquals(new Sysctls(1, 2, 2), nic.sysctls());
	}

	@Test
	void hostWideValuesAreRead() throws Exception {
		host.sysctlAll(0, 1, 1);

		final var reader = host.reader();

		assertEquals("client-a", reader.hostname());
		assertEquals(new Sysctls(0, 1, 1), reader.read().allSysctls());
	}

	@Test
	void interfacesAreListedInNaturalOrder() {
		host.physical("ens10np0", "0000:3d:00.0");
		host.physical("ens2np0", "0000:17:00.0");
		host.physical("ens101np0", "0000:bc:00.0");
		host.loopback();

		assertEquals(List.of("ens2np0", "ens10np0", "ens101np0", "lo"), host.read().nics().stream().map(Nic::name).toList());
	}

	@Test
	void downAndUnknownOperstates() {
		host.physical("eth0", "0000:01:00.0").down();
		host.physical("eth1", "0000:02:00.0").operstate("unknown");
		host.physical("eth2", "0000:03:00.0").down().operstate("unknown");
		host.physical("eth3", "0000:04:00.0").operstate("lowerlayerdown");

		assertFalse(nic("eth0").up());
		assertTrue(nic("eth1").up(), "unknown with IFF_UP and carrier counts as up");
		assertFalse(nic("eth2").up());
		assertFalse(nic("eth3").up());
	}

	@Test
	void unknownSpeedAndNumaNode() {
		host.physical("eth0", "0000:01:00.0").speed(-1);
		host.numaNode("0000:01:00.0", -1);
		host.physical("eth1", "0000:02:00.0").speedUnreadable();

		assertNull(nic("eth0").speedMbps());
		assertNull(nic("eth0").numaNode());
		assertNull(nic("eth1").speedMbps());
		assertNull(nic("eth1").numaNode(), "no numa_node file anywhere up the tree");
	}

	@Test
	void numaNodeComesFromAnAncestorDevice() {
		host.virtio("ens18", "0000:00:12.0").address("192.0.2.30/24");
		host.numaNode("0000:00:12.0", 0);

		final var nic = nic("ens18");

		assertEquals(Kind.PHYSICAL, nic.kind());
		assertNull(nic.exclusion());
		assertNull(nic.speedMbps(), "virtio-net reports no speed");
		assertEquals(0, nic.numaNode());
		assertEquals("0000:00:12.0/0", nic.port());
	}

	@Test
	void virtualFunctionSharesItsPhysicalFunctionsPort() {
		host.physical("ens2f0np0", "0000:17:00.0");
		host.virtualFunction("ens2f0v0", "0000:17:00.2", "0000:17:00.0");

		assertEquals("0000:17:00.0/0", nic("ens2f0v0").port());
		assertEquals(nic("ens2f0np0").port(), nic("ens2f0v0").port());
	}

	@Test
	void portsOfOneFunctionDifferByDevPort() {
		host.physical("eth0", "0000:01:00.0").devPort(0);
		host.physical("eth1", "0000:01:00.0").devPort(1);

		assertEquals("0000:01:00.0/0", nic("eth0").port());
		assertEquals("0000:01:00.0/1", nic("eth1").port());
	}

	@Test
	void structuralExclusions() {
		host.loopback();
		host.virtual("docker0");
		host.virtual("veth1a2b").address("172.17.0.1/16");
		host.physical("wlp1s0", "0000:05:00.0").wireless();
		host.physical("ib0", "0000:06:00.0").type(32);

		assertEquals(Kind.LOOPBACK, nic("lo").kind());
		assertEquals(NicCode.VIRTUAL, nic("lo").exclusion());
		assertEquals(Kind.VIRTUAL, nic("docker0").kind());
		assertEquals(NicCode.VIRTUAL, nic("docker0").exclusion());
		assertNull(nic("docker0").port());
		assertEquals(NicCode.VIRTUAL, nic("veth1a2b").exclusion());
		assertEquals(NicCode.NOT_ETHERNET, nic("wlp1s0").exclusion());
		assertEquals(NicCode.NOT_ETHERNET, nic("ib0").exclusion());
	}

	// ---------------------------------------------------------------- bonds and VLANs

	@Test
	void bondAndItsMembers() {
		host.physical("ens3f1np1", "0000:3d:00.1").speed(100000);
		host.physical("ens3f0np0", "0000:3d:00.0").speed(100000);
		host.numaNode("0000:3d:00.0", 1).numaNode("0000:3d:00.1", 1);
		host.bond("bond0", "ens3f1np1", "ens3f0np0").address("192.0.2.40/24").speed(200000).hashPolicy("layer2");

		final var bond = nic("bond0");
		final var member = nic("ens3f0np0");

		assertEquals(Kind.BOND, bond.kind());
		assertNull(bond.exclusion());
		assertEquals(List.of("ens3f0np0", "ens3f1np1"), bond.lowerDevices());
		assertEquals("layer2", bond.bondHashPolicy());
		assertEquals("bond:bond0", bond.port());
		assertEquals(1, bond.numaNode());
		assertEquals(200000, bond.speedMbps());
		assertEquals(Kind.PHYSICAL, member.kind());
		assertEquals(NicCode.BOND_MEMBER, member.exclusion());
		assertEquals("bond0", member.bondMaster());
		assertNull(member.bondHashPolicy(), "the policy is the bond's");
	}

	@Test
	void bondOverTwoNumaNodesHasNoNode() {
		host.physical("ens3f0np0", "0000:3d:00.0");
		host.physical("ens7f0np0", "0000:bc:00.0");
		host.numaNode("0000:3d:00.0", 0).numaNode("0000:bc:00.0", 1);
		host.bond("bond0", "ens3f0np0", "ens7f0np0");

		assertNull(nic("bond0").numaNode());
	}

	@Test
	void bondOverVirtualDevicesIsVirtual() {
		host.virtual("veth0");
		host.bond("bond1", "veth0");
		host.bond("bond2");

		assertEquals(NicCode.VIRTUAL, nic("bond1").exclusion());
		assertEquals(NicCode.VIRTUAL, nic("bond2").exclusion(), "a bond without members");
	}

	@Test
	void vlanTakesItsParentsPortNumaNodeAndRdmaDevice() {
		host.physical("ens2np0", "0000:17:00.0");
		host.numaNode("0000:17:00.0", 1);
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core");
		host.vlan("ens2np0.100", "ens2np0").address("198.51.100.11/24");

		final var vlan = nic("ens2np0.100");

		assertEquals(Kind.VLAN, vlan.kind());
		assertNull(vlan.exclusion());
		assertEquals(List.of("ens2np0"), vlan.lowerDevices());
		assertEquals("0000:17:00.0/0", vlan.port());
		assertEquals(1, vlan.numaNode());
		assertEquals("mlx5_0", vlan.rdmaDevice());
	}

	@Test
	void vlanOnAVirtualDeviceIsVirtual() {
		host.virtual("br0");
		host.vlan("br0.5", "br0");

		assertEquals(NicCode.VIRTUAL, nic("br0.5").exclusion());
		assertNull(nic("br0.5").port());
	}

	@Test
	void vlanOnABondUsesTheBondsPort() {
		host.physical("ens3f0np0", "0000:3d:00.0");
		host.physical("ens3f1np1", "0000:3d:00.1");
		host.numaNode("0000:3d:00.0", 0).numaNode("0000:3d:00.1", 0);
		host.bond("bond0", "ens3f0np0", "ens3f1np1");
		host.vlan("bond0.200", "bond0");

		final var vlan = nic("bond0.200");

		assertNull(vlan.exclusion());
		assertEquals("bond:bond0", vlan.port());
		assertEquals(0, vlan.numaNode());
	}

	// ---------------------------------------------------------------- RDMA

	@Test
	void rdmaDeviceWithItsPortsGidsAndProbeAttributes() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24");
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core")
						.gid(1, 2, "192.0.2.11", "IB/RoCE v1", "ens2np0")
						.roceV2(3, "192.0.2.11", "ens2np0")
						.probe(true, 4096);

		final var inventory = host.read();
		final var device = inventory.rdmaDevice("mlx5_0").orElseThrow();
		final var nic = inventory.nic("ens2np0").orElseThrow();

		assertEquals("mlx5_core", device.driver());
		assertTrue(device.mlx5());
		assertEquals(List.of(new RdmaPort(1, "ACTIVE", "Ethernet")), device.ports());
		assertEquals(Boolean.TRUE, device.dcSupported());
		assertEquals(4096, device.activeMtu());
		assertEquals("mlx5_0", nic.rdmaDevice());
		final int address = Ipv4.parse("192.0.2.11");
		assertEquals(
						List.of(new RoceGid("mlx5_0", 1, 2, address, false), new RoceGid("mlx5_0", 1, 3, address, true)),
						nic.gids(),
						"the zero GID at index 0 is skipped");
	}

	@Test
	void probeAnswersMayBeUnknown() {
		host.physical("ens2np0", "0000:17:00.0");
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core");
		host.physical("ens5f0np0", "0000:9a:00.0");
		host.rdma("bnxt_re0", "0000:9a:00.0", "bnxt_en").probe(null, 1024);

		final var inventory = host.read();

		assertNull(inventory.rdmaDevice("mlx5_0").orElseThrow().dcSupported());
		assertNull(inventory.rdmaDevice("mlx5_0").orElseThrow().activeMtu());
		assertFalse(inventory.rdmaDevice("bnxt_re0").orElseThrow().mlx5());
		assertEquals(1024, inventory.rdmaDevice("bnxt_re0").orElseThrow().activeMtu());
	}

	@Test
	void gidsGoToTheNetdevTheyName() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24");
		host.vlan("ens2np0.100", "ens2np0").address("198.51.100.11/24");
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core")
						.roceV2(3, "192.0.2.11", "ens2np0")
						.roceV2(5, "198.51.100.11", "ens2np0.100");

		final var inventory = host.read();

		assertEquals(List.of(3), inventory.nic("ens2np0").orElseThrow().gids().stream().map(RoceGid::index).toList());
		assertEquals(List.of(5), inventory.nic("ens2np0.100").orElseThrow().gids().stream().map(RoceGid::index).toList());
	}

	@Test
	void gidOnPortTwoIsReadWithItsPort() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24");
		host.rdma("mlx5_0", "0000:17:00.0", "mlx5_core").port(2, "ACTIVE", "Ethernet").gid(2, 3, "192.0.2.11", "RoCE v2", "ens2np0");

		final var inventory = host.read();

		assertEquals(List.of(new RoceGid("mlx5_0", 2, 3, Ipv4.parse("192.0.2.11"), true)), inventory.nic("ens2np0").orElseThrow().gids());
		assertEquals(2, inventory.rdmaDevice("mlx5_0").orElseThrow().ports().size());
	}

	@Test
	void roceLagDeviceServesTheBond() {
		host.physical("ens3f0np0", "0000:3d:00.0");
		host.physical("ens3f1np1", "0000:3d:00.1");
		host.numaNode("0000:3d:00.0", 0).numaNode("0000:3d:00.1", 0);
		host.bond("bond0", "ens3f0np0", "ens3f1np1").address("192.0.2.40/24");
		host.rdma("mlx5_bond_0", "0000:3d:00.0", "mlx5_core").roceV2(3, "192.0.2.40", "bond0");

		final var inventory = host.read();

		assertEquals("mlx5_bond_0", inventory.nic("bond0").orElseThrow().rdmaDevice());
		assertEquals(1, inventory.nic("bond0").orElseThrow().gids().size());
		assertTrue(inventory.nic("ens3f0np0").orElseThrow().gids().isEmpty());
	}

	@Test
	void noRdmaTreeMeansNoDevices() {
		host.physical("eth0", "0000:01:00.0");

		assertTrue(host.read().rdmaDevices().isEmpty());
	}

	// ---------------------------------------------------------------- routes

	@Test
	void routesFromTheMainTable() {
		host.route("ens2np0", "192.0.2.0/24", null).route("ens2np0", "203.0.113.0/24", "192.0.2.1");

		final int mask = Ipv4.mask(24);
		assertEquals(
						List.of(
										new Route("ens2np0", Ipv4.parse("192.0.2.0"), mask, 0, false),
										new Route("ens2np0", Ipv4.parse("203.0.113.0"), mask, Ipv4.parse("192.0.2.1"), true)),
						host.read().routes());
	}

	@Test
	void routeCoversItsSubnet() {
		final var route = new Route("eth0", Ipv4.parse("203.0.113.0"), Ipv4.mask(25), Ipv4.parse("192.0.2.1"), true);

		assertTrue(route.covers(Ipv4.parse("203.0.113.100")));
		assertFalse(route.covers(Ipv4.parse("203.0.113.200")));
	}

	private Nic nic(final String name) {
		return host.read().nic(name).orElseThrow();
	}
}
