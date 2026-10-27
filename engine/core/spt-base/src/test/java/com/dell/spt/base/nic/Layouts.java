package com.dell.spt.base.nic;

import java.util.List;

/** Host layouts shared by the planner and golden tests, on documentation addresses only. */
final class Layouts {

	static final String DATA_SUBNET = "192.0.2.0/24";
	static final List<String> SC26_CONNECTX = List.of("ens2np0", "ens10np0", "ens33np0", "ens101np0");
	static final List<String> SC26_BROADCOM = List.of("ens5f0np0", "ens5f1np1", "ens6f0np0", "ens6f1np1");

	private Layouts() {}

	/**
	 * The SC26 client's shape: four 400G ConnectX and four 200G Broadcom NICs on one /24, four
	 * storage nodes on it, and a management NIC off it that holds the default route.
	 *
	 * @return the four endpoints
	 */
	static List<String> sc26(final FakeHost host) {
		host.physical("eno8303", "0000:04:00.0").address("198.51.100.10/24").speed(1000);
		host.numaNode("0000:04:00.0", 0);
		host.route("eno8303", "0.0.0.0/0", "198.51.100.1").route("eno8303", "198.51.100.0/24", null);
		connectX(host, "ens2np0", "0000:17:00.0", "mlx5_0", 0, "192.0.2.11");
		connectX(host, "ens10np0", "0000:3d:00.0", "mlx5_1", 0, "192.0.2.12");
		connectX(host, "ens33np0", "0000:9a:00.0", "mlx5_2", 1, "192.0.2.13");
		connectX(host, "ens101np0", "0000:bc:00.0", "mlx5_3", 1, "192.0.2.14");
		broadcom(host, "ens5f0np0", "0000:31:00.0", "bnxt_re0", 0, "192.0.2.21");
		broadcom(host, "ens5f1np1", "0000:31:00.1", "bnxt_re1", 0, "192.0.2.22");
		broadcom(host, "ens6f0np0", "0000:b1:00.0", "bnxt_re2", 1, "192.0.2.23");
		broadcom(host, "ens6f1np1", "0000:b1:00.1", "bnxt_re3", 1, "192.0.2.24");
		host.loopback();
		host.virtual("docker0").address("172.17.0.1/16").down();
		return List.of("192.0.2.101:9000", "192.0.2.102:9000", "192.0.2.103:9000", "192.0.2.104:9000");
	}

	static void connectX(final FakeHost host, final String name, final String pci, final String device, final int numa, final String address) {
		dataNic(host, name, pci, numa, address, 400000);
		host.rdma(device, pci, "mlx5_core").gid(1, 2, address, "IB/RoCE v1", name).roceV2(3, address, name).probe(true, 4096);
	}

	static void broadcom(final FakeHost host, final String name, final String pci, final String device, final int numa, final String address) {
		dataNic(host, name, pci, numa, address, 200000);
		host.rdma(device, pci, "bnxt_en").gid(1, 2, address, "IB/RoCE v1", name).roceV2(3, address, name).probe(false, 4096);
	}

	private static void dataNic(final FakeHost host, final String name, final String pci, final int numa, final String address, final int speed) {
		host.physical(name, pci).address(address + "/24").speed(speed).mtu(9000);
		host.numaNode(pci, numa);
		host.route(name, DATA_SUBNET, null);
	}
}
