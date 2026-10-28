package com.dell.spt.base.nic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a sysfs and {@code /proc} tree below a temporary root, with real symlinks where Linux
 * has them, and reads it back through {@link SysfsInventoryReader}.
 */
final class FakeHost {

	private final Path root;
	private final Map<String, List<Ipv4Prefix>> addresses = new HashMap<>();
	private final Map<String, RdmaDeviceProbe.Attributes> rdmaAttributes = new HashMap<>();
	private final List<String> routes = new ArrayList<>();

	FakeHost(final Path root) {
		this.root = root;
		write("proc/sys/kernel/hostname", "client-a");
		sysctlAll(1, 2, 2);
	}

	SysfsInventoryReader reader() {
		return reader(true);
	}

	/** A reader that asks the fake native probe, as with {@code --rdma}, or nothing. */
	SysfsInventoryReader reader(final boolean probe) {
		writeRoutes();
		return new SysfsInventoryReader(
						root,
						iface -> addresses.getOrDefault(iface, List.of()),
						probe ? device -> rdmaAttributes.getOrDefault(device, RdmaDeviceProbe.Attributes.UNKNOWN) : RdmaDeviceProbe.NONE);
	}

	NicInventory read() {
		try {
			return reader().read();
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	Path root() {
		return root;
	}

	// ---------------------------------------------------------------- interfaces

	/** A NIC on its own PCI function, up, Ethernet, of unknown speed and NUMA node until set. */
	Iface physical(final String name, final String pciAddress) {
		final var pci = pciDir(pciAddress);
		final var net = pci.resolve("net").resolve(name);
		final var iface = new Iface(name, net);
		link(net.resolve("device"), pci);
		return iface;
	}

	/** A virtio-net NIC: the netdev sits below a virtio device below the PCI function. */
	Iface virtio(final String name, final String pciAddress) {
		final var virtio = pciDir(pciAddress).resolve("virtio0");
		final var net = virtio.resolve("net").resolve(name);
		final var iface = new Iface(name, net);
		link(net.resolve("device"), virtio);
		return iface;
	}

	/** An SR-IOV virtual function of the physical function at {@code pfAddress}. */
	Iface virtualFunction(final String name, final String vfAddress, final String pfAddress) {
		final var iface = physical(name, vfAddress);
		link(pciDir(vfAddress).resolve("physfn"), pciDir(pfAddress));
		return iface;
	}

	/** An interface without a device: veth, bridge, tun, WireGuard. */
	Iface virtual(final String name) {
		return new Iface(name, root.resolve("sys/devices/virtual/net").resolve(name));
	}

	Iface loopback() {
		return virtual("lo").type(772).address("127.0.0.1/8");
	}

	/** The bonding driver's control file, which it adds to {@code /sys/class/net} even without bonds. */
	FakeHost bondingMasters(final String... bonds) {
		write("sys/class/net/bonding_masters", String.join(" ", bonds));
		return this;
	}

	Iface vlan(final String name, final String parent) {
		final var iface = virtual(name).devType("vlan");
		link(iface.dir.resolve("lower_" + parent), root.resolve("sys/class/net").resolve(parent));
		return iface;
	}

	Iface bond(final String name, final String... members) {
		final var iface = virtual(name).devType("bond");
		write(iface.dir.resolve("bonding/slaves"), String.join(" ", members));
		write(iface.dir.resolve("bonding/xmit_hash_policy"), "layer3+4 1");
		for (final var member : members) {
			final var memberDir = root.resolve("sys/class/net").resolve(member);
			link(iface.dir.resolve("lower_" + member), memberDir);
			link(memberDir.resolve("master"), root.resolve("sys/class/net").resolve(name));
			mkdirs(memberDir.resolve("bonding_slave"));
		}
		return iface;
	}

	/** The NUMA node of a PCI function; -1 means unknown. */
	FakeHost numaNode(final String pciAddress, final int node) {
		write(pciDir(pciAddress).resolve("numa_node"), Integer.toString(node));
		return this;
	}

	final class Iface {

		private final String name;
		private final Path dir;

		private Iface(final String name, final Path dir) {
			this.name = name;
			this.dir = dir;
			mkdirs(dir);
			link(root.resolve("sys/class/net").resolve(name), dir);
			write(dir.resolve("operstate"), "up");
			write(dir.resolve("flags"), "0x1003");
			write(dir.resolve("carrier"), "1");
			write(dir.resolve("type"), "1");
			write(dir.resolve("mtu"), "1500");
			write(dir.resolve("speed"), "-1");
			write(dir.resolve("uevent"), "INTERFACE=" + name + "\n");
			sysctl(0, 0, 0);
		}

		Iface address(final String cidr) {
			addresses.computeIfAbsent(name, key -> new ArrayList<>()).add(Ipv4Prefix.parse(cidr));
			return this;
		}

		Iface speed(final int mbps) {
			write(dir.resolve("speed"), Integer.toString(mbps));
			return this;
		}

		/** sysfs refuses to read the speed, as for some down links. */
		Iface speedUnreadable() {
			delete(dir.resolve("speed"));
			mkdirs(dir.resolve("speed"));
			return this;
		}

		Iface mtu(final int mtu) {
			write(dir.resolve("mtu"), Integer.toString(mtu));
			return this;
		}

		Iface down() {
			write(dir.resolve("operstate"), "down");
			write(dir.resolve("flags"), "0x1002");
			return this;
		}

		Iface operstate(final String state) {
			write(dir.resolve("operstate"), state);
			return this;
		}

		Iface type(final int type) {
			write(dir.resolve("type"), Integer.toString(type));
			return this;
		}

		Iface devType(final String devType) {
			write(dir.resolve("uevent"), "DEVTYPE=" + devType + "\nINTERFACE=" + name + "\n");
			return this;
		}

		Iface wireless() {
			mkdirs(dir.resolve("wireless"));
			return devType("wlan");
		}

		Iface devPort(final int port) {
			write(dir.resolve("dev_port"), Integer.toString(port));
			return this;
		}

		Iface hashPolicy(final String policy) {
			write(dir.resolve("bonding/xmit_hash_policy"), policy + " 0");
			return this;
		}

		/** This interface's own {@code net.ipv4.conf.<if>} values. */
		Iface sysctl(final int arpIgnore, final int arpAnnounce, final int rpFilter) {
			writeSysctls(name, arpIgnore, arpAnnounce, rpFilter);
			return this;
		}

		FakeHost host() {
			return FakeHost.this;
		}
	}

	// ---------------------------------------------------------------- RDMA

	/**
	 * An RDMA device on the PCI function, with port 1 active on Ethernet. A RoCE LAG device is one
	 * too: it sits on the first member's PCI function, and its GIDs name the bond.
	 */
	Rdma rdma(final String device, final String pciAddress, final String driver) {
		return new Rdma(device, pciAddress, driver);
	}

	final class Rdma {

		private final String device;
		private final Path dir;

		private Rdma(final String device, final String pciAddress, final String driver) {
			this.device = device;
			this.dir = root.resolve("sys/class/infiniband").resolve(device);
			final var pci = pciDir(pciAddress);
			mkdirs(pci.resolve("infiniband").resolve(device));
			link(dir.resolve("device"), pci);
			final var drivers = root.resolve("sys/bus/pci/drivers").resolve(driver);
			mkdirs(drivers);
			if (!Files.isSymbolicLink(pci.resolve("driver"))) {
				link(pci.resolve("driver"), drivers);
			}
			port(1, "ACTIVE", "Ethernet");
			// An unused entry, as sysfs shows them: a zero GID whose type cannot be read.
			write(dir.resolve("ports/1/gids/0"), "0000:0000:0000:0000:0000:0000:0000:0000");
		}

		Rdma port(final int number, final String state, final String linkLayer) {
			write(dir.resolve("ports/" + number + "/state"), (state.equals("ACTIVE") ? "4: " : "1: ") + state);
			write(dir.resolve("ports/" + number + "/link_layer"), linkLayer);
			return this;
		}

		/** A GID table entry holding {@code address} for {@code netdev}. */
		Rdma gid(final int port, final int index, final String address, final String type, final String netdev) {
			final var value = Ipv4.parse(address);
			write(
							dir.resolve("ports/" + port + "/gids/" + index),
							String.format("0000:0000:0000:0000:0000:ffff:%04x:%04x", (value >>> 16) & 0xffff, value & 0xffff));
			write(dir.resolve("ports/" + port + "/gid_attrs/types/" + index), type);
			write(dir.resolve("ports/" + port + "/gid_attrs/ndevs/" + index), netdev);
			return this;
		}

		Rdma roceV2(final int index, final String address, final String netdev) {
			return gid(1, index, address, "RoCE v2", netdev);
		}

		/** What the native probe reports for the device. */
		Rdma probe(final Boolean dc, final Integer activeMtu) {
			rdmaAttributes.put(device, new RdmaDeviceProbe.Attributes(dc, activeMtu));
			return this;
		}

		FakeHost host() {
			return FakeHost.this;
		}
	}

	// ---------------------------------------------------------------- routes and sysctls

	/** A main-table route; {@code gateway} null for an on-link route. */
	FakeHost route(final String iface, final String destination, final String gateway) {
		final var prefix = Ipv4Prefix.parse(destination);
		final int flags = 0x1 | (gateway == null ? 0 : 0x2);
		routes.add(String.format(
						"%s\t%08X\t%08X\t%04X\t0\t0\t0\t%08X\t0\t0\t0",
						iface,
						Integer.reverseBytes(prefix.address() & Ipv4.mask(prefix.prefixLength())),
						Integer.reverseBytes(gateway == null ? 0 : Ipv4.parse(gateway)),
						flags,
						Integer.reverseBytes(Ipv4.mask(prefix.prefixLength()))));
		return this;
	}

	FakeHost sysctlAll(final int arpIgnore, final int arpAnnounce, final int rpFilter) {
		writeSysctls("all", arpIgnore, arpAnnounce, rpFilter);
		return this;
	}

	private void writeSysctls(final String name, final int arpIgnore, final int arpAnnounce, final int rpFilter) {
		final var dir = root.resolve("proc/sys/net/ipv4/conf").resolve(name);
		write(dir.resolve("arp_ignore"), Integer.toString(arpIgnore));
		write(dir.resolve("arp_announce"), Integer.toString(arpAnnounce));
		write(dir.resolve("rp_filter"), Integer.toString(rpFilter));
	}

	private void writeRoutes() {
		final var text = new StringBuilder("Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n");
		routes.forEach(route -> text.append(route).append('\n'));
		write("proc/net/route", text.toString());
	}

	// ---------------------------------------------------------------- files

	private Path pciDir(final String pciAddress) {
		final var dir = root.resolve("sys/devices/pci0000:00").resolve(pciAddress);
		mkdirs(dir);
		return dir;
	}

	private void write(final String relative, final String content) {
		write(root.resolve(relative), content);
	}

	private static void write(final Path file, final String content) {
		try {
			Files.createDirectories(file.getParent());
			Files.writeString(file, content + "\n", StandardCharsets.UTF_8);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void link(final Path link, final Path target) {
		try {
			Files.createDirectories(link.getParent());
			Files.deleteIfExists(link);
			Files.createSymbolicLink(link, target);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void mkdirs(final Path dir) {
		try {
			Files.createDirectories(dir);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void delete(final Path file) {
		try {
			Files.deleteIfExists(file);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
