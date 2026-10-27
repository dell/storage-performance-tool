package com.dell.spt.base.nic;

import java.util.List;
import java.util.Optional;

/**
 * What one host shows about its network interfaces, read once from sysfs, {@code /proc} and,
 * for RDMA attributes sysfs lacks, the native layer. The planner takes it as data.
 */
public record NicInventory(List<Nic>nics,List<RdmaDevice>rdmaDevices,List<Route>routes,Sysctls allSysctls){

public NicInventory{nics=List.copyOf(nics);rdmaDevices=List.copyOf(rdmaDevices);routes=List.copyOf(routes);}

public Optional<Nic>nic(final String name){return nics.stream().filter(nic->nic.name().equals(name)).findFirst();}

public Optional<RdmaDevice>rdmaDevice(final String name){return rdmaDevices.stream().filter(device->device.name().equals(name)).findFirst();}

/** How sysfs classifies an interface. */
public enum Kind {
	/** Backed by a PCI device, virtio-net included. */
	PHYSICAL, BOND, VLAN,
	/** Everything without a PCI device of its own: veth, bridges, tun/tap, WireGuard. */
	VIRTUAL, LOOPBACK;

	public String id() {
		return name().toLowerCase(java.util.Locale.ROOT);
	}

	}

	/**
	 * One interface. {@code exclusion} is set when the interface can never be an automatic
	 * candidate whatever the endpoints: virtual, not wired Ethernet, or a bond member.
	 * {@code port} identifies the physical port the interface uses: interfaces with the same value
	 * share that port's bandwidth.
	 */
	public record Nic(
					String name,
					Kind kind,
					NicCode exclusion,
					boolean up,
					List<Ipv4Prefix> addresses,
					Integer speedMbps,
					Integer mtu,
					Integer numaNode,
					String port,
					List<String> lowerDevices,
					String bondMaster,
					String bondHashPolicy,
					String rdmaDevice,
					List<RoceGid> gids,
					Sysctls sysctls) {

		public Nic {
			addresses = List.copyOf(addresses);
			lowerDevices = List.copyOf(lowerDevices);
			gids = List.copyOf(gids);
		}
	}

	/** A GID table entry that holds an IPv4 address, with its device, port and index. */
	public record RoceGid(String device, int port, int index, int address, boolean roceV2) {}

	/** One RDMA device: its kernel driver, its ports, and what only the native layer reports. */
	public record RdmaDevice(
					String name, String driver, List<RdmaPort> ports, Boolean dcSupported, Integer activeMtu) {

		public RdmaDevice {
			ports = List.copyOf(ports);
		}

	public boolean mlx5() {
		return "mlx5_core".equals(driver);
	}

	public Optional<RdmaPort> port(final int number) {
		return ports.stream().filter(port -> port.number() == number).findFirst();
	}

	}

	public record RdmaPort(int number, String state, String linkLayer) {

	public boolean activeEthernet() {
		return "ACTIVE".equals(state) && "Ethernet".equals(linkLayer);
	}

	}

	/** A main-table route from {@code /proc/net/route}. */
	public record Route(String iface, int destination, int mask, int gateway, boolean viaGateway) {

	public boolean covers(final int address) {
		return (address & mask) == destination;
	}

	}

	/** The ARP and reverse-path settings of {@code all} or of one interface; null when unread. */
	public record Sysctls(Integer arpIgnore, Integer arpAnnounce, Integer rpFilter) {

	public static final Sysctls UNKNOWN = new Sysctls(null, null, null);
}}
