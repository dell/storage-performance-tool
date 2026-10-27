package com.dell.spt.base.nic;

import com.dell.spt.base.nic.NicInventory.Kind;
import com.dell.spt.base.nic.NicInventory.Nic;
import com.dell.spt.base.nic.NicInventory.RdmaDevice;
import com.dell.spt.base.nic.NicInventory.RdmaPort;
import com.dell.spt.base.nic.NicInventory.RoceGid;
import com.dell.spt.base.nic.NicInventory.Route;
import com.dell.spt.base.nic.NicInventory.Sysctls;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Reads a host's {@link NicInventory} from sysfs and {@code /proc}, below a root that is {@code /}
 * in production and a fixture tree in tests. IPv4 addresses come from an {@link AddressSource},
 * because sysfs does not hold them, and DC support and the RoCE active MTU from an {@link
 * RdmaDeviceProbe}.
 */
public final class SysfsInventoryReader {

	/** The IPv4 addresses of an interface, with the prefix lengths of their subnets. */
	@FunctionalInterface
	public interface AddressSource {
		List<Ipv4Prefix> addresses(String iface);
	}

	/** This process's view through {@link NetworkInterface}. */
	public static final AddressSource SYSTEM_ADDRESSES = SysfsInventoryReader::systemAddresses;

	private static final Pattern PCI_ADDRESS = Pattern.compile("[0-9a-f]{4,}:[0-9a-f]{2}:[0-9a-f]{2}\\.[0-7]");
	private static final Pattern WHITESPACE = Pattern.compile("\\s+");
	private static final String IPV4_MAPPED_GID_PREFIX = "0000:0000:0000:0000:0000:ffff:";
	private static final int ARPHRD_ETHER = 1;
	private static final int ARPHRD_LOOPBACK = 772;
	private static final int IFF_UP = 0x1;
	private static final int RTF_UP = 0x1;
	private static final int RTF_GATEWAY = 0x2;

	private final Path root;
	private final AddressSource addressSource;
	private final RdmaDeviceProbe probe;

	public SysfsInventoryReader(final Path root, final AddressSource addressSource, final RdmaDeviceProbe probe) {
		this.root = root;
		this.addressSource = addressSource;
		this.probe = probe;
	}

	public static SysfsInventoryReader system(final RdmaDeviceProbe probe) {
		return new SysfsInventoryReader(Path.of("/"), SYSTEM_ADDRESSES, probe);
	}

	/** The host name from {@code /proc/sys/kernel/hostname}, or {@code null}. */
	public String hostname() {
		return readTrimmed(root.resolve("proc/sys/kernel/hostname"));
	}

	public NicInventory read() throws IOException {
		final var gidsByNetdev = new HashMap<String, List<RoceGid>>();
		final var rdmaDevices = readRdmaDevices(gidsByNetdev);
		final var raw = new LinkedHashMap<String, RawNic>();
		for (final var name : listNames(root.resolve("sys/class/net"))) {
			raw.put(name, readRawNic(name));
		}
		final var nics = new ArrayList<Nic>();
		for (final var nic : raw.values()) {
			nics.add(toNic(nic, raw, gidsByNetdev));
		}
		return new NicInventory(nics, rdmaDevices, readRoutes(), readSysctls("all"));
	}

	// ---------------------------------------------------------------- interfaces

	/** What one {@code /sys/class/net/<if>} directory shows, before relations are resolved. */
	private record RawNic(
					String name,
					int type,
					String devType,
					boolean up,
					String pciAddress,
					String devPort,
					boolean wireless,
					Integer speedMbps,
					Integer mtu,
					Integer numaNode,
					List<String> lowers,
					boolean bond,
					String bondMaster,
					String bondHashPolicy,
					String structuralRdmaDevice,
					Sysctls sysctls) {}

	private RawNic readRawNic(final String name) throws IOException {
		final var dir = root.resolve("sys/class/net").resolve(name);
		final var type = readInt(dir.resolve("type"));
		final var devType = ueventValue(dir.resolve("uevent"), "DEVTYPE");
		final var device = realPath(dir.resolve("device"));
		String pciAddress = null;
		Integer numaNode = null;
		String rdmaDevice = null;
		if (device != null) {
			final var physfn = realPath(device.resolve("physfn"));
			pciAddress = lastPciSegment(physfn != null ? physfn : device);
			numaNode = numaNode(device);
			rdmaDevice = listNames(device.resolve("infiniband")).stream().findFirst().orElse(null);
		}
		final var bond = Files.isDirectory(dir.resolve("bonding")) || "bond".equals(devType);
		final var lowers = new ArrayList<String>();
		for (final var entry : listNames(dir)) {
			if (entry.startsWith("lower_")) {
				lowers.add(entry.substring("lower_".length()));
			}
		}
		if (bond) {
			final var slaves = readTrimmed(dir.resolve("bonding/slaves"));
			if (slaves != null) {
				for (final var slave : WHITESPACE.split(slaves, 0)) {
					if (!slave.isEmpty() && !lowers.contains(slave)) {
						lowers.add(slave);
					}
				}
			}
		}
		lowers.sort(NaturalOrder.INSTANCE);
		String bondMaster = null;
		final var master = realPath(dir.resolve("master"));
		if (master != null && (Files.isDirectory(master.resolve("bonding")) || Files.isDirectory(dir.resolve("bonding_slave")))) {
			bondMaster = master.getFileName().toString();
		}
		final var policy = readTrimmed(dir.resolve("bonding/xmit_hash_policy"));
		final var speed = readInt(dir.resolve("speed"));
		return new RawNic(
						name,
						type == null ? -1 : type,
						devType,
						isUp(dir),
						pciAddress,
						pciAddress == null ? null : orDefault(readTrimmed(dir.resolve("dev_port")), "0"),
						Files.exists(dir.resolve("wireless")) || Files.exists(dir.resolve("phy80211")) || "wlan".equals(devType),
						speed == null || speed < 0 ? null : speed,
						readInt(dir.resolve("mtu")),
						numaNode,
						lowers,
						bond,
						bondMaster,
						policy == null ? null : WHITESPACE.split(policy, 0)[0],
						rdmaDevice,
						readSysctls(name));
	}

	private Nic toNic(final RawNic nic, final Map<String, RawNic> all, final Map<String, List<RoceGid>> gidsByNetdev) {
		final var kind = kind(nic);
		final var gids = gidsByNetdev.getOrDefault(nic.name(), List.of());
		final var rdmaDevice = gids.isEmpty() ? structuralRdmaDevice(nic, all, new HashSet<>()) : gids.get(0).device();
		return new Nic(
						nic.name(),
						kind,
						exclusion(nic, all, new HashSet<>()),
						nic.up(),
						addressSource.addresses(nic.name()),
						nic.speedMbps(),
						nic.mtu(),
						numaNode(nic, all, new HashSet<>()),
						port(nic, all, new HashSet<>()),
						nic.lowers(),
						nic.bondMaster(),
						kind == Kind.BOND ? nic.bondHashPolicy() : null,
						rdmaDevice,
						gids,
						nic.sysctls());
	}

	private static Kind kind(final RawNic nic) {
		if (nic.type() == ARPHRD_LOOPBACK) {
			return Kind.LOOPBACK;
		}
		if (nic.bond()) {
			return Kind.BOND;
		}
		if ("vlan".equals(nic.devType())) {
			return Kind.VLAN;
		}
		return nic.pciAddress() != null ? Kind.PHYSICAL : Kind.VIRTUAL;
	}

	/** Why {@code auto} can never pick the interface, or {@code null}. */
	private static NicCode exclusion(final RawNic nic, final Map<String, RawNic> all, final Set<String> seen) {
		if (!seen.add(nic.name())) {
			return NicCode.VIRTUAL;
		}
		final var kind = kind(nic);
		final NicCode own;
		switch (kind) {
		case LOOPBACK, VIRTUAL -> own = NicCode.VIRTUAL;
		case BOND, VLAN -> {
			// Backed by PCI only through its lower devices: every one of them for a bond, its parent for a VLAN.
			NicCode lower = nic.lowers().isEmpty() ? NicCode.VIRTUAL : null;
			for (final var name : nic.lowers()) {
				final var raw = all.get(name);
				final var code = raw == null ? NicCode.VIRTUAL : structural(raw, all, seen, kind == Kind.BOND);
				if (code != null) {
					lower = code;
					break;
				}
			}
			own = lower;
		}
		default -> own = null;
		}
		if (own != null) {
			return own;
		}
		if (nic.type() != ARPHRD_ETHER || nic.wireless()) {
			return NicCode.NOT_ETHERNET;
		}
		return nic.bondMaster() != null ? NicCode.BOND_MEMBER : null;
	}

	/** A lower device's structural exclusion, where a bond's own members do not count as bond members. */
	private static NicCode structural(final RawNic lower, final Map<String, RawNic> all, final Set<String> seen, final boolean ofBond) {
		final var code = exclusion(lower, all, seen);
		return ofBond && code == NicCode.BOND_MEMBER ? null : code;
	}

	private static Integer numaNode(final RawNic nic, final Map<String, RawNic> all, final Set<String> seen) {
		if (!seen.add(nic.name())) {
			return null;
		}
		return switch (kind(nic)) {
			case PHYSICAL -> nic.numaNode();
			case BOND, VLAN -> {
				Integer node = null;
				for (final var name : nic.lowers()) {
					final var raw = all.get(name);
					final var lower = raw == null ? null : numaNode(raw, all, seen);
					if (lower == null || (node != null && !node.equals(lower))) {
						yield null;
					}
					node = lower;
				}
				yield node;
			}
			default -> null;
		};
	}

	/** The physical port the interface uses: its PCI function and port, its parent's for a VLAN, its own for a bond. */
	private static String port(final RawNic nic, final Map<String, RawNic> all, final Set<String> seen) {
		if (!seen.add(nic.name())) {
			return null;
		}
		return switch (kind(nic)) {
			case PHYSICAL -> nic.pciAddress() + "/" + nic.devPort();
			case BOND -> "bond:" + nic.name();
			case VLAN -> {
				final var parent = nic.lowers().isEmpty() ? null : all.get(nic.lowers().get(0));
				yield parent == null ? null : port(parent, all, seen);
			}
			default -> null;
		};
	}

	private static String structuralRdmaDevice(final RawNic nic, final Map<String, RawNic> all, final Set<String> seen) {
		if (!seen.add(nic.name())) {
			return null;
		}
		if (nic.structuralRdmaDevice() != null) {
			return nic.structuralRdmaDevice();
		}
		if ("vlan".equals(nic.devType()) && !nic.lowers().isEmpty()) {
			final var parent = all.get(nic.lowers().get(0));
			return parent == null ? null : structuralRdmaDevice(parent, all, seen);
		}
		return null;
	}

	private boolean isUp(final Path dir) {
		final var operstate = readTrimmed(dir.resolve("operstate"));
		if ("up".equals(operstate)) {
			return true;
		}
		if (!"unknown".equals(operstate)) {
			return false;
		}
		// Some drivers and loopback report "unknown": fall back to the administrative flag and the carrier.
		final var flags = readTrimmed(dir.resolve("flags"));
		final var carrier = readInt(dir.resolve("carrier"));
		return flags != null && (Integer.decode(flags) & IFF_UP) != 0 && carrier != null && carrier == 1;
	}

	private Integer numaNode(final Path device) {
		final var sysDevices = realPath(root.resolve("sys/devices"));
		final var devices = sysDevices != null ? sysDevices : root.resolve("sys/devices").normalize();
		for (var dir = device; dir != null && dir.startsWith(devices); dir = dir.getParent()) {
			final var node = readInt(dir.resolve("numa_node"));
			if (node != null) {
				return node < 0 ? null : node;
			}
		}
		return null;
	}

	private static String lastPciSegment(final Path device) {
		String last = null;
		for (final var segment : device) {
			if (PCI_ADDRESS.matcher(segment.toString()).matches()) {
				last = segment.toString();
			}
		}
		return last;
	}

	private Sysctls readSysctls(final String name) {
		final var dir = root.resolve("proc/sys/net/ipv4/conf").resolve(name);
		return new Sysctls(readInt(dir.resolve("arp_ignore")), readInt(dir.resolve("arp_announce")), readInt(dir.resolve("rp_filter")));
	}

	// ---------------------------------------------------------------- RDMA

	private List<RdmaDevice> readRdmaDevices(final Map<String, List<RoceGid>> gidsByNetdev) throws IOException {
		final var devices = new ArrayList<RdmaDevice>();
		for (final var name : listNames(root.resolve("sys/class/infiniband"))) {
			final var dir = root.resolve("sys/class/infiniband").resolve(name);
			final var driver = realPath(dir.resolve("device/driver"));
			final var ports = new ArrayList<RdmaPort>();
			for (final var portName : listNames(dir.resolve("ports"))) {
				final int number;
				try {
					number = Integer.parseInt(portName);
				} catch (final NumberFormatException e) {
					continue;
				}
				final var portDir = dir.resolve("ports").resolve(portName);
				final var state = readTrimmed(portDir.resolve("state"));
				ports.add(new RdmaPort(number, portState(state), readTrimmed(portDir.resolve("link_layer"))));
				readGids(name, number, portDir, gidsByNetdev);
			}
			final var attributes = probe.query(name);
			devices.add(new RdmaDevice(
							name,
							driver == null ? null : driver.getFileName().toString(),
							ports,
							attributes.dcSupported(),
							attributes.activeMtu()));
		}
		return devices;
	}

	/** {@code 4: ACTIVE} becomes {@code ACTIVE}. */
	private static String portState(final String state) {
		if (state == null) {
			return null;
		}
		final var colon = state.indexOf(':');
		return colon < 0 ? state : state.substring(colon + 1).trim();
	}

	private void readGids(final String device, final int port, final Path portDir, final Map<String, List<RoceGid>> gidsByNetdev) throws IOException {
		final var gidDir = portDir.resolve("gids");
		final var indexes = new ArrayList<Integer>();
		for (final var entry : listNames(gidDir)) {
			try {
				indexes.add(Integer.parseInt(entry));
			} catch (final NumberFormatException e) {
				// not a GID entry
			}
		}
		indexes.sort(Integer::compare);
		for (final int index : indexes) {
			final var gid = readTrimmed(gidDir.resolve(Integer.toString(index)));
			if (gid == null || !gid.startsWith(IPV4_MAPPED_GID_PREFIX)) {
				continue;
			}
			// Unused entries fail to read their type and device.
			final var type = readTrimmed(portDir.resolve("gid_attrs/types/" + index));
			final var netdev = readTrimmed(portDir.resolve("gid_attrs/ndevs/" + index));
			if (type == null || netdev == null) {
				continue;
			}
			final var tail = gid.substring(IPV4_MAPPED_GID_PREFIX.length()).replace(":", "");
			final int address = (int) Long.parseLong(tail, 16);
			gidsByNetdev.computeIfAbsent(netdev, key -> new ArrayList<>())
							.add(new RoceGid(device, port, index, address, "RoCE v2".equalsIgnoreCase(type)));
		}
	}

	// ---------------------------------------------------------------- routes

	private List<Route> readRoutes() {
		final var file = root.resolve("proc/net/route");
		final List<String> lines;
		try {
			lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
		} catch (final IOException e) {
			return List.of();
		}
		final var routes = new ArrayList<Route>();
		for (final var line : lines.subList(Math.min(1, lines.size()), lines.size())) {
			final var fields = WHITESPACE.split(line.trim(), 0);
			if (fields.length < 8) {
				continue;
			}
			try {
				final int flags = Integer.parseInt(fields[3], 16);
				if ((flags & RTF_UP) == 0) {
					continue;
				}
				routes.add(new Route(
								fields[0],
								routeAddress(fields[1]),
								routeAddress(fields[7]),
								routeAddress(fields[2]),
								(flags & RTF_GATEWAY) != 0));
			} catch (final NumberFormatException e) {
				// skip a malformed line
			}
		}
		return routes;
	}

	/** {@code /proc/net/route} prints addresses as host-order hex, little-endian on x86 and arm64. */
	private static int routeAddress(final String hex) {
		return Integer.reverseBytes(Integer.parseUnsignedInt(hex, 16));
	}

	// ---------------------------------------------------------------- files

	private static List<String> listNames(final Path dir) throws IOException {
		if (!Files.isDirectory(dir)) {
			return List.of();
		}
		try (Stream<Path> entries = Files.list(dir)) {
			return entries.map(path -> path.getFileName().toString()).sorted(NaturalOrder.INSTANCE).toList();
		}
	}

	private static Path realPath(final Path link) {
		try {
			return Files.exists(link) ? link.toRealPath() : null;
		} catch (final IOException e) {
			return null;
		}
	}

	private static String readTrimmed(final Path file) {
		try {
			return Files.readString(file, StandardCharsets.UTF_8).trim();
		} catch (final IOException | RuntimeException e) {
			// sysfs refuses some reads, such as the speed of a down link
			return null;
		}
	}

	private static Integer readInt(final Path file) {
		final var text = readTrimmed(file);
		if (text == null || text.isEmpty()) {
			return null;
		}
		try {
			return Integer.parseInt(text);
		} catch (final NumberFormatException e) {
			return null;
		}
	}

	private static String ueventValue(final Path file, final String key) {
		final var text = readTrimmed(file);
		if (text == null) {
			return null;
		}
		for (final var line : text.split("\n", -1)) {
			if (line.startsWith(key + "=")) {
				return line.substring(key.length() + 1).trim();
			}
		}
		return null;
	}

	private static String orDefault(final String value, final String fallback) {
		return value == null || value.isEmpty() ? fallback : value;
	}

	private static List<Ipv4Prefix> systemAddresses(final String iface) {
		try {
			final var nif = NetworkInterface.getByName(iface);
			if (nif == null) {
				return List.of();
			}
			final var addresses = new ArrayList<Ipv4Prefix>();
			for (final var address : nif.getInterfaceAddresses()) {
				if (address.getAddress() instanceof Inet4Address inet4) {
					final var bytes = inet4.getAddress();
					final int value = ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
					addresses.add(new Ipv4Prefix(value, address.getNetworkPrefixLength()));
				}
			}
			return addresses;
		} catch (final SocketException e) {
			return List.of();
		}
	}
}
