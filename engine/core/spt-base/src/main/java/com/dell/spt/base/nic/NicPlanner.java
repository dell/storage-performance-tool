package com.dell.spt.base.nic;

import com.dell.spt.base.nic.NicInventory.Kind;
import com.dell.spt.base.nic.NicInventory.Nic;
import com.dell.spt.base.nic.NicInventory.RdmaDevice;
import com.dell.spt.base.nic.NicPlan.Finding;
import com.dell.spt.base.nic.NicPlan.PairingGroup;
import com.dell.spt.base.nic.NicPlan.Path;
import com.dell.spt.base.nic.NicPlan.PlannedEndpoint;
import com.dell.spt.base.nic.NicPlan.PlannedNic;
import com.dell.spt.base.nic.NicPlan.Reason;
import com.dell.spt.base.nic.NicPlan.Reduction;
import com.dell.spt.base.nic.NicPlan.SharedPort;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns an inventory, the engine's endpoints after slicing and the value of {@code
 * storage.net.interfaces} into a {@link NicPlan}: entry resolution (item 2), eligibility and the
 * per-endpoint tier (item 4), shared ports (item 5), the host checks (items 6 to 6b), pairing
 * (item 7) and endpoint identity (item 7a). It reads nothing itself.
 */
public final class NicPlanner {

	public static final String AUTO = "auto";

	static final int ROCE_MIN_ACTIVE_MTU = 4096;
	static final String ARP_FIX = "set net.ipv4.conf.all.arp_ignore=1 and net.ipv4.conf.all.arp_announce=2";
	static final String RP_FILTER_FIX = "set net.ipv4.conf.all.rp_filter=2";
	static final String BOND_HASH_FIX = "set the bond's xmit_hash_policy to layer3+4";
	static final String ROCE_MTU_FIX = "raise the port MTU so the RoCE active MTU is 4096, on the client and the servers";

	private final EndpointResolver resolver;

	public NicPlanner(final EndpointResolver resolver) {
		this.resolver = resolver;
	}

	/**
	 * Plans the NIC set and its Paths for one step's endpoints on this host.
	 *
	 * @param interfaces the setting's entries: {@code [auto]} or a list; never empty
	 * @param endpoints the engine's configured endpoint strings after slicing, in order
	 * @param rdma whether the step uses {@code s3-rdma}
	 */
	public NicPlan plan(final NicInventory inventory, final List<String> interfaces, final List<String> endpoints, final boolean rdma) {
		if (interfaces.isEmpty()) {
			throw new IllegalArgumentException("No interface entries: without a NIC set there is no plan");
		}
		return new Run(inventory, interfaces, rdma).plan(endpoints);
	}

	/** One configured endpoint with its frozen address. */
	private record Endpoint(String endpoint, Integer address, Integer port) {}

	private record AddressPort(int address, Integer port) {}

	/** A NIC under consideration: its chosen address, its RDMA GID index and the endpoints it reaches. */
	private record Candidate(Nic nic, Ipv4Prefix address, Integer gidIndex, List<Endpoint> reach) {}

	/** The result of the RDMA checks: a reason the NIC cannot be used, or its GID index on port 1. */
	private record RdmaCheck(NicCode failure, Integer gidIndex) {}

	private final class Run {

		private final NicInventory inventory;
		private final List<String> interfaces;
		private final boolean auto;
		private final boolean rdma;
		private final List<Endpoint> endpoints = new ArrayList<>();
		private final List<Finding> failures = new ArrayList<>();
		private final List<Finding> warnings = new ArrayList<>();
		private final Map<String, Candidate> chosen = new LinkedHashMap<>();
		private final Map<String, Candidate> candidates = new LinkedHashMap<>();
		private final Map<String, Reason> reasons = new HashMap<>();
		private final Map<String, Set<String>> serves = new HashMap<>();
		private final List<PlannedEndpoint> plannedEndpoints = new ArrayList<>();

		Run(final NicInventory inventory, final List<String> interfaces, final boolean rdma) {
			this.inventory = inventory;
			this.interfaces = interfaces.stream().map(String::trim).toList();
			this.auto = this.interfaces.stream().anyMatch(AUTO::equalsIgnoreCase);
			this.rdma = rdma;
		}

		NicPlan plan(final List<String> configuredEndpoints) {
			resolveEndpoints(configuredEndpoints);
			if (auto && interfaces.size() > 1) {
				failure(NicCode.AUTO_MIXED, "auto must be the only entry, but the setting is " + String.join(",", interfaces), null, null, null);
				for (final var endpoint : endpoints) {
					plannedEndpoints.add(new PlannedEndpoint(endpoint.endpoint(), format(endpoint.address()), List.of(), null, List.of(), List.of()));
				}
			} else if (auto) {
				planAuto();
			} else {
				planList();
			}
			checkCoverage();
			checkSharedPorts();
			checkHost();
			final var groups = pair();
			return new NicPlan(
							interfaces,
							auto,
							rdma,
							List.copyOf(chosen.keySet()),
							plannedNics(groups),
							plannedEndpoints,
							sharedPorts(),
							groups,
							failures,
							warnings);
		}

		// ------------------------------------------------------------ endpoints (item 7a)

		private void resolveEndpoints(final List<String> configured) {
			final var seen = new HashSet<String>();
			final var reported = new HashSet<String>();
			final var byAddress = new HashMap<AddressPort, String>();
			for (final var endpoint : configured) {
				if (!seen.add(endpoint)) {
					if (reported.add(endpoint)) {
						failure(
										NicCode.ENDPOINT_DUPLICATE,
										"endpoint " + endpoint + " is given more than once; with a NIC set each endpoint string must be unique",
										null,
										endpoint,
										"remove the repeated endpoint");
					}
					continue;
				}
				final var hostPort = EndpointResolver.HostPort.parse(endpoint);
				final var address = resolver.resolveIpv4(hostPort.host());
				if (address == null) {
					failure(NicCode.ENDPOINT_NO_IPV4, "endpoint " + endpoint + " has no IPv4 address", null, endpoint, null);
				} else {
					final var first = byAddress.putIfAbsent(new AddressPort(address, hostPort.port()), endpoint);
					if (first != null) {
						warning(
										NicCode.ENDPOINT_SAME_ADDRESS,
										"endpoints " + first + " and " + endpoint + " both resolve to " + Ipv4.format(address)
														+ (hostPort.port() == null ? "" : ":" + hostPort.port()) + "; they count as two endpoints",
										null,
										endpoint,
										null);
					}
				}
				endpoints.add(new Endpoint(endpoint, address, hostPort.port()));
			}
		}

		private List<Endpoint> resolved() {
			return endpoints.stream().filter(endpoint -> endpoint.address() != null).toList();
		}

		// ------------------------------------------------------------ auto (item 4)

		private void planAuto() {
			final var nics = new ArrayList<>(inventory.nics());
			nics.sort(Comparator.comparing(Nic::name, NaturalOrder.INSTANCE));
			for (final var nic : nics) {
				final var reason = autoCandidate(nic);
				if (reason != null) {
					reasons.put(nic.name(), new Reason(reason, null));
				}
			}
			final var reductions = new LinkedHashMap<String, List<Reduction>>();
			for (final var endpoint : endpoints) {
				if (endpoint.address() == null) {
					plannedEndpoints.add(new PlannedEndpoint(endpoint.endpoint(), null, List.of(), null, List.of(), List.of()));
					continue;
				}
				final var reaching = candidates.values().stream().filter(candidate -> candidate.reach().contains(endpoint)).toList();
				final var endpointReductions = new ArrayList<Reduction>();
				// Shared ports (D39): keep the first by name on each physical port, for this endpoint only.
				final var firstOnPort = new HashMap<String, String>();
				final var remaining = new ArrayList<Candidate>();
				for (final var candidate : reaching) {
					final var port = candidate.nic().port();
					final var first = port == null ? null : firstOnPort.putIfAbsent(port, candidate.nic().name());
					if (first != null) {
						endpointReductions.add(new Reduction(candidate.nic().name(), NicCode.SHARES_PORT, first));
					} else {
						remaining.add(candidate);
					}
				}
				// Speed (D28, D22): the fastest known tier; unknown speeds only when no speed is known.
				final var tier = remaining.stream().map(candidate -> candidate.nic().speedMbps()).filter(Objects::nonNull).max(Integer::compare).orElse(null);
				final var kept = new ArrayList<String>();
				for (final var candidate : remaining) {
					final var speed = candidate.nic().speedMbps();
					if (tier == null || tier.equals(speed)) {
						kept.add(candidate.nic().name());
					} else {
						endpointReductions.add(new Reduction(
										candidate.nic().name(),
										speed == null ? NicCode.SPEED_UNKNOWN_BESIDE_KNOWN : NicCode.SLOWER_SPEED_TIER,
										null));
					}
				}
				for (final var name : kept) {
					serves.computeIfAbsent(name, key -> new LinkedHashSet<>()).add(endpoint.endpoint());
				}
				for (final var reduction : endpointReductions) {
					reductions.computeIfAbsent(reduction.nic(), key -> new ArrayList<>()).add(reduction);
				}
				plannedEndpoints.add(new PlannedEndpoint(
								endpoint.endpoint(),
								Ipv4.format(endpoint.address()),
								reaching.stream().map(candidate -> candidate.nic().name()).toList(),
								tier,
								kept,
								endpointReductions));
			}
			for (final var candidate : candidates.values()) {
				final var name = candidate.nic().name();
				if (serves.containsKey(name)) {
					chosen.put(name, candidate);
				} else {
					// Unused on every endpoint it reaches: the first reason in item 4's order.
					final var reduction = reductions.get(name).stream().min(Comparator.comparing(Reduction::code)).orElseThrow();
					reasons.put(name, new Reason(reduction.code(), reduction.with()));
				}
			}
			if (chosen.isEmpty()) {
				failure(
								NicCode.AUTO_NONE_ELIGIBLE,
								resolved().isEmpty()
												? "auto found no eligible NIC: no endpoint has an IPv4 address"
												: "auto found no eligible NIC; each NIC's reason says why",
								null,
								null,
								"name the NICs in a list, or fix the reasons shown");
			}
		}

		/** Registers {@code nic} as a candidate and returns {@code null}, or returns why it is not one. */
		private NicCode autoCandidate(final Nic nic) {
			if (nic.exclusion() != null) {
				return nic.exclusion();
			}
			if (!nic.up()) {
				return NicCode.INTERFACE_DOWN;
			}
			final var reaching = nic.addresses().stream().filter(address -> !onLink(address).isEmpty()).toList();
			if (reaching.isEmpty()) {
				return NicCode.NO_ENDPOINT_ON_SUBNET;
			}
			if (reaching.size() > 1) {
				return NicCode.ADDRESS_AMBIGUOUS;
			}
			final var address = reaching.get(0);
			Integer gidIndex = null;
			if (rdma) {
				final var check = rdmaCheck(nic, address);
				if (check.failure() != null) {
					return check.failure();
				}
				gidIndex = check.gidIndex();
			}
			candidates.put(nic.name(), new Candidate(nic, address, gidIndex, onLink(address)));
			return null;
		}

		private List<Endpoint> onLink(final Ipv4Prefix address) {
			return resolved().stream().filter(endpoint -> address.contains(endpoint.address())).toList();
		}

		// ------------------------------------------------------------ explicit list (item 2)

		private void planList() {
			for (final var entry : interfaces) {
				final var nic = resolveEntry(entry);
				if (nic == null) {
					continue;
				}
				final var name = nic.nic().name();
				if (chosen.containsKey(name)) {
					failure(NicCode.ENTRY_DUPLICATE, "entry " + entry + " names " + name + ", which an earlier entry already names", name, null, "list each NIC once");
					continue;
				}
				chosen.put(name, nic);
			}
			for (final var candidate : chosen.values()) {
				final var name = candidate.nic().name();
				if (!candidate.nic().up()) {
					failure(NicCode.NIC_DOWN, name + " is down", name, null, "bring the interface up or remove it from the list");
				}
				if (candidate.address() != null && candidate.reach().isEmpty()) {
					failure(
									NicCode.NIC_SERVES_NO_ENDPOINT,
									name + " (" + candidate.address() + ") reaches none of the engine's endpoints, on-link or by a route through its device",
									name,
									null,
									"remove it from the list, or add a route through it");
				}
				for (final var endpoint : candidate.reach()) {
					serves.computeIfAbsent(name, key -> new LinkedHashSet<>()).add(endpoint.endpoint());
				}
			}
			for (final var nic : inventory.nics()) {
				if (!chosen.containsKey(nic.name())) {
					reasons.put(nic.name(), new Reason(NicCode.NOT_LISTED, null));
				}
			}
			for (final var endpoint : endpoints) {
				final var kept = chosen.values().stream().filter(candidate -> candidate.reach().contains(endpoint)).map(candidate -> candidate.nic().name()).toList();
				plannedEndpoints.add(new PlannedEndpoint(endpoint.endpoint(), format(endpoint.address()), kept, null, kept, List.of()));
			}
			final var speeds = new TreeSet<Integer>(Comparator.reverseOrder());
			chosen.values().stream().map(candidate -> candidate.nic().speedMbps()).filter(Objects::nonNull).forEach(speeds::add);
			if (speeds.size() > 1) {
				warning(NicCode.MIXED_SPEEDS, "the listed NICs run at different speeds: " + join(speeds.stream().map(speed -> speed + " Mb/s").toList()), null, null, null);
			}
		}

		/** Resolves one list entry to a NIC with its address and reach, or records the failure and returns {@code null}. */
		private Candidate resolveEntry(final String entry) {
			if (entry.isEmpty()) {
				failure(NicCode.ENTRY_UNRESOLVED, "the interface list has an empty entry", null, null, null);
				return null;
			}
			if (entry.indexOf(':') >= 0) {
				failure(NicCode.ENTRY_IPV6, "entry " + entry + " is an IPv6 address; NIC sets are IPv4 only", null, null, "name the NIC or its IPv4 address");
				return null;
			}
			Nic nic;
			Ipv4Prefix address = null;
			final var ip = Ipv4.parse(entry);
			if (ip != null) {
				final var owners = new ArrayList<Nic>();
				for (final var candidate : inventory.nics()) {
					for (final var prefix : candidate.addresses()) {
						if (prefix.address() == ip) {
							owners.add(candidate);
							address = prefix;
						}
					}
				}
				if (owners.size() != 1) {
					unresolvedOrAmbiguous(entry, owners);
					return null;
				}
				nic = owners.get(0);
			} else {
				nic = inventory.nic(entry).orElse(null);
				if (nic == null) {
					// An RDMA device names the interface it serves directly: not the VLANs above it, nor, for RoCE LAG, the bond's members.
					final var attached = inventory.nics().stream()
									.filter(candidate -> entry.equals(candidate.rdmaDevice()) && candidate.kind() != Kind.VLAN && candidate.bondMaster() == null)
									.toList();
					if (attached.size() != 1) {
						unresolvedOrAmbiguous(entry, attached);
						return null;
					}
					nic = attached.get(0);
				}
			}
			final var name = nic.name();
			if (address == null) {
				if (nic.addresses().isEmpty()) {
					failure(NicCode.NIC_NO_IPV4, name + " has no IPv4 address", name, null, null);
					return new Candidate(nic, null, null, List.of());
				}
				final var onSubnet = nic.addresses().stream().filter(prefix -> !onLink(prefix).isEmpty()).toList();
				if (nic.addresses().size() == 1) {
					address = nic.addresses().get(0);
				} else if (onSubnet.size() == 1) {
					address = onSubnet.get(0);
				} else {
					failure(
									NicCode.ENTRY_AMBIGUOUS,
									"entry " + entry + " names " + name + ", which has several IPv4 addresses and "
													+ (onSubnet.isEmpty() ? "none" : "more than one") + " on an endpoint's subnet",
									name,
									null,
									"name the address instead");
					return new Candidate(nic, null, null, List.of());
				}
			}
			Integer gidIndex = null;
			if (rdma) {
				final var check = rdmaCheck(nic, address);
				if (check.failure() != null) {
					failure(check.failure(), name + " (" + Ipv4.format(address.address()) + "): " + check.failure().text(), name, null, null);
				}
				gidIndex = check.gidIndex();
			}
			return new Candidate(nic, address, gidIndex, reach(nic, address));
		}

		private void unresolvedOrAmbiguous(final String entry, final List<Nic> matches) {
			if (matches.isEmpty()) {
				failure(NicCode.ENTRY_UNRESOLVED, "entry " + entry + " is not an interface, an IPv4 address or an RDMA device on this host", null, null, null);
			} else {
				failure(
								NicCode.ENTRY_AMBIGUOUS,
								"entry " + entry + " matches " + join(matches.stream().map(Nic::name).toList()),
								null,
								null,
								"name the interface instead");
			}
		}

		/** On-link, or by a main-table route through the NIC's device (Language: Reaches). */
		private List<Endpoint> reach(final Nic nic, final Ipv4Prefix address) {
			return resolved().stream()
							.filter(endpoint -> address.contains(endpoint.address())
											|| inventory.routes().stream().anyMatch(route -> route.iface().equals(nic.name()) && route.viaGateway() && route.covers(endpoint.address())))
							.toList();
		}

		// ------------------------------------------------------------ RDMA attachment (items 4, 5)

		private RdmaCheck rdmaCheck(final Nic nic, final Ipv4Prefix address) {
			final RdmaDevice device = nic.rdmaDevice() == null ? null : inventory.rdmaDevice(nic.rdmaDevice()).orElse(null);
			if (device == null) {
				return new RdmaCheck(NicCode.NO_RDMA_DEVICE, null);
			}
			// An unknown answer from the native layer does not exclude an mlx5 device; the step's own open decides.
			if (!device.mlx5() || Boolean.FALSE.equals(device.dcSupported())) {
				return new RdmaCheck(NicCode.RDMA_NO_DC, null);
			}
			if (device.port(1).map(port -> !port.activeEthernet()).orElse(true)) {
				return new RdmaCheck(NicCode.RDMA_PORT_NOT_ACTIVE, null);
			}
			final var roceV2 = nic.gids().stream().filter(gid -> gid.roceV2() && gid.address() == address.address() && gid.device().equals(device.name())).toList();
			final var onPort1 = roceV2.stream().filter(gid -> gid.port() == 1).findFirst();
			if (onPort1.isPresent()) {
				return new RdmaCheck(null, onPort1.get().index());
			}
			return new RdmaCheck(roceV2.isEmpty() ? NicCode.NO_ROCEV2_GID : NicCode.ROCEV2_GID_NOT_PORT_1, null);
		}

		// ------------------------------------------------------------ checks (items 5, 6, 6a, 7)

		private void checkCoverage() {
			if (chosen.isEmpty()) {
				return;
			}
			for (final var endpoint : resolved()) {
				if (servers(endpoint).isEmpty()) {
					failure(NicCode.ENDPOINT_UNSERVED, "no chosen NIC serves endpoint " + endpoint.endpoint() + " (" + Ipv4.format(endpoint.address()) + ")", null, endpoint.endpoint(), null);
				}
			}
		}

		private void checkSharedPorts() {
			final var names = List.copyOf(chosen.keySet());
			for (int i = 0; i < names.size(); i++) {
				for (int j = i + 1; j < names.size(); j++) {
					final var a = chosen.get(names.get(i)).nic();
					final var b = chosen.get(names.get(j)).nic();
					if (a.port() == null || !a.port().equals(b.port())) {
						continue;
					}
					final var common = new ArrayList<>(serves.getOrDefault(a.name(), Set.of()));
					common.retainAll(serves.getOrDefault(b.name(), Set.of()));
					if (common.isEmpty()) {
						warning(NicCode.SHARED_PORT_CHOSEN, a.name() + " and " + b.name() + " share port " + a.port() + "; together they add no bandwidth", b.name(), null, null);
					} else {
						failure(
										NicCode.SHARED_PORT_COMMON_ENDPOINT,
										a.name() + " and " + b.name() + " share port " + a.port() + " and both serve " + join(common),
										b.name(),
										common.get(0),
										"list only one NIC of each physical port for these endpoints");
					}
				}
			}
		}

		private void checkHost() {
			final var all = inventory.allSysctls();
			for (final var candidate : chosen.values()) {
				final var nic = candidate.nic();
				final var address = candidate.address();
				if (address != null) {
					final var neighbours = inventory.nics().stream()
									.filter(other -> !other.name().equals(nic.name()) && other.up() && other.addresses().stream().anyMatch(address::sharesSubnetWith))
									.toList();
					if (!neighbours.isEmpty()) {
						final var onSubnet = new ArrayList<Nic>();
						onSubnet.add(nic);
						onSubnet.addAll(neighbours);
						final var shared = nic.name() + " (" + address + ") shares its subnet with " + join(neighbours.stream().map(Nic::name).toList());
						final var arpIgnoreLow = onSubnet.stream().filter(other -> effective(all.arpIgnore(), other.sysctls().arpIgnore()) < 1).map(Nic::name).toList();
						if (!arpIgnoreLow.isEmpty()) {
							failure(NicCode.ARP_FLUX, shared + ", and the effective arp_ignore is 0 on " + join(arpIgnoreLow), nic.name(), null, ARP_FIX);
						}
						final var arpAnnounceLow = onSubnet.stream().filter(other -> effective(all.arpAnnounce(), other.sysctls().arpAnnounce()) < 2).map(Nic::name).toList();
						if (!arpAnnounceLow.isEmpty()) {
							warning(NicCode.ARP_ANNOUNCE_LOW, shared + ", and the effective arp_announce is below 2 on " + join(arpAnnounceLow), nic.name(), null, ARP_FIX);
						}
						if (effective(all.rpFilter(), nic.sysctls().rpFilter()) == 1) {
							failure(NicCode.RP_FILTER_STRICT, shared + ", and its effective rp_filter is 1 (strict)", nic.name(), null, RP_FILTER_FIX);
						}
					}
				}
				if (nic.kind() == Kind.BOND && ("layer2".equals(nic.bondHashPolicy()) || "layer2+3".equals(nic.bondHashPolicy()))) {
					warning(
									NicCode.BOND_HASH_LAYER2,
									nic.name() + " hashes by " + nic.bondHashPolicy() + ", so every connection to one endpoint uses one member link",
									nic.name(),
									null,
									BOND_HASH_FIX);
				}
				if (rdma && nic.rdmaDevice() != null) {
					final var device = inventory.rdmaDevice(nic.rdmaDevice()).orElse(null);
					if (device != null && device.activeMtu() != null && device.activeMtu() < ROCE_MIN_ACTIVE_MTU) {
						warning(NicCode.ROCE_MTU_LOW, nic.name() + " (" + device.name() + ") has RoCE active MTU " + device.activeMtu(), nic.name(), null, ROCE_MTU_FIX);
					}
					if (device != null && device.mlx5() && device.dcSupported() == null) {
						warning(NicCode.RDMA_DC_UNKNOWN, nic.name() + " (" + device.name() + "): the native layer could not report DC support", nic.name(), null, null);
					}
				}
			}
		}

		// ------------------------------------------------------------ pairing (item 7)

		private List<PairingGroup> pair() {
			final var members = new LinkedHashMap<List<String>, List<String>>();
			for (final var endpoint : resolved()) {
				final var servers = servers(endpoint);
				if (!servers.isEmpty()) {
					members.computeIfAbsent(servers, key -> new ArrayList<>()).add(endpoint.endpoint());
				}
			}
			final var groups = new ArrayList<PairingGroup>();
			final var groupsOfNic = new LinkedHashMap<String, Integer>();
			for (final var group : members.entrySet()) {
				final var nics = group.getKey();
				final var groupEndpoints = group.getValue();
				final int n = nics.size();
				final int e = groupEndpoints.size();
				final var paths = new ArrayList<Path>();
				for (int k = 0; k < Math.max(e, n); k++) {
					paths.add(new Path(nics.get(k % n), groupEndpoints.get(k % e)));
				}
				groups.add(new PairingGroup(nics, groupEndpoints, paths));
				if (e % n != 0 && n % e != 0) {
					warning(
									NicCode.UNEVEN_PAIRING,
									n + " NICs (" + join(nics) + ") and " + e + " endpoints: neither count divides the other, so some "
													+ (e > n ? "NICs take more endpoints" : "endpoints take more NICs") + " than others",
									null,
									null,
									null);
				}
				for (final var nic : nics) {
					groupsOfNic.merge(nic, 1, Integer::sum);
				}
			}
			for (final var nic : groupsOfNic.entrySet()) {
				if (nic.getValue() > 1) {
					final var paths = groups.stream().flatMap(group -> group.paths().stream()).filter(path -> path.nic().equals(nic.getKey())).count();
					warning(
									NicCode.NIC_IN_SEVERAL_GROUPS,
									nic.getKey() + " is in " + nic.getValue() + " pairing groups with " + paths
													+ " paths in all; its permits cover every one, so the rate per endpoint is not equal",
									nic.getKey(),
									null,
									null);
				}
			}
			return groups;
		}

		/** The chosen NICs that serve {@code endpoint}, in NIC set order. */
		private List<String> servers(final Endpoint endpoint) {
			return chosen.keySet().stream().filter(name -> serves.getOrDefault(name, Set.of()).contains(endpoint.endpoint())).toList();
		}

		// ------------------------------------------------------------ result

		private List<PlannedNic> plannedNics(final List<PairingGroup> groups) {
			final var nics = new ArrayList<>(inventory.nics());
			nics.sort(Comparator.comparing(Nic::name, NaturalOrder.INSTANCE));
			final var planned = new ArrayList<PlannedNic>();
			for (final var nic : nics) {
				final var name = nic.name();
				final var candidate = chosen.containsKey(name) ? chosen.get(name) : candidates.get(name);
				final int paths = (int) groups.stream().flatMap(group -> group.paths().stream()).filter(path -> path.nic().equals(name)).count();
				planned.add(new PlannedNic(
								name,
								chosen.containsKey(name),
								candidate == null || candidate.address() == null ? null : Ipv4.format(candidate.address().address()),
								candidate == null ? null : candidate.gidIndex(),
								List.copyOf(serves.getOrDefault(name, Set.of())),
								reasons.get(name),
								paths));
			}
			return planned;
		}

		private List<SharedPort> sharedPorts() {
			final var byPort = new LinkedHashMap<String, List<String>>();
			final var nics = new ArrayList<>(inventory.nics());
			nics.sort(Comparator.comparing(Nic::name, NaturalOrder.INSTANCE));
			for (final var nic : nics) {
				if (nic.port() != null) {
					byPort.computeIfAbsent(nic.port(), key -> new ArrayList<>()).add(nic.name());
				}
			}
			return byPort.entrySet().stream().filter(port -> port.getValue().size() > 1).map(port -> new SharedPort(port.getKey(), port.getValue())).toList();
		}

		private void failure(final NicCode code, final String text, final String nic, final String endpoint, final String fix) {
			failures.add(new Finding(code, text, nic, endpoint, fix));
		}

		private void warning(final NicCode code, final String text, final String nic, final String endpoint, final String fix) {
			warnings.add(new Finding(code, text, nic, endpoint, fix));
		}
	}

	/** The kernel applies the larger of {@code all} and the interface's own value; an unread value counts as 0. */
	static int effective(final Integer all, final Integer own) {
		return Math.max(all == null ? 0 : all, own == null ? 0 : own);
	}

	private static String format(final Integer address) {
		return address == null ? null : Ipv4.format(address);
	}

	private static String join(final Collection<String> names) {
		return String.join(", ", names);
	}
}
