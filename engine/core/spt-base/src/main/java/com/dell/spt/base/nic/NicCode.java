package com.dell.spt.base.nic;

import java.util.Locale;

/**
 * The stable codes of the NIC plan, each with its English text. The step-start log, {@code spt
 * verify} and the image command use these words; verify keys pass, warn and fail on the code and
 * on the list a finding appears in, never on the text. Codes are only ever added.
 */
public enum NicCode {

	// Why an interface is not used under auto (item 4)
	INTERFACE_DOWN("interface down"), VIRTUAL("virtual"), NOT_ETHERNET("not a wired Ethernet interface"), BOND_MEMBER("bond member"), NO_ENDPOINT_ON_SUBNET("no endpoint on its subnet"), ADDRESS_AMBIGUOUS("several of its IPv4 addresses reach endpoints"), NO_RDMA_DEVICE("no RDMA device"), RDMA_NO_DC("RDMA device without DC support"), RDMA_PORT_NOT_ACTIVE("RDMA port not active"), NO_ROCEV2_GID("no RoCE v2 GID"), ROCEV2_GID_NOT_PORT_1("RoCE v2 GID on a port other than 1"), SHARES_PORT("shares a port"), SLOWER_SPEED_TIER("slower speed tier"), SPEED_UNKNOWN_BESIDE_KNOWN("speed unknown, beside a NIC of known speed"), NOT_LISTED("not in the interface list"),

	// Failures
	ENTRY_UNRESOLVED("entry does not resolve"), ENTRY_AMBIGUOUS("entry is ambiguous"), ENTRY_DUPLICATE("entry names a NIC already listed"), ENTRY_IPV6("entry is an IPv6 address"), AUTO_MIXED("auto mixed with other entries"), ENDPOINT_NO_IPV4("endpoint has no IPv4 address"), ENDPOINT_DUPLICATE("endpoint given twice"), NIC_DOWN("NIC is down"), NIC_NO_IPV4("NIC has no IPv4 address"), NIC_SERVES_NO_ENDPOINT("NIC serves no endpoint"), ENDPOINT_UNSERVED("no chosen NIC serves the endpoint"), SHARED_PORT_COMMON_ENDPOINT("NICs on one port serve a common endpoint"), ARP_FLUX("ARP flux on a shared subnet"), RP_FILTER_STRICT("strict reverse-path filtering on a shared subnet"), AUTO_NONE_ELIGIBLE("no eligible NIC"),

	// Warnings
	ROCE_MTU_LOW("RoCE active MTU below 4096"), RDMA_DC_UNKNOWN("DC support of the RDMA device is unknown"), ARP_ANNOUNCE_LOW("arp_announce below 2 on a shared subnet"), BOND_HASH_LAYER2("bond hashes by layer 2"), MIXED_SPEEDS("chosen NICs have different link speeds"), UNEVEN_PAIRING("uneven pairing"), NIC_IN_SEVERAL_GROUPS("NIC in several pairing groups"), SHARED_PORT_CHOSEN("chosen NICs share a physical port"), ENDPOINT_SAME_ADDRESS("endpoints resolve to the same address and port");

	private final String text;

	NicCode(final String text) {
		this.text = text;
	}

	/** The code as it appears in the plan output, such as {@code shares_port}. */
	public String id() {
		return name().toLowerCase(Locale.ROOT);
	}

	public String text() {
		return text;
	}
}
