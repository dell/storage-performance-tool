package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.nic.EndpointResolver.HostPort;
import com.dell.spt.base.nic.NicPlan;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The NICs and Paths of one driver, taken from a successful plan: each chosen NIC with its IPv4
 * address and its Paths over every pairing group, each Path with the configured endpoint string
 * and the address the endpoint was resolved to once for the step (item 7a).
 */
public record NicSetLayout(List<Nic> nics) {

	public NicSetLayout {
		nics = List.copyOf(nics);
	}

	/** A chosen NIC and its Paths, in pairing order. */
	public record Nic(String name, Inet4Address address, List<PathSpec> paths) {

		public Nic {
			paths = List.copyOf(paths);
		}
	}

	/** One Path's endpoint: the configured string, which keys idle connections, and its frozen destination. */
	public record PathSpec(String endpoint, InetSocketAddress destination) {}

	/**
	 * Builds the layout from {@code plan}, whose endpoints are the driver's {@code host:port}
	 * strings.
	 *
	 * @throws IllegalArgumentException when the plan failed or misses an address or a port
	 */
	public static NicSetLayout from(final NicPlan plan) {
		if (!plan.ok()) {
			throw new IllegalArgumentException("A failed plan has no layout: " + failureMessage(plan));
		}
		final Map<String, InetSocketAddress> destinations = new HashMap<>();
		for (final var endpoint : plan.endpoints()) {
			final var port = HostPort.parse(endpoint.endpoint()).port();
			if (endpoint.address() == null || port == null) {
				throw new IllegalArgumentException("Endpoint " + endpoint.endpoint() + " has no IPv4 address and port");
			}
			destinations.put(endpoint.endpoint(), new InetSocketAddress(ipv4(endpoint.address()), port));
		}
		final Map<String, List<PathSpec>> paths = new LinkedHashMap<>();
		plan.nicSet().forEach(nic -> paths.put(nic, new ArrayList<>()));
		for (final var group : plan.groups()) {
			for (final var path : group.paths()) {
				paths.get(path.nic()).add(new PathSpec(path.endpoint(), destinations.get(path.endpoint())));
			}
		}
		final List<Nic> nics = new ArrayList<>(paths.size());
		for (final var planned : plan.nics()) {
			if (planned.used()) {
				nics.add(new Nic(planned.name(), ipv4(planned.address()), paths.get(planned.name())));
			}
		}
		// Keep the plan's NIC-set order, which is the list order or, under auto, the name order.
		final Map<String, Nic> byName = nics.stream().collect(Collectors.toMap(Nic::name, nic -> nic));
		return new NicSetLayout(plan.nicSet().stream().map(byName::get).toList());
	}

	/** The plan's failures as one line: each finding's text, with its fix when it has one. */
	public static String failureMessage(final NicPlan plan) {
		return plan.failures().stream()
						.map(failure -> failure.fix() == null ? failure.text() : failure.text() + " (" + failure.fix() + ")")
						.collect(Collectors.joining("; "));
	}

	private static Inet4Address ipv4(final String literal) {
		try {
			// A literal never reaches DNS.
			return (Inet4Address) InetAddress.getByName(literal);
		} catch (final UnknownHostException | ClassCastException e) {
			throw new IllegalArgumentException("Not an IPv4 address: " + literal, e);
		}
	}
}
