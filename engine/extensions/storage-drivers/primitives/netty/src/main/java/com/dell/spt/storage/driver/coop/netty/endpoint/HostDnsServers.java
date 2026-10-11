package com.dell.spt.storage.driver.coop.netty.endpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the IPv4 name servers of the host resolver configuration in file order. There is no
 * fallback: a file without a usable IPv4 name server is an error, never a hard-coded public
 * resolver. Search domains and options are intentionally ignored.
 */
public final class HostDnsServers {

	private static final String NAMESERVER = "nameserver";

	/** Usable servers in file order and the entries skipped because they are not IPv4 literals. */
	public record Discovery(List<InetSocketAddress> servers, List<String> ignored) {}

	private HostDnsServers() {}

	public static Discovery read(final Path resolvConf) throws IOException {
		final List<InetSocketAddress> servers = new ArrayList<>();
		final List<String> ignored = new ArrayList<>();
		for (final var line : Files.readAllLines(resolvConf, StandardCharsets.UTF_8)) {
			final var tokens = line.strip().split("\\s+", -1);
			if (tokens.length < 2 || !NAMESERVER.equals(tokens[0])) {
				continue;
			}
			final var address = Ipv4Literal.parse(tokens[1]);
			if (address.isPresent()) {
				servers.add(new InetSocketAddress(address.get(), EndpointSelectionConstants.DNS_PORT));
			} else {
				ignored.add(tokens[1]);
			}
		}
		if (servers.isEmpty()) {
			throw new IOException("No usable IPv4 nameserver in " + resolvConf
							+ (ignored.isEmpty() ? "" : " (ignored non-IPv4 entries: " + ignored + ")")
							+ "; configure an explicit DNS server");
		}
		return new Discovery(List.copyOf(servers), List.copyOf(ignored));
	}
}
