package com.dell.spt.base.nic;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/** Resolves an endpoint's host to its first IPv4 answer (item 7a). */
@FunctionalInterface
public interface EndpointResolver {

	/** The first IPv4 address of {@code host}, or {@code null} when it has none. */
	Integer resolveIpv4(String host);

	EndpointResolver SYSTEM = host -> {
		final var literal = Ipv4.parse(host);
		if (literal != null) {
			return literal;
		}
		try {
			for (final var address : InetAddress.getAllByName(host)) {
				if (address instanceof Inet4Address) {
					return Ipv4.parse(address.getHostAddress());
				}
			}
		} catch (final UnknownHostException | SecurityException e) {
			// no answer
		}
		return null;
	};

	/** The host and optional port of a configured endpoint string. */
	record HostPort(String host, Integer port) {

		/** Splits {@code host}, {@code host:port}, {@code [v6]} and {@code [v6]:port}; a bare IPv6 address has no port. */
		public static HostPort parse(final String endpoint) {
			final var text = endpoint.trim();
			if (text.startsWith("[")) {
				final var close = text.indexOf(']');
				if (close > 0) {
					final var rest = text.substring(close + 1);
					return new HostPort(text.substring(1, close), rest.startsWith(":") ? port(rest.substring(1)) : null);
				}
			}
			final var colon = text.indexOf(':');
			if (colon < 0 || colon != text.lastIndexOf(':')) {
				return new HostPort(text, null);
			}
			return new HostPort(text.substring(0, colon), port(text.substring(colon + 1)));
		}

		private static Integer port(final String text) {
			try {
				return Integer.parseInt(text);
			} catch (final NumberFormatException e) {
				return null;
			}
		}
	}
}
