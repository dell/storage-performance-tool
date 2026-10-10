package com.dell.spt.storage.driver.coop.netty.endpoint;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/** Strict dotted-quad IPv4 parsing that never performs a name lookup. */
public final class Ipv4Literal {

	private static final int OCTETS = 4;
	private static final int MAX_OCTET = 255;

	private Ipv4Literal() {}

	/**
	 * Parses four decimal octets without leading zeros. Returns empty for anything else, including
	 * hostnames, IPv6 literals and shorthand forms that {@link InetAddress#getByName} would accept.
	 */
	public static Optional<Inet4Address> parse(final String text) {
		if (text == null) {
			return Optional.empty();
		}
		final var parts = text.split("\\.", -1);
		if (parts.length != OCTETS) {
			return Optional.empty();
		}
		final var bytes = new byte[OCTETS];
		for (var i = 0; i < OCTETS; i++) {
			final var octet = parseOctet(parts[i]);
			if (octet < 0) {
				return Optional.empty();
			}
			bytes[i] = (byte) octet;
		}
		try {
			return Optional.of((Inet4Address) InetAddress.getByAddress(bytes));
		} catch (final UnknownHostException e) {
			// Unreachable: a four-byte array is always a valid IPv4 address.
			throw new IllegalStateException(e);
		}
	}

	private static int parseOctet(final String part) {
		if (part.isEmpty() || part.length() > 3 || (part.length() > 1 && part.charAt(0) == '0')) {
			return -1;
		}
		var value = 0;
		for (var i = 0; i < part.length(); i++) {
			final var c = part.charAt(i);
			if (c < '0' || c > '9') {
				return -1;
			}
			value = value * 10 + (c - '0');
		}
		return value <= MAX_OCTET ? value : -1;
	}
}
