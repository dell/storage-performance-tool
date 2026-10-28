package com.dell.spt.base.nic;

/** IPv4 addresses as unsigned 32-bit values held in an {@code int}, and their dotted form. */
public final class Ipv4 {

	private Ipv4() {}

	/** Parses a dotted-quad IPv4 address; returns {@code null} for anything else, IPv6 included. */
	public static Integer parse(final String text) {
		if (text == null) {
			return null;
		}
		final var parts = text.split("\\.", -1);
		if (parts.length != 4) {
			return null;
		}
		int address = 0;
		for (final var part : parts) {
			if (part.isEmpty() || part.length() > 3 || !part.chars().allMatch(Character::isDigit)) {
				return null;
			}
			final int octet = Integer.parseInt(part);
			if (octet > 255) {
				return null;
			}
			address = (address << 8) | octet;
		}
		return address;
	}

	public static String format(final int address) {
		return ((address >>> 24) & 0xff)
						+ "."
						+ ((address >>> 16) & 0xff)
						+ "."
						+ ((address >>> 8) & 0xff)
						+ "."
						+ (address & 0xff);
	}

	public static int mask(final int prefixLength) {
		return prefixLength == 0 ? 0 : -1 << (32 - prefixLength);
	}
}
