package com.dell.spt.base.nic;

/** One IPv4 address of an interface with the prefix length of its connected subnet. */
public record Ipv4Prefix(int address, int prefixLength) {

	public Ipv4Prefix {
		if (prefixLength < 0 || prefixLength > 32) {
			throw new IllegalArgumentException("IPv4 prefix length out of range: " + prefixLength);
		}
	}

	/** Parses {@code a.b.c.d/len}. */
	public static Ipv4Prefix parse(final String text) {
		final var slash = text.indexOf('/');
		if (slash < 0) {
			throw new IllegalArgumentException("Missing prefix length: " + text);
		}
		final var address = Ipv4.parse(text.substring(0, slash));
		if (address == null) {
			throw new IllegalArgumentException("Not an IPv4 address: " + text);
		}
		return new Ipv4Prefix(address, Integer.parseInt(text.substring(slash + 1)));
	}

	/** Whether {@code other} lies in this address's connected subnet. */
	public boolean contains(final int other) {
		final var mask = Ipv4.mask(prefixLength);
		return (address & mask) == (other & mask);
	}

	/** Whether either address lies in the other's subnet. */
	public boolean sharesSubnetWith(final Ipv4Prefix other) {
		return contains(other.address) || other.contains(address);
	}

	@Override
	public String toString() {
		return Ipv4.format(address) + "/" + prefixLength;
	}
}
