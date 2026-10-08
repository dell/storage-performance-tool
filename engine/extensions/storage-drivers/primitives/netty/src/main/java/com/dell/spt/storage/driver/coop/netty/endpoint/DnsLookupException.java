package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.handler.codec.dns.DnsResponseCode;
import io.netty.resolver.dns.DnsErrorCauseException;
import io.netty.resolver.dns.DnsNameResolverTimeoutException;
import java.io.IOException;

/** A failed per-request DNS lookup, classified for counters and diagnostics. */
public final class DnsLookupException extends IOException {

	private static final long serialVersionUID = 1L;

	/** Why a lookup produced no usable IPv4 address. */
	public enum Kind {
		/** The total lookup deadline expired, or every query timed out. */
		TIMEOUT,
		/** The server answered NXDOMAIN. */
		NOT_FOUND,
		/** The name exists but the answer held no usable A record (NODATA), or a CNAME chain failed. */
		NO_ADDRESS,
		/** The server answered with another error code, such as SERVFAIL or REFUSED. */
		SERVER_FAILURE,
		/** The resolver closed before the lookup finished, normally because the driver stopped. */
		CANCELLED,
	}

	private final Kind kind;

	DnsLookupException(final Kind kind, final String hostname, final Throwable cause) {
		super("DNS lookup of " + hostname + " failed: " + kind, cause);
		this.kind = kind;
	}

	public Kind kind() {
		return kind;
	}

	/** Maps a Netty resolver failure onto a lookup failure kind. */
	static DnsLookupException classify(final String hostname, final Throwable failure) {
		for (var cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof DnsNameResolverTimeoutException) {
				return new DnsLookupException(Kind.TIMEOUT, hostname, failure);
			}
			if (cause instanceof DnsErrorCauseException dnsError) {
				final var kind = DnsResponseCode.NXDOMAIN.equals(dnsError.getCode())
								? Kind.NOT_FOUND
								: Kind.SERVER_FAILURE;
				return new DnsLookupException(kind, hostname, failure);
			}
		}
		return new DnsLookupException(Kind.NO_ADDRESS, hostname, failure);
	}
}
