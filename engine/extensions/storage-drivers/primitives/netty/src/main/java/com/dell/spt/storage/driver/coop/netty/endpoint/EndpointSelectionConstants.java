package com.dell.spt.storage.driver.coop.netty.endpoint;

import java.nio.file.Path;

/** Shared constants for opt-in endpoint selection. */
public final class EndpointSelectionConstants {

	/** Standard DNS port used when an explicit DNS server omits its port. */
	public static final int DNS_PORT = 53;

	/** Host resolver configuration read when no explicit DNS server is configured. */
	public static final Path RESOLV_CONF = Path.of("/etc/resolv.conf");

	/**
	 * How long a per-request connection waits for the server to close it after the response. The
	 * request carries {@code Connection: close}; a server-side close keeps TIME_WAIT off the client's
	 * ephemeral ports. The client closes after this grace period otherwise.
	 */
	public static final long SERVER_CLOSE_GRACE_MILLIS = 1_000;

	/** Distinct destinations counted individually per driver; further ones are counted as "other". */
	public static final int MAX_TRACKED_DESTINATIONS = 256;

	/** Upper bound for closing resolver and connection resources owned by a driver. */
	public static final long CLOSE_TIMEOUT_MILLIS = 5_000;

	private EndpointSelectionConstants() {}
}
