package com.dell.spt.storage.driver.coop.netty.endpoint;

import java.nio.file.Path;

/** Shared constants for opt-in endpoint selection. */
public final class EndpointSelectionConstants {

	/** Standard DNS port used when an explicit DNS server omits its port. */
	public static final int DNS_PORT = 53;

	/** Host resolver configuration read when no explicit DNS server is configured. */
	public static final Path RESOLV_CONF = Path.of("/etc/resolv.conf");

	/** Upper bound for closing resolver and connection resources owned by a driver. */
	public static final long CLOSE_TIMEOUT_MILLIS = 5_000;

	private EndpointSelectionConstants() {}
}
