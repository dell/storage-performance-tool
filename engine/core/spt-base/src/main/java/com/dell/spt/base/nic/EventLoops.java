package com.dell.spt.base.nic;

import java.util.Objects;

/**
 * One storage driver's Netty event loops, fixed when it builds its groups.
 *
 * @param route the route its loops serve
 * @param driverThreads the configured {@code storage.driver.threads}, or 0 for the default
 * @param eventLoops every event loop the driver owns, counted from its groups as created: the
 *     default route's group, or a NIC set's per-NIC groups and its base group
 */
public record EventLoops(EventLoopRoute route, int driverThreads, int eventLoops) {

	/** The source a report names when {@code storage.driver.threads} is set. */
	public static final String SOURCE_DRIVER_THREADS = "storage.driver.threads";

	/** The source a report names when the driver's default sized its groups. */
	public static final String SOURCE_DEFAULT = "default";

	public EventLoops {
		Objects.requireNonNull(route, "route");
		if (driverThreads < 0 || eventLoops < 0) {
			throw new IllegalArgumentException(
							"negative count: driverThreads=" + driverThreads + ", eventLoops=" + eventLoops);
		}
	}

	/** A NIC-set driver's loops: its per-NIC groups and its base group. */
	public static EventLoops of(final NicSetResources resources) {
		return new EventLoops(EventLoopRoute.NIC_SET, resources.driverThreads(), resources.totalEventLoops());
	}

	/** What sized the groups: {@code storage.driver.threads} when it is set, otherwise the default. */
	public String source() {
		return driverThreads > 0 ? SOURCE_DRIVER_THREADS : SOURCE_DEFAULT;
	}
}
