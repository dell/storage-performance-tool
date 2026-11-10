package com.dell.spt.storage.driver.coop.netty;

/**
 * The Netty drivers' default event-loop counts, used when {@code storage.driver.threads} is not
 * set: {@code max(FLOOR, hardware threads / divisor)}, with one divisor per route. A count is read
 * once, when a driver builds its groups.
 */
public final class EventLoopDefaults {

	/** No default group has fewer loops. */
	public static final int FLOOR = 4;

	/**
	 * The default route's single group, one loop per 24 hardware threads. It was 8 before 5.17: on a
	 * large client, a third as many loops carried as much or more for less client CPU per operation.
	 */
	public static final int DEFAULT_ROUTE_DIVISOR = 24;

	/** Each NIC's group of an HTTP NIC set, whose loops carry every payload (D49). */
	public static final int HTTP_PER_NIC_DIVISOR = 8;

	/**
	 * Each NIC's group while payloads move over RDMA (D83), whose loops carry the HTTP exchange of each
	 * request and only the payloads that stay on HTTP.
	 */
	public static final int RDMA_PER_NIC_DIVISOR = 48;

	private EventLoopDefaults() {}

	/** The default route's group size on a host with {@code hardwareThreads}. */
	public static int defaultRoute(final int hardwareThreads) {
		return count(hardwareThreads, DEFAULT_ROUTE_DIVISOR);
	}

	/** Each NIC's group size of an HTTP NIC set on a host with {@code hardwareThreads}. */
	public static int httpPerNic(final int hardwareThreads) {
		return count(hardwareThreads, HTTP_PER_NIC_DIVISOR);
	}

	/** Each NIC's group size while payloads move over RDMA, on a host with {@code hardwareThreads}. */
	public static int rdmaPerNic(final int hardwareThreads) {
		return count(hardwareThreads, RDMA_PER_NIC_DIVISOR);
	}

	private static int count(final int hardwareThreads, final int divisor) {
		return Math.max(FLOOR, hardwareThreads / divisor);
	}
}
