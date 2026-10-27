package com.dell.spt.base.nic;

import java.util.ServiceLoader;

/**
 * Reads the RDMA device attributes that sysfs does not show. The {@code s3-rdma} extension
 * implements it with its native library, so this module needs none.
 */
public interface RdmaDeviceProbe {

	/** What the probe found; a null field is unknown. */
	record Attributes(Boolean dcSupported, Integer activeMtu) {

		public static final Attributes UNKNOWN = new Attributes(null, null);
	}

	/** A probe that knows nothing, used without {@code --rdma} or without the extension. */
	RdmaDeviceProbe NONE = device -> Attributes.UNKNOWN;

	/** Port 1's attributes of the named device. Never throws: a failed query is unknown. */
	Attributes query(String device);

	/** The first probe the class loader provides, or {@link #NONE}. */
	static RdmaDeviceProbe load(final ClassLoader classLoader) {
		for (final var probe : ServiceLoader.load(RdmaDeviceProbe.class, classLoader)) {
			return probe;
		}
		return NONE;
	}
}
