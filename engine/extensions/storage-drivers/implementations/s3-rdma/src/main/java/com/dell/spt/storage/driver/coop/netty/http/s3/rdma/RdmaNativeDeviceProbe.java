package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.nic.RdmaDeviceProbe;

/**
 * The NIC inventory's RDMA probe: DC support and the RoCE active MTU from the native library.
 * Without the library, or with one that predates the query, both are unknown.
 */
public final class RdmaNativeDeviceProbe implements RdmaDeviceProbe {

	@Override
	public Attributes query(final String device) {
		final var result = RdmaTransport.queryDevice(device);
		if (result == null || result.length < 2) {
			return Attributes.UNKNOWN;
		}
		return new Attributes(result[0] < 0 ? null : result[0] == 1, result[1] > 0 ? result[1] : null);
	}
}
