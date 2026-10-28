package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.dell.spt.base.nic.RdmaDeviceProbe;
import org.junit.jupiter.api.Test;

/** The NIC inventory's native probe, as the extension registers it. */
class RdmaNativeDeviceProbeTest {

	@Test
	void theExtensionRegistersTheProbe() {
		assertInstanceOf(RdmaNativeDeviceProbe.class, RdmaDeviceProbe.load(getClass().getClassLoader()));
	}

	@Test
	void anUnknownDeviceIsUnknown() {
		// With or without the native library, a device that does not exist has unknown attributes.
		assertEquals(RdmaDeviceProbe.Attributes.UNKNOWN, new RdmaNativeDeviceProbe().query("spt_no_such_device_0"));
	}
}
