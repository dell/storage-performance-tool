package com.dell.spt.base.data;

import com.github.akurilov.commons.system.SizeInBytes;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataInputTest {

	@Test
	public void testMemoryTypeAllocation()
					throws Exception {
		try (final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1"), 1, false)) {
			assertTrue(dataInput.getLayer(0).isDirect());
		}
		try (final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1"), 1, true)) {
			assertFalse(dataInput.getLayer(0).isDirect());
		}
	}
}
