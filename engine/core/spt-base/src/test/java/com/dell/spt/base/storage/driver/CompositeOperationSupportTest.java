package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

/** The step-start check that refuses a ranges threshold on a driver without composite support. */
class CompositeOperationSupportTest {

	@Test
	void aDriverSupportsCompositeOperationsUnlessItSaysOtherwise() {
		final StorageDriver<?, ?> driver = mock(StorageDriver.class, CALLS_REAL_METHODS);
		assertTrue(driver.supportsCompositeOperations(), "the interface default, kept by a driver built outside the engine");
	}

	@Test
	void aThresholdOnADriverWithoutSupportIsRefusedForCreateAndRead() {
		final StorageDriver<?, ?> driver = driver(false);
		for (final OpType opType : new OpType[]{OpType.CREATE, OpType.READ
		}) {
			final var failure = assertThrows(
							IllegalConfigurationException.class,
							() -> CompositeOperationSupport.requireSupport(driver, opType, 1048576),
							opType.name());
			assertEquals(
							"The storage driver type \"fs\" does not support composite (multipart) operations, but"
											+ " item.data.ranges.threshold=1048576 would make composite " + opType
											+ " operations; set item.data.ranges.threshold to 0 for this driver",
							failure.getMessage());
		}
	}

	@Test
	void aThresholdOfZeroOrAnOpTypeWithoutCompositesOrADriverWithSupportPasses() {
		final StorageDriver<?, ?> unsupported = driver(false);
		for (final OpType opType : OpType.values()) {
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, 0), opType.name());
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, -1), opType.name());
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(driver(true), opType, 1048576), opType.name());
		}
		for (final OpType opType : new OpType[]{OpType.NOOP, OpType.UPDATE, OpType.DELETE, OpType.STAT, OpType.LIST
		}) {
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, 1048576), opType.name());
		}
	}

	private static StorageDriver<?, ?> driver(final boolean supported) {
		final StorageDriver<?, ?> driver = mock(StorageDriver.class);
		when(driver.supportsCompositeOperations()).thenReturn(supported);
		when(driver.driverType()).thenReturn("fs");
		return driver;
	}
}
