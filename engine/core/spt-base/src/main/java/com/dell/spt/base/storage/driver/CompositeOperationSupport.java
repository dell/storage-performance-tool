package com.dell.spt.base.storage.driver;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;

/**
 * The step-start check that refuses a composite (multipart) configuration on a storage driver
 * without composite support: one that implements {@link CompositeOperationsUnsupported}.
 */
public final class CompositeOperationSupport {

	private CompositeOperationSupport() {}

	/**
	 * Refuses a step that would build composite operations on a driver that does not run them: an
	 * op type with a composite implementation (CREATE or READ) and a size threshold above 0. Any
	 * other op type, a threshold of 0 or less, or a driver with composite support passes.
	 *
	 * @param driver the step's storage driver
	 * @param opType the op type the step builds
	 * @param sizeThreshold the configured {@code item-data-ranges-threshold}, in bytes
	 * @throws IllegalConfigurationException naming the driver type, the op type and the setting
	 */
	public static void requireSupport(
					final StorageDriver<?, ?> driver, final OpType opType, final long sizeThreshold)
					throws IllegalConfigurationException {
		if (sizeThreshold > 0
						&& DataOperationsBuilderImpl.buildsCompositeOperations(opType)
						&& driver instanceof CompositeOperationsUnsupported) {
			throw new IllegalConfigurationException(
							"The storage driver type \"" + driver.driverType()
											+ "\" does not support composite (multipart) operations, but item.data.ranges.threshold="
											+ sizeThreshold + " would make composite " + opType
											+ " operations; set item.data.ranges.threshold to 0 for this driver");
		}
	}
}
