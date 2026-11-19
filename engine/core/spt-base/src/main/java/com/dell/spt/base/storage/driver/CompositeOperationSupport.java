package com.dell.spt.base.storage.driver;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.confuse.Config;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The step-start check that refuses a composite (multipart) configuration on a storage driver
 * without composite support: one whose runtime class declares {@link CompositeOperationsUnsupported}
 * itself (see {@link #declaresNoCompositeSupport}).
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
	 * @throws Refusal naming the driver type, the op type and the setting
	 */
	public static void requireSupport(
					final StorageDriver<?, ?> driver, final OpType opType, final long sizeThreshold)
					throws IllegalConfigurationException {
		if (sizeThreshold > 0
						&& DataOperationsBuilderImpl.buildsCompositeOperations(opType)
						&& declaresNoCompositeSupport(driver)) {
			throw new Refusal(
							"The storage driver type \"" + driver.driverType()
											+ "\" does not support composite (multipart) operations, but item.data.ranges.threshold="
											+ sizeThreshold + " would make composite " + opType
											+ " operations; set item.data.ranges.threshold to 0 for this driver");
		}
	}

	/**
	 * Whether {@code driver}'s runtime class names {@link CompositeOperationsUnsupported} in its own
	 * {@code implements} clause. A marker inherited from a superclass or a superinterface does not
	 * count, so a subclass of a refusing driver runs as it did before the marker existed.
	 */
	public static boolean declaresNoCompositeSupport(final StorageDriver<?, ?> driver) {
		for (final Class<?> declared : driver.getClass().getInterfaces()) {
			if (declared == CompositeOperationsUnsupported.class) {
				return true;
			}
		}
		return false;
	}

	/**
	 * The configured {@code item-data-ranges-threshold}, in bytes, from the {@code item-data-ranges}
	 * configuration: a size such as {@code 1MB}, or a number.
	 */
	public static long sizeThreshold(final Config rangesConfig) {
		final var raw = rangesConfig.val("threshold");
		if (raw instanceof String size) {
			return BinarySizeFormat.parseFixedSize(size);
		}
		return TypeUtil.typeConvert(raw, long.class);
	}

	/** The refusal in {@code failure}'s chain of causes, or {@code null} when there is none. */
	public static Refusal find(final Throwable failure) {
		final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		for (var cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
			if (cause instanceof Refusal refusal) {
				return refusal;
			}
		}
		return null;
	}

	/**
	 * The refusal of a composite configuration on a driver without composite support. Its message
	 * names only the driver type, the op type, the threshold in bytes and the setting to change, so
	 * the run API can show it.
	 */
	public static final class Refusal extends IllegalConfigurationException {

		private static final long serialVersionUID = 1L;

		Refusal(final String message) {
			super(message);
		}
	}
}
