package com.dell.spt.base.item.io;

/**
 * What an item input that keeps going past a bad record can say afterwards. {@link CsvItemInput}
 * logs a malformed item and returns as it does at the end of input, so a reader that must know the
 * whole file was read (D54-R1) asks here. The workload's own parsing does not change.
 */
public interface ItemInputStatus {

	/** The malformed items met so far. */
	long malformedItems();

	/** The first malformed item's line, or {@code null} when there was none. */
	String firstMalformedItem();
}
