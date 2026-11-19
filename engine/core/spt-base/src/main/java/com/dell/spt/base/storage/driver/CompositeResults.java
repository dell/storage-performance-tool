package com.dell.spt.base.storage.driver;

/**
 * How a storage driver publishes the results of a composite operation, one above
 * {@code item-data-ranges-threshold}: see {@link StorageDriver#compositeResults()}.
 */
public enum CompositeResults {

	/**
	 * The driver does not say. A step that needs to know, such as a count-limited {@code mixed}
	 * step, treats it as it did before drivers could say.
	 */
	UNDECLARED,

	/**
	 * After a composite CREATE's or READ's successful, unfinished result (a multipart upload's
	 * initiation), the driver publishes its parts' results and one more result for the composite.
	 * A split READ's result can come before the result of a range that ended at the same moment, so
	 * do not rely on the order. A composite of another type, which only an operations builder
	 * outside the engine makes, has one result.
	 */
	IN_PARTS,

	/** The driver publishes one result for each composite operation, as for any other. */
	ONE_RESULT
}
