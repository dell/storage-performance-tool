package com.dell.spt.base.storage.driver;

/**
 * Optional storage-driver capability for preparing resources used only by standalone DELETE.
 *
 * <p>The load-step lifecycle invokes this after the driver starts and before DELETE admission or
 * measurement begins. Implementations must be safe to invoke once per driver instance and must
 * complete synchronously so setup time cannot leak into request or scheduled-phase timing.
 */
@FunctionalInterface
public interface StandaloneDeletePreparable {

	/** Prepares resources required by first-class standalone DELETE requests. */
	void prepareStandaloneDelete();
}
