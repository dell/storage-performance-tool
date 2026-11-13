package com.dell.spt.base.config;

/**
 * A value other than {@code true} or {@code false}, in any case, for a setting the schema types as a
 * boolean. The configuration would read it as false, so it is refused wherever it is given.
 */
public final class InvalidBooleanValueException extends IllegalArgumentException {

	public InvalidBooleanValueException(final String message) {
		super(message);
	}
}
