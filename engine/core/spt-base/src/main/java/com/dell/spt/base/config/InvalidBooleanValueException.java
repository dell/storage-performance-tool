package com.dell.spt.base.config;

/**
 * A value other than {@code true} or {@code false}, in any case, for a setting the schema types as a
 * boolean. The configuration would read it as false, so it is refused wherever it is given.
 */
public final class InvalidBooleanValueException extends IllegalArgumentException {

	private final String path;

	public InvalidBooleanValueException(final String message) {
		this(null, message);
	}

	/**
	 * @param path the boolean setting, or {@code null} when not known
	 * @param message names the setting and quotes the refused value
	 */
	public InvalidBooleanValueException(final String path, final String message) {
		super(message);
		this.path = path;
	}

	/** The boolean setting the value was refused for, or {@code null} when not known. */
	public String path() {
		return path;
	}
}
