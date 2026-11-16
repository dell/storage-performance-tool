package com.dell.spt.base.config;

/**
 * A value a YAML or JSON defaults document gives where the setting takes text: a number or boolean
 * for a text setting, a list or map for a URI argument, request header or object tag, or no value
 * for a request header. The configuration would fail with a cast, so the document is refused when it
 * is read. The message names the setting and never quotes the value, which may be a credential.
 */
public final class InvalidTextValueException extends IllegalArgumentException {

	private final String path;

	/**
	 * @param path the setting the value was refused for
	 * @param message names the setting, not the value
	 */
	public InvalidTextValueException(final String path, final String message) {
		super(message);
		this.path = path;
	}

	/** The setting the value was refused for. */
	public String path() {
		return path;
	}
}
