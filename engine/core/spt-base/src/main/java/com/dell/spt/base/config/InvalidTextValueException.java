package com.dell.spt.base.config;

/**
 * A value a YAML or JSON defaults document gives where the setting takes text, and that would fail
 * with a cast: a number or boolean for a text setting every run reads, a list or map for a URI
 * argument or request header, or no value for a request header, refused when the document is read;
 * or a list or map object tag, refused by the Netty {@code s3} driver. The message names the setting
 * and never quotes the value, which may be a credential.
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
