package com.dell.spt.base.config;

import java.util.List;
import java.util.Map;

/**
 * A value a YAML or JSON defaults document gives where the setting takes text, and that would fail
 * with a cast: a number or boolean for a text setting every run reads, refused when the document is
 * read; or a list or map URI argument, request header or object tag, or a request header with no
 * value, refused by the Netty HTTP drivers where they read those maps. The message names the
 * setting and never quotes the value, which may be a credential.
 */
public final class InvalidTextValueException extends IllegalArgumentException {

	private final String path;

	/**
	 * Refuses a list or map entry of a map setting whose entries are text.
	 *
	 * @param path the entry's setting, such as {@code storage-net-http-headers-X-Id}
	 * @param value the entry's value
	 * @throws InvalidTextValueException if the value is a list or map
	 */
	public static void refuseListOrMap(final String path, final Object value) {
		if (value instanceof List<?> || value instanceof Map<?, ?>) {
			throw new InvalidTextValueException(path, path + ": the value is "
							+ (value instanceof List<?> ? "a list" : "a map") + ", not text");
		}
	}

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
