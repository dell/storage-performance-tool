package com.dell.spt.base.config;

/**
 * An engine command-line argument that cannot be applied: an argument without the {@code --}
 * prefix, an unknown option, a value the option's type cannot take, or an option the engine
 * refuses. The engine stops before it starts a node or a scenario and exits with status 2.
 *
 * <p>The message names the argument and is printed as it is, so it must carry no credential
 * values.
 */
public final class CliArgumentException extends RuntimeException {

	public CliArgumentException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
