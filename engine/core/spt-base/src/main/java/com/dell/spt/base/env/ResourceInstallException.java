package com.dell.spt.base.env;

/**
 * A bundled resource that could not be installed into the app home: the installed copy is missing or
 * differs from the bundled one. Its message explains the refusal and is printed as it is; the engine
 * stops with exit status 1.
 */
public final class ResourceInstallException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ResourceInstallException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
