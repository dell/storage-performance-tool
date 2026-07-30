package com.dell.spt.base.integrity;

/** Typed terminal cause for metadata-mode setup, digest, artifact, or publication failures. */
public final class IntegrityTerminalException extends IllegalStateException {

	public enum Category {
		CONFIGURATION, DIGEST, ARTIFACT, PUBLICATION
	}

	private final Category category;

	public IntegrityTerminalException(
					final Category category, final String message, final Throwable cause) {
		super(message, cause);
		this.category = category;
	}

	public IntegrityTerminalException(final Category category, final String message) {
		super(message);
		this.category = category;
	}

	public Category category() {
		return category;
	}
}
