package com.dell.spt.base.item.io;

import java.io.IOException;

/**
 * Marks an item-input failure that must terminate the producing load step instead of being treated
 * as ordinary end-of-input.
 *
 * <p>This exception is intentionally narrow. Inputs retain the legacy warning-and-EOF behavior
 * unless their implementation explicitly uses this type for a correctness-critical discovery
 * boundary.
 */
public final class TerminalItemInputException extends IOException {

	public TerminalItemInputException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
