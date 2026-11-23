package com.dell.spt.base.load.step.client;

/**
 * Tells an interrupt from a missing file when the entry collects a node's step files, so a
 * collector stops at once on an interrupt instead of taking it for one more missing node.
 */
final class CollectionInterrupt {

	/** Deeper cause chains than any wrapping here makes; also ends a cycle. */
	private static final int MAX_CAUSES = 32;

	private CollectionInterrupt() {}

	/**
	 * Whether {@code failure} ends the collection as an interrupt: the thread's interrupt flag is
	 * set, as file I/O on an interrupted thread leaves it, or the failure or one of its causes is an
	 * {@link InterruptedException}, in which case the flag is set again for the caller. A
	 * {@code ClosedByInterruptException} alone is no interrupt here: one sent back by a worker whose
	 * own thread was interrupted counts as that node missing. A socket timeout is no interrupt.
	 */
	static boolean isInterrupt(final Throwable failure) {
		if (Thread.currentThread().isInterrupted()) {
			return true;
		}
		Throwable cause = failure;
		for (int depth = 0; cause != null && depth < MAX_CAUSES; depth++) {
			if (cause instanceof InterruptedException) {
				Thread.currentThread().interrupt();
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}
}
