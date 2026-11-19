package com.dell.spt.base.concurrent;

/**
 * Whether the JVM has begun to exit. The engine's shutdown hook sets this before it closes the
 * daemons, and it stays set until the JVM halts: the hook's log drain can take seconds, and no load
 * step may start in that time. A node's {@code /shutdown} and {@code DELETE /run} stop a run without
 * ending the JVM, so they do not set it.
 */
public final class JvmExit {

	private static volatile boolean begun;

	private JvmExit() {}

	/** Called only by the shutdown hook's thread. */
	public static void markBegun() {
		begun = true;
	}

	public static boolean begun() {
		return begun;
	}

	/** For tests: the flag is process-wide and a test JVM runs on after a test sets it. */
	static void reset() {
		begun = false;
	}
}
