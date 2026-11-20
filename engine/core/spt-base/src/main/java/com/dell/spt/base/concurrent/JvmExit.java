package com.dell.spt.base.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Whether the JVM has begun to exit. The engine's shutdown hook sets this before it closes the
 * daemons, and it stays set until the JVM halts: the hook's log drain can take seconds, and no load
 * step may start in that time. A node's {@code /shutdown} and {@code DELETE /run} stop a run without
 * ending the JVM, so they do not set it.
 */
public final class JvmExit {

	private static final AtomicBoolean START_STOP_REPORTED = new AtomicBoolean();
	private static volatile boolean begun;

	private JvmExit() {}

	/** Called only by the shutdown hook's thread. */
	public static void markBegun() {
		begun = true;
	}

	public static boolean begun() {
		return begun;
	}

	/**
	 * Whether the caller, whose step start has just been stopped, is the one to report that the exit
	 * stopped it. A step's start runs its local slice's inside it, and the exit stops both.
	 */
	public static boolean claimStartStopReport() {
		return begun && START_STOP_REPORTED.compareAndSet(false, true);
	}

	/** For tests: the state is process-wide and a test JVM runs on after a test sets it. */
	static void reset() {
		begun = false;
		START_STOP_REPORTED.set(false);
	}
}
