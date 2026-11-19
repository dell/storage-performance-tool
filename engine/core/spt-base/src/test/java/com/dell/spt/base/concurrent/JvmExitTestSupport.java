package com.dell.spt.base.concurrent;

/** Lets tests in other packages clear {@link JvmExit}; each one that sets it must do so afterwards. */
public final class JvmExitTestSupport {

	private JvmExitTestSupport() {}

	public static void reset() {
		JvmExit.reset();
	}
}
