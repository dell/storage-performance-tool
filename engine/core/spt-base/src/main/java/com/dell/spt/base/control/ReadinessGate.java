package com.dell.spt.base.control;

import java.util.concurrent.atomic.AtomicBoolean;

/** Simple thread-safe readiness flag shared with the readiness servlet. */
public final class ReadinessGate {
	private final AtomicBoolean ready = new AtomicBoolean(false);

	public void setReady(final boolean value) {
		ready.set(value);
	}

	public boolean isReady() {
		return ready.get();
	}
}
