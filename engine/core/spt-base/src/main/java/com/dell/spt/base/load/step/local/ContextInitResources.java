package com.dell.spt.base.load.step.local;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.logging.LogUtil;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.logging.log4j.Level;

/**
 * What a local step has built for one load step context while that context is not yet in the step's
 * list, where the step's close would reach it. Unless {@link #handedOver()} was called, closing this
 * closes what it holds, the latest first, and leaves no partly built daemon registered. It never
 * throws, so the failure that ended the init is the one the caller sees.
 */
public final class ContextInitResources implements AutoCloseable {

	private final DaemonBase.ConstructionScope construction = DaemonBase.openConstructionScope();
	private final Deque<AutoCloseable> held = new ArrayDeque<>();

	/** Closes {@code resource} if the context is not handed over; {@code null} is ignored. */
	public void hold(final AutoCloseable resource) {
		if (resource != null) {
			held.push(resource);
		}
	}

	/** {@code owner} now closes {@code owned}, which must not be closed a second time. */
	public void ownedBy(final AutoCloseable owner, final AutoCloseable... owned) {
		for (final var resource : owned) {
			held.removeIf(candidate -> candidate == resource);
		}
		held.push(owner);
	}

	/** The step's list has the context: the step closes all of it from here on. */
	public void handedOver() {
		held.clear();
		construction.succeeded();
	}

	@Override
	public void close() {
		try {
			for (final var resource : held) {
				try {
					resource.close();
				} catch (final Throwable e) {
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
					LogUtil.exception(Level.WARN, e, "Failed to close {} after a failed step init", resource);
				}
			}
			held.clear();
		} finally {
			construction.close();
		}
	}
}
