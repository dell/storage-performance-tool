package com.dell.spt.base.concurrent;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle contract for an asynchronous component with start/shutdown/stop/close semantics.
 *
 * <p>State machine:
 * <pre>
 * INITIAL ──start()──► STARTED ──shutdown()──► SHUTDOWN ──stop()──► STOPPED ──► start() ...
 *                                                                       │
 * ANY (except CLOSED) ─────────────────── close() ──────────────────► CLOSED
 * </pre>
 */
public interface AsyncRunnable extends Closeable {

	enum State {
		INITIAL, STARTED, SHUTDOWN, STOPPED, CLOSED
	}

	State state();

	boolean isInitial();

	boolean isStarted();

	boolean isShutdown();

	boolean isStopped();

	boolean isClosed();

	AsyncRunnable start();

	AsyncRunnable shutdown();

	AsyncRunnable stop();

	AsyncRunnable await() throws InterruptedException;

	boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException;
}
