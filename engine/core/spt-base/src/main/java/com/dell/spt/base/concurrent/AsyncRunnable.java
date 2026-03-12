package com.dell.spt.base.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
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
 *
 * <p>This interface extends {@link Remote} so that all lifecycle methods are eligible for
 * RMI invocation when a concrete class is exported as a remote object. Local (non-exported)
 * implementations are unaffected — {@code Remote} is a marker interface with no methods.
 */
public interface AsyncRunnable extends Closeable, Remote {

	enum State {
		INITIAL, STARTED, SHUTDOWN, STOPPED, CLOSED
	}

	State state() throws RemoteException;

	boolean isInitial() throws RemoteException;

	boolean isStarted() throws RemoteException;

	boolean isShutdown() throws RemoteException;

	boolean isStopped() throws RemoteException;

	boolean isClosed() throws RemoteException;

	AsyncRunnable start() throws RemoteException;

	AsyncRunnable shutdown() throws RemoteException;

	AsyncRunnable stop() throws RemoteException;

	AsyncRunnable await() throws InterruptedException, RemoteException;

	boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException, RemoteException;

	/**
	 * Redeclared from {@link Closeable} so that RMI considers it a remote method.
	 * Without this explicit override, RMI traces {@code close()} back to {@code Closeable}
	 * which does not extend {@link Remote}, causing {@code RemoteException: Method is not Remote}.
	 */
	@Override
	void close() throws IOException;
}
