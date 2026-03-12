package com.dell.spt.base.concurrent;

import java.io.Closeable;
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
 * <p>Methods declare {@code throws RemoteException} for compatibility with {@link java.rmi.Remote}
 * subinterfaces (e.g. {@link com.dell.spt.base.svc.Service}). Local implementations do not
 * throw it; the declaration exists solely to satisfy the RMI contract.
 */
public interface AsyncRunnable extends Closeable {

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
}
