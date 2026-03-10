package com.dell.spt.base.concurrent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Thread-safe state machine implementing the {@link AsyncRunnable} lifecycle.
 *
 * <p>Subclasses override the template methods ({@code doStart}, {@code doShutdown},
 * {@code doStop}, {@code doClose}) to hook into state transitions. All transitions
 * are serialized under a {@link ReentrantLock}; waiters on {@link #await} are
 * notified via a {@link Condition}.
 *
 * <p>Invalid transitions are silently ignored (logged at TRACE level) to match
 * the original java-commons behavior that callers depend on.
 */
public abstract class AsyncRunnableBase implements AsyncRunnable {

	private static final Logger LOG = LogManager.getLogger(AsyncRunnableBase.class);

	private volatile State state = State.INITIAL;
	private final Lock stateLock = new ReentrantLock();
	private final Condition stateChanged = stateLock.newCondition();

	// ---- State accessors ----

	@Override
	public final State state() {
		return state;
	}

	@Override
	public boolean isInitial() {
		return State.INITIAL == state;
	}

	@Override
	public boolean isStarted() {
		return State.STARTED == state;
	}

	@Override
	public boolean isShutdown() {
		return State.SHUTDOWN == state;
	}

	@Override
	public boolean isStopped() {
		return State.STOPPED == state;
	}

	@Override
	public boolean isClosed() {
		return State.CLOSED == state;
	}

	// ---- Lifecycle transitions ----

	@Override
	public final AsyncRunnableBase start() {
		stateLock.lock();
		try {
			if (State.INITIAL == state || State.STOPPED == state) {
				doStart();
				state = State.STARTED;
				stateChanged.signalAll();
			} else {
				LOG.trace("Not starting — already in state {}", state);
			}
		} finally {
			stateLock.unlock();
		}
		return this;
	}

	@Override
	public final AsyncRunnableBase shutdown() {
		stateLock.lock();
		try {
			if (State.STARTED == state || State.INITIAL == state) {
				doShutdown();
				state = State.SHUTDOWN;
				stateChanged.signalAll();
			} else {
				LOG.trace("Not shutting down — already in state {}", state);
			}
		} finally {
			stateLock.unlock();
		}
		return this;
	}

	@Override
	public final AsyncRunnableBase stop() {
		shutdown();
		stateLock.lock();
		try {
			if (State.SHUTDOWN == state) {
				doStop();
				state = State.STOPPED;
				stateChanged.signalAll();
			} else {
				LOG.trace("Not stopping — already in state {}", state);
			}
		} finally {
			stateLock.unlock();
		}
		return this;
	}

	@Override
	public final AsyncRunnableBase await() throws InterruptedException {
		await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		return this;
	}

	@Override
	public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
		long remainingNanos = timeUnit.toNanos(timeout);
		stateLock.lock();
		try {
			while (State.STARTED == state || State.SHUTDOWN == state) {
				if (remainingNanos <= 0) {
					return false;
				}
				remainingNanos = stateChanged.awaitNanos(remainingNanos);
			}
		} finally {
			stateLock.unlock();
		}
		return true;
	}

	@Override
	public void close() throws IOException {
		stop();
		stateLock.lock();
		try {
			if (State.CLOSED != state) {
				doClose();
				state = State.CLOSED;
				stateChanged.signalAll();
			}
		} finally {
			stateLock.unlock();
		}
	}

	// ---- Template methods for subclasses ----

	protected void doStart() {}

	protected void doShutdown() {}

	protected void doStop() {}

	protected void doClose() throws IOException {}
}
