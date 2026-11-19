package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DaemonBaseTest {

	private static class TestDaemon extends DaemonBase {
		boolean startCalled, shutdownCalled, stopCalled, closeCalled;

		@Override
		protected void doStart() {
			startCalled = true;
		}

		@Override
		protected void doShutdown() {
			shutdownCalled = true;
		}

		@Override
		protected void doStop() {
			stopCalled = true;
		}

		@Override
		protected void doClose() {
			closeCalled = true;
		}
	}

	@Test
	void closeTransitionsToClosedAndCallsDoClose() throws IOException {
		var d = new TestDaemon();
		d.start();
		d.close();
		assertEquals(State.CLOSED, d.state());
		assertTrue(d.startCalled);
		assertTrue(d.closeCalled);
		assertTrue(d.stopCalled);
		assertTrue(d.shutdownCalled);
	}

	@Test
	void closeFromInitial() throws IOException {
		var d = new TestDaemon();
		d.close();
		assertEquals(State.CLOSED, d.state());
		assertTrue(d.closeCalled);
	}

	@Test
	void closeIsIdempotent() throws IOException {
		var d = new TestDaemon();
		d.start();
		d.close();
		d.close(); // should not throw
		assertEquals(State.CLOSED, d.state());
	}

	@Test
	void closeAllClosesAllOpenDaemons() throws IOException {
		var d1 = new TestDaemon();
		var d2 = new TestDaemon();
		d1.start();
		d2.start();

		DaemonBase.closeAll();

		assertTrue(d1.isClosed());
		assertTrue(d2.isClosed());
		assertTrue(d1.closeCalled);
		assertTrue(d2.closeCalled);
	}

	@Test
	void closeAllSkipsAlreadyClosedDaemons() throws IOException {
		var d1 = new TestDaemon();
		var d2 = new TestDaemon();
		d1.start();
		d2.start();
		d1.close(); // close d1 before closeAll

		DaemonBase.closeAll();

		assertTrue(d1.isClosed());
		assertTrue(d2.isClosed());
		assertTrue(d2.closeCalled);
	}

	@Test
	void closeAllHandlesExceptionInOneDaemon() throws IOException {
		var good = new TestDaemon();
		good.start();

		var bad = new DaemonBase() {
			@Override
			protected void doClose() throws IOException {
				throw new IOException("simulated close failure");
			}
		};
		bad.start();

		// closeAll should not throw — it logs and continues
		DaemonBase.closeAll();

		assertTrue(good.isClosed());
	}

	@Test
	void closeRemovesFromRegistry() throws IOException {
		var d = new TestDaemon();
		d.start();
		d.close(); // removes from registry

		// closeAll after individual close should not fail or re-close
		DaemonBase.closeAll();
		assertTrue(d.isClosed());
	}

	@Test
	void closeAllWithNoDaemons() {
		// Ensure closeAll on empty registry does not throw
		DaemonBase.closeAll();
	}

	@Test
	void closeAllInterruptsAStartInProgressAndClosesTheDaemon() throws Exception {
		final var daemon = new BlockedStartDaemon();
		final var starter = daemon.startInNewThread();
		try {
			closeAllWithinBound();

			assertTrue(daemon.interrupted.await(0, TimeUnit.SECONDS), "the starting thread saw no interrupt");
			assertEquals(State.CLOSED, daemon.state());
		} finally {
			stop(starter);
		}
	}

	@Test
	void closeAllInterruptsEveryStartBeforeItsFirstClose() throws Exception {
		final var starting = new AtomicBoolean();
		final var blockedRef = new AtomicReference<BlockedStartDaemon>();
		final var closedFirst = new DaemonBase() {
			@Override
			protected void doClose() {
				try {
					starting.set(blockedRef.get().interrupted.await(SHORT_WAIT_SECONDS, TimeUnit.SECONDS));
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		};
		final var blocked = new BlockedStartDaemon();
		blockedRef.set(blocked);
		// Registered after closedFirst, so without the first pass its start is interrupted by nothing.
		final var starter = blocked.startInNewThread();
		try {
			closeAllWithinBound();

			assertTrue(closedFirst.isClosed());
			assertTrue(starting.get(), "a close ran before the start it could wait on was interrupted");
		} finally {
			stop(starter);
		}
	}

	@Test
	void closeAllLeavesTheThreadOfAFinishedStartAlone() throws Exception {
		final var daemon = new TestDaemon();
		daemon.start();

		DaemonBase.closeAll();

		assertFalse(Thread.currentThread().isInterrupted());
		assertTrue(daemon.isClosed());
	}

	@Test
	void aFailedStartLeavesNoThreadToInterrupt() throws Exception {
		final var daemon = new DaemonBase() {
			@Override
			protected void doStart() {
				throw new IllegalStateException("simulated start failure");
			}
		};
		assertThrows(IllegalStateException.class, daemon::start);
		final var other = new Thread(daemon::interruptStarter);
		other.start();
		other.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

		assertFalse(Thread.currentThread().isInterrupted());
		daemon.close();
	}

	@Test
	void aStartIsNotInterruptedByItsOwnThread() throws Exception {
		final var interruptedItself = new AtomicBoolean();
		final var daemon = new DaemonBase() {
			@Override
			protected void doStart() {
				interruptStarter();
				interruptedItself.set(Thread.currentThread().isInterrupted());
			}
		};
		daemon.start();
		daemon.close();

		assertFalse(interruptedItself.get());
	}

	private static final int WAIT_SECONDS = 20;
	private static final int SHORT_WAIT_SECONDS = 3;

	/** Runs closeAll on its own thread, as the shutdown hook does: a stuck close must fail the test, not hang it. */
	private static void closeAllWithinBound() throws InterruptedException {
		final var hook = new Thread(DaemonBase::closeAll, "test-close-all");
		hook.setDaemon(true);
		hook.start();
		hook.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
		final boolean stuck = hook.isAlive();
		assertFalse(stuck, "closeAll is still waiting for the start");
	}

	private static void stop(final Thread starter) throws InterruptedException {
		starter.interrupt();
		starter.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
		assertFalse(starter.isAlive());
	}

	/** A daemon whose start holds its state lock until the starting thread is interrupted. */
	private static final class BlockedStartDaemon extends DaemonBase {

		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch interrupted = new CountDownLatch(1);

		@Override
		protected void doStart() {
			entered.countDown();
			try {
				new CountDownLatch(1).await(2 * WAIT_SECONDS, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				interrupted.countDown();
			}
		}

		private Thread startInNewThread() throws InterruptedException {
			final var starter = new Thread(this::start, "test-daemon-start");
			starter.setDaemon(true);
			starter.start();
			assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS), "the start did not begin");
			return starter;
		}
	}
}
