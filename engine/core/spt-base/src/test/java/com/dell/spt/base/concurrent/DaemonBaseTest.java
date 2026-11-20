package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
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

	private static final class ReleaseRecordingDaemon extends DaemonBase {
		private final List<ReleaseRecordingDaemon> released;
		private final boolean releaseThrows;

		ReleaseRecordingDaemon(final List<ReleaseRecordingDaemon> released, final boolean releaseThrows) {
			this.released = released;
			this.releaseThrows = releaseThrows;
		}

		@Override
		protected void releaseAfterFailedConstruction() {
			released.add(this);
			if (releaseThrows) {
				throw new IllegalStateException("simulated release failure");
			}
		}
	}

	private static boolean registered(final Daemon daemon) {
		return DaemonBase.isRegistered(candidate -> candidate == daemon);
	}

	@Test
	void aFailedConstructionScopeReleasesItsDaemonsLatestFirstAndUnregistersThem() {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final ReleaseRecordingDaemon first;
		final ReleaseRecordingDaemon second;
		try (final var scope = DaemonBase.openConstructionScope()) {
			first = new ReleaseRecordingDaemon(released, false);
			second = new ReleaseRecordingDaemon(released, false);
			assertTrue(registered(first));
		}

		assertEquals(List.of(second, first), released);
		assertFalse(registered(first));
		assertFalse(registered(second));
	}

	@Test
	void aSucceededConstructionScopeLeavesItsDaemonsRegisteredAndUnreleased() throws IOException {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final ReleaseRecordingDaemon daemon;
		try (final var scope = DaemonBase.openConstructionScope()) {
			daemon = new ReleaseRecordingDaemon(released, false);
			scope.succeeded();
		}

		assertEquals(List.of(), released);
		assertTrue(registered(daemon));
		daemon.close();
	}

	@Test
	void anEnclosingScopeThatFailsReleasesWhatAnInnerScopeBuilt() {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final ReleaseRecordingDaemon inner;
		try (final var outer = DaemonBase.openConstructionScope()) {
			try (final var scope = DaemonBase.openConstructionScope()) {
				inner = new ReleaseRecordingDaemon(released, false);
				scope.succeeded();
			}
			assertEquals(List.of(), released);
		}

		assertEquals(List.of(inner), released);
		assertFalse(registered(inner));
	}

	@Test
	void aReleaseThatThrowsDoesNotStopTheOthersAndAClosedDaemonIsNotReleased() throws IOException {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final ReleaseRecordingDaemon survivor;
		final ReleaseRecordingDaemon throwing;
		final ReleaseRecordingDaemon closed;
		try (final var scope = DaemonBase.openConstructionScope()) {
			survivor = new ReleaseRecordingDaemon(released, false);
			throwing = new ReleaseRecordingDaemon(released, true);
			closed = new ReleaseRecordingDaemon(released, false);
			closed.close();
		}

		assertEquals(List.of(throwing, survivor), released);
		assertFalse(registered(throwing));
		assertFalse(registered(survivor));
	}

	@Test
	void aLogHandlerThatThrowsDoesNotStopTheRelease() {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final Handler throwingHandler = new Handler() {
			@Override
			public void publish(final LogRecord record) {
				throw new IllegalStateException("simulated log handler failure");
			}

			@Override
			public void flush() {}

			@Override
			public void close() {}
		};
		final Logger logger = Logger.getLogger(DaemonBase.class.getSimpleName());
		logger.addHandler(throwingHandler);
		final ReleaseRecordingDaemon earlier;
		final ReleaseRecordingDaemon throwing;
		try {
			try (final var scope = DaemonBase.openConstructionScope()) {
				earlier = new ReleaseRecordingDaemon(released, false);
				throwing = new ReleaseRecordingDaemon(released, true);
			}
		} finally {
			logger.removeHandler(throwingHandler);
		}

		assertEquals(List.of(throwing, earlier), released);
		assertFalse(registered(throwing));
		assertFalse(registered(earlier));
	}

	@Test
	void aLogHandlerThatRethrowsTheReleaseFailureDoesNotStopTheRelease() {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		final Handler rethrowingHandler = new Handler() {
			@Override
			public void publish(final LogRecord record) {
				if (record.getThrown() instanceof RuntimeException failure) {
					throw failure;
				}
			}

			@Override
			public void flush() {}

			@Override
			public void close() {}
		};
		final Logger logger = Logger.getLogger(DaemonBase.class.getSimpleName());
		logger.addHandler(rethrowingHandler);
		final ReleaseRecordingDaemon earlier;
		final ReleaseRecordingDaemon throwing;
		try {
			try (final var scope = DaemonBase.openConstructionScope()) {
				earlier = new ReleaseRecordingDaemon(released, false);
				throwing = new ReleaseRecordingDaemon(released, true);
			}
		} finally {
			logger.removeHandler(rethrowingHandler);
		}

		assertEquals(List.of(throwing, earlier), released);
		assertFalse(registered(throwing));
		assertFalse(registered(earlier));
	}

	@Test
	void aDaemonBuiltAfterTheScopeClosedIsNotRecorded() throws IOException {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		DaemonBase.openConstructionScope().close();

		try (final var later = new ReleaseRecordingDaemon(released, false)) {
			assertTrue(registered(later));
		}
		assertEquals(List.of(), released);
	}

	/**
	 * A close failure caught by {@code closeAll} is a WARN on the engine's {@code Loggers.ERR} (Log4j), with
	 * the cause attached and the daemon's class named, not a java.util.logging record, which the JVM's own
	 * shutdown handling loses at exit.
	 */
	@Test
	void closeAllLogsACloseFailureAsAWarningOnTheErrorsLogger() throws Exception {
		final var failure = new IOException("simulated close failure " + System.nanoTime());
		final var bad = new FailingCloseDaemon(failure);
		bad.start();
		final var appender = new CapturingAppender(failure);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			DaemonBase.closeAll();
			// Engine loggers are asynchronous. Other tests leave daemons registered and closeAll logs them too,
			// so only this test's throwable identifies its record.
			assertTrue(appender.awaitFirst(5, TimeUnit.SECONDS), "no record carries the close failure");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		final var events = appender.events();
		assertEquals(1, events.size(), events.toString());
		final var event = events.get(0);
		assertEquals(Level.WARN, event.level());
		assertSame(failure, event.thrown());
		assertTrue(event.message().contains(FailingCloseDaemon.class.getName()), event.message());
	}

	private static final class FailingCloseDaemon extends DaemonBase {
		private final IOException failure;

		FailingCloseDaemon(final IOException failure) {
			this.failure = failure;
		}

		@Override
		protected void doClose() throws IOException {
			throw failure;
		}
	}

	private record CapturedEvent(Level level, Throwable thrown, String message) {}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<CapturedEvent> captured = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch first = new CountDownLatch(1);
		private final Throwable marker;

		CapturingAppender(final Throwable marker) {
			super("testDaemonCloseFailureCapture", null, null, true, Property.EMPTY_ARRAY);
			this.marker = marker;
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getThrown() == marker) {
				captured.add(new CapturedEvent(
								event.getLevel(), event.getThrown(), event.getMessage().getFormattedMessage()));
				first.countDown();
			}
		}

		boolean awaitFirst(final long timeout, final TimeUnit unit) throws InterruptedException {
			return first.await(timeout, unit);
		}

		List<CapturedEvent> events() {
			return List.copyOf(captured);
		}
	}
}
