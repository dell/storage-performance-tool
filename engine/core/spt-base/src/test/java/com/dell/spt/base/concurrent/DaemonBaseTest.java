package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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
	void aDaemonBuiltAfterTheScopeClosedIsNotRecorded() throws IOException {
		final List<ReleaseRecordingDaemon> released = new ArrayList<>();
		DaemonBase.openConstructionScope().close();

		try (final var later = new ReleaseRecordingDaemon(released, false)) {
			assertTrue(registered(later));
		}
		assertEquals(List.of(), released);
	}
}
