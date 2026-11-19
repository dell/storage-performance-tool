package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import java.io.IOException;
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
	void aDaemonClosedByAFailingCloseLeavesTheRegistry() throws Exception {
		final var failure = new IOException("after close");
		final var d = new TestDaemon() {
			@Override
			protected void afterClosed() throws IOException {
				throw failure;
			}
		};
		assertTrue(registered(d));

		assertSame(failure, assertThrows(IOException.class, d::close));

		assertTrue(d.isClosed());
		assertFalse(registered(d));
	}

	@Test
	void aDaemonWhoseCloseFailsStaysInTheRegistry() throws Exception {
		final var d = new TestDaemon() {
			@Override
			protected void doClose() {
				throw new IllegalStateException("close failed");
			}
		};

		assertThrows(IllegalStateException.class, d::close);

		assertFalse(d.isClosed());
		assertTrue(registered(d), "closeAll still closes it");
	}

	private static boolean registered(final Daemon daemon) throws ReflectiveOperationException {
		final var field = DaemonBase.class.getDeclaredField("REGISTRY");
		field.setAccessible(true);
		for (final Object ref : (java.util.Collection<?>) field.get(null)) {
			if (((java.lang.ref.WeakReference<?>) ref).get() == daemon) {
				return true;
			}
		}
		return false;
	}
}
