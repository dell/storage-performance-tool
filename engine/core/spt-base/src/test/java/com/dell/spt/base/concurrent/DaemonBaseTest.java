package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
