package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.AsyncRunnable.State;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class AsyncRunnableBaseTest {

	/** Concrete subclass that records which template methods were called. */
	private static class TestRunnable extends AsyncRunnableBase {
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
	void initialState() {
		var r = new TestRunnable();
		assertEquals(State.INITIAL, r.state());
		assertTrue(r.isInitial());
		assertFalse(r.isStarted());
		assertFalse(r.isShutdown());
		assertFalse(r.isStopped());
		assertFalse(r.isClosed());
	}

	@Test
	void startFromInitial() {
		var r = new TestRunnable();
		var result = r.start();
		assertSame(r, result);
		assertEquals(State.STARTED, r.state());
		assertTrue(r.startCalled);
	}

	@Test
	void shutdownFromStarted() {
		var r = new TestRunnable();
		r.start();
		r.shutdown();
		assertEquals(State.SHUTDOWN, r.state());
		assertTrue(r.shutdownCalled);
	}

	@Test
	void shutdownFromInitial() {
		var r = new TestRunnable();
		r.shutdown();
		assertEquals(State.SHUTDOWN, r.state());
		assertTrue(r.shutdownCalled);
	}

	@Test
	void stopCallsShutdownThenStop() {
		var r = new TestRunnable();
		r.start();
		r.stop();
		assertEquals(State.STOPPED, r.state());
		assertTrue(r.shutdownCalled);
		assertTrue(r.stopCalled);
	}

	@Test
	void closeFromStartedCallsStopAndClose() throws IOException {
		var r = new TestRunnable();
		r.start();
		r.close();
		assertEquals(State.CLOSED, r.state());
		assertTrue(r.shutdownCalled);
		assertTrue(r.stopCalled);
		assertTrue(r.closeCalled);
	}

	@Test
	void closeIsIdempotent() throws IOException {
		var r = new TestRunnable();
		r.start();
		r.close();
		r.close(); // should not throw
		assertEquals(State.CLOSED, r.state());
	}

	@Test
	void startFromStopped() {
		var r = new TestRunnable();
		r.start();
		r.stop();
		r.start();
		assertEquals(State.STARTED, r.state());
	}

	@Test
	void startFromStartedIsIgnored() {
		var r = new TestRunnable();
		r.start();
		r.start(); // silently ignored
		assertEquals(State.STARTED, r.state());
	}

	@Test
	void shutdownFromStoppedIsIgnored() {
		var r = new TestRunnable();
		r.start();
		r.stop();
		r.shutdown(); // silently ignored — already past SHUTDOWN
		assertEquals(State.STOPPED, r.state());
	}

	@Test
	void awaitReturnsImmediatelyWhenStopped() throws Exception {
		var r = new TestRunnable();
		r.start();
		r.stop();
		assertTrue(r.await(100, TimeUnit.MILLISECONDS));
	}

	@Test
	void awaitTimesOutWhenStarted() throws Exception {
		var r = new TestRunnable();
		r.start();
		assertFalse(r.await(50, TimeUnit.MILLISECONDS));
	}

	@Test
	void awaitUnblocksOnStop() throws Exception {
		var r = new TestRunnable();
		r.start();
		var awaited = new AtomicBoolean(false);
		var latch = new CountDownLatch(1);
		var t = Thread.ofVirtual().start(() -> {
			try {
				latch.countDown();
				r.await();
				awaited.set(true);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(latch.await(1, TimeUnit.SECONDS));
		Thread.sleep(50); // let await() block
		r.stop();
		t.join(2000);
		assertTrue(awaited.get(), "await() should have returned after stop()");
	}

	@Test
	void awaitUnblocksOnClose() throws Exception {
		var r = new TestRunnable();
		r.start();
		var awaited = new AtomicBoolean(false);
		var latch = new CountDownLatch(1);
		var t = Thread.ofVirtual().start(() -> {
			try {
				latch.countDown();
				r.await();
				awaited.set(true);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(latch.await(1, TimeUnit.SECONDS));
		Thread.sleep(50);
		r.close();
		t.join(2000);
		assertTrue(awaited.get(), "await() should have returned after close()");
	}
}
