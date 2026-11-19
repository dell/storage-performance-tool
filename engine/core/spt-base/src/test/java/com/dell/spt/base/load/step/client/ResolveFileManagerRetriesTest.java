package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.svc.DroppedConnectTarget;
import com.dell.spt.base.svc.ServiceUtilTestAccess;
import java.lang.reflect.Proxy;
import java.rmi.ConnectException;
import java.rmi.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The node resolve loop of {@link LoadStepClientBase}, with an injected resolver and sleeper. */
class ResolveFileManagerRetriesTest {

	private static final String NODE = "127.0.0.1:1";
	private static final int MAX_RETRIES = 10;
	private static final FileManagerService SERVICE = (FileManagerService) Proxy.newProxyInstance(
					FileManagerService.class.getClassLoader(), new Class<?>[]{FileManagerService.class
					},
					(proxy, method, args) -> {
						throw new UnsupportedOperationException(method.getName());
					});

	private final List<Long> sleeps = new ArrayList<>();
	private int tries;

	/** Records each sleep and, like {@link Thread#sleep(long)}, throws at once when interrupted. */
	private final LoadStepClientBase.RetrySleeper sleeper = millis -> {
		sleeps.add(millis);
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
	};

	@AfterEach
	void clearInterrupt() {
		Thread.interrupted();
	}

	@Test
	void aRefusedNodeIsTriedTenTimesWithNoSleepAfterTheLastTry() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			tries++;
			throw new ConnectException("Connection refused to host: " + addr);
		}, sleeper);

		assertNull(fms);
		assertEquals(10, tries);
		assertEquals(
						List.of(2000L, 4000L, 8000L, 16000L, 32000L, 64000L, 120000L, 120000L, 120000L),
						sleeps);
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void aNodeFoundOnTheThirdTryStopsTheRetries() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			if (++tries < 3) {
				throw new ConnectException("Connection refused to host: " + addr);
			}
			return SERVICE;
		}, sleeper);

		assertSame(SERVICE, fms);
		assertEquals(3, tries);
		assertEquals(List.of(2000L, 4000L), sleeps);
	}

	@Test
	void anUnknownHostIsTriedOnce() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			tries++;
			throw new UnknownHostException("Unknown host: " + addr);
		}, sleeper);

		assertNull(fms);
		assertEquals(1, tries);
		assertEquals(List.of(), sleeps);
	}

	@Test
	void anyOtherFailureIsTriedOnce() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			tries++;
			throw new java.rmi.ConnectIOException("Exception creating connection to: " + addr);
		}, sleeper);

		assertNull(fms);
		assertEquals(1, tries);
		assertEquals(List.of(), sleeps);
	}

	@Test
	void anInterruptDuringASleepEndsTheRetriesWithTheFlagSet() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			if (++tries == 4) {
				Thread.currentThread().interrupt();
			}
			throw new ConnectException("Connection refused to host: " + addr);
		}, sleeper);

		assertNull(fms);
		assertEquals(4, tries);
		assertEquals(List.of(2000L, 4000L, 8000L, 16000L), sleeps);
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void anInterruptDuringTheLastTryRecordsNoSleepAfterIt() {
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			if (++tries == MAX_RETRIES) {
				Thread.currentThread().interrupt();
			}
			throw new ConnectException("Connection refused to host: " + addr);
		}, sleeper);

		assertNull(fms);
		assertEquals(MAX_RETRIES, tries);
		assertEquals(9, sleeps.size());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	@Test
	void anInterruptBeforeTheFirstTryMakesNoTry() {
		Thread.currentThread().interrupt();
		final var fms = LoadStepClientBase.resolveFileManagerWithRetries(NODE, MAX_RETRIES, addr -> {
			tries++;
			return SERVICE;
		}, sleeper);

		assertNull(fms);
		assertEquals(0, tries);
		assertEquals(List.of(), sleeps);
		assertTrue(Thread.currentThread().isInterrupted());
	}

	/** A connect that times out is retried like a refused one, on the same schedule. */
	@Test
	void aTimedOutConnectIsRetried() throws Exception {
		try (DroppedConnectTarget target = new DroppedConnectTarget()) {
			final var fms = LoadStepClientBase.resolveFileManagerWithRetries(
							target.host() + ":" + target.port(), 3, addr -> {
								tries++;
								return resolveWithTimeout(addr, 200);
							}, sleeper);

			assertNull(fms);
			assertEquals(3, tries);
			assertEquals(List.of(2000L, 4000L), sleeps);
		}
	}

	/**
	 * An interrupt cannot end a connect, but the connect timeout bounds it: the loop then makes no
	 * further try and returns null with the interrupt flag set.
	 */
	@Test
	void anInterruptDuringADroppedTryReturnsWithinTheTimeout() throws Exception {
		final int timeoutMillis = 2_000;
		try (DroppedConnectTarget target = new DroppedConnectTarget()) {
			final AtomicInteger realTries = new AtomicInteger();
			final AtomicReference<FileManagerService> result = new AtomicReference<>(SERVICE);
			final AtomicBoolean interruptedAfter = new AtomicBoolean();
			final Thread resolving = new Thread(() -> {
				result.set(LoadStepClientBase.resolveFileManagerWithRetries(
								target.host() + ":" + target.port(), MAX_RETRIES, addr -> {
									realTries.incrementAndGet();
									return resolveWithTimeout(addr, timeoutMillis);
								}, Thread::sleep));
				interruptedAfter.set(Thread.currentThread().isInterrupted());
			}, "resolve-dropped");
			resolving.start();
			Thread.sleep(200);
			final long interruptNanos = System.nanoTime();
			resolving.interrupt();
			resolving.join(timeoutMillis + 5_000);
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - interruptNanos);

			assertFalse(resolving.isAlive(), "the resolve did not return");
			assertTrue(elapsedMillis < timeoutMillis + 1_000, () -> "returned " + elapsedMillis + " ms after the interrupt");
			assertEquals(1, realTries.get());
			assertNull(result.get());
			assertTrue(interruptedAfter.get());
		}
	}

	private static FileManagerService resolveWithTimeout(final String addr, final int timeoutMillis)
					throws Exception {
		return ServiceUtilTestAccess.resolveWithConnectTimeout(
						addr, FileManagerService.SVC_NAME, FileManagerService.class, timeoutMillis);
	}
}
