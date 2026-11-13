package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import java.lang.ref.WeakReference;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The SigV4 signing caches are per driver and per thread. A thread that signed for a driver and
 * outlives it, such as the entry engine's scenario thread, must not keep the closed driver
 * reachable.
 */
class S3SigningCacheRetentionTest {

	private static final int DRIVERS = 5;
	private static final long COLLECT_SECONDS = 10;

	private final NicSetLoopback stub = new NicSetLoopback();

	@AfterEach
	void stopStub() {
		stub.close();
	}

	@Test
	void closeOnTheSigningThreadEmptiesItsCachesAndTheCachedFunctionHoldsNoDriver() throws Exception {
		final var driver = new S3StorageDriver<Item, Operation<Item>>(
						"signing-retention", null, plainStorage(), false, 1);
		sign(driver);
		final var functions = driver.signingKeyCreateFunctionCache.get();
		assertEquals(1, functions.size(), "one signing function for the one secret");
		assertHoldsNoDriver(functions.get(CREDENTIAL.getSecret()), driver);
		// SPT's own function does not fill the date-keyed cache; an extension's function does, and
		// close clears it.
		assertTrue(driver.signingKeyCache.get().isEmpty(), "SPT's own keys are not in the date-keyed cache");
		driver.signingKeyCache.get().put("20261113", new byte[0]);

		driver.close();

		// get() after remove() starts a new empty map; without remove() it returns the old one.
		assertTrue(driver.signingKeyCreateFunctionCache.get().isEmpty(), "the closing thread's functions were cleared");
		assertTrue(driver.signingKeyCache.get().isEmpty(), "the closing thread's keys were cleared");
		driver.signingKeyCreateFunctionCache.remove();
		driver.signingKeyCache.remove();
	}

	@Test
	void aFailedCloseStillEmptiesTheClosingThreadsCaches() throws Exception {
		final var driver = new S3StorageDriver<Item, Operation<Item>>(
						"signing-retention", null, plainStorage(), false, 1);
		sign(driver);
		driver.signingKeyCache.get().put("20261113", new byte[0]);
		final var cleanups = multipartCleanupTasks(driver);
		cleanups.add(CompletableFuture.failedFuture(new IllegalStateException("a failed multipart cleanup")));
		try {
			assertThrows(CompletionException.class, driver::close, "the cleanup's failure ends close");
			assertTrue(driver.signingKeyCreateFunctionCache.get().isEmpty(), "the closing thread's functions were cleared");
			assertTrue(driver.signingKeyCache.get().isEmpty(), "the closing thread's keys were cleared");
		} finally {
			// The failed close left the driver open; with the failed task gone, close finishes.
			cleanups.clear();
			driver.close();
			driver.signingKeyCreateFunctionCache.remove();
			driver.signingKeyCache.remove();
		}
	}

	@Test
	void controlDriversThatSignedOnThisThreadAreCollectableAfterClose() throws Exception {
		final var storage = controlStorage();
		final List<WeakReference<Object>> closed = new ArrayList<>();
		for (var i = 0; i < DRIVERS; i++) {
			closed.add(checkAndClose(storage, i));
		}
		awaitCollected(closed);
	}

	@Test
	void plainDriversThatSignedOnThisThreadAreCollectableAfterClose() throws Exception {
		final var storage = plainStorage();
		final List<WeakReference<Object>> closed = new ArrayList<>();
		for (var i = 0; i < DRIVERS; i++) {
			closed.add(signAndClose(storage, i));
		}
		awaitCollected(closed);
	}

	@Test
	void driversClosedOnAnotherThreadThanTheSignerAreCollectable() throws Exception {
		final var storage = controlStorage();
		final List<WeakReference<Object>> closed = new ArrayList<>();
		final List<ThreadLocal<Map<String, SigningKeyCreateFunction>>> caches = new ArrayList<>();
		for (var i = 0; i < DRIVERS; i++) {
			closed.add(checkHereAndCloseElsewhere(storage, i, caches));
		}
		// The caches are held here, so this thread's entries stay and are not expunged as stale: only
		// the cached functions' own references could keep the drivers reachable.
		for (final var cache : caches) {
			assertEquals(1, cache.get().size(), "the signing thread keeps its entry when another thread closes");
		}
		awaitCollected(closed);
		for (final var cache : caches) {
			cache.remove();
		}
	}

	private Config controlStorage() throws Exception {
		final var storage = NicSetLoopback.storage(stub.listeners(1), List.of(), 1);
		storage.val("net-transport", "nio");
		return storage;
	}

	private static Config plainStorage() {
		return S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9024").configVal("storage");
	}

	private static WeakReference<Object> checkAndClose(final Config storage, final int i) throws Exception {
		final var control = S3TargetBucketControl.open("signing-retention-" + i, storage);
		final var driver = driverOf(control);
		assertNotNull(control.check("bucket", CREDENTIAL));
		assertFalse(driver.signingKeyCreateFunctionCache.get().isEmpty(), "the check was signed on this thread");
		control.close();
		return new WeakReference<>(driver);
	}

	private static WeakReference<Object> signAndClose(final Config storage, final int i) throws Exception {
		final var driver = new S3StorageDriver<Item, Operation<Item>>("signing-retention-" + i, null, storage, false, 1);
		sign(driver);
		driver.close();
		return new WeakReference<>(driver);
	}

	private static WeakReference<Object> checkHereAndCloseElsewhere(
					final Config storage, final int i, final List<ThreadLocal<Map<String, SigningKeyCreateFunction>>> caches)
					throws Exception {
		final var control = S3TargetBucketControl.open("signing-retention-" + i, storage);
		final var driver = driverOf(control);
		assertNotNull(control.check("bucket", CREDENTIAL));
		assertHoldsNoDriver(driver.signingKeyCreateFunctionCache.get().get(CREDENTIAL.getSecret()), driver);
		caches.add(driver.signingKeyCreateFunctionCache);
		final var closer = new Thread(control::close, "signing-retention-closer-" + i);
		closer.start();
		closer.join(TimeUnit.SECONDS.toMillis(COLLECT_SECONDS));
		assertFalse(closer.isAlive(), "the control driver closed");
		return new WeakReference<>(driver);
	}

	private static void sign(final S3StorageDriver<?, ?> driver) {
		final HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "127.0.0.1:9024");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		driver.applyAuthHeaders(headers, HttpMethod.HEAD, "/bucket", CREDENTIAL);
		assertNotNull(headers.get(HttpHeaderNames.AUTHORIZATION));
	}

	@SuppressWarnings("unchecked")
	private static Set<CompletableFuture<Void>> multipartCleanupTasks(final S3StorageDriver<?, ?> driver) throws Exception {
		final var field = S3StorageDriver.class.getDeclaredField("multipartCleanupTasks");
		field.setAccessible(true);
		return (Set<CompletableFuture<Void>>) field.get(driver);
	}

	private static S3StorageDriver<?, ?> driverOf(final S3TargetBucketControl control) throws Exception {
		final var field = S3TargetBucketControl.class.getDeclaredField("driver");
		field.setAccessible(true);
		return (S3StorageDriver<?, ?>) field.get(control);
	}

	/** The function is a static class, and none of its fields, its own or inherited, is the driver. */
	private static void assertHoldsNoDriver(final Object function, final Object driver) throws Exception {
		assertNotNull(function);
		final var type = function.getClass();
		assertTrue(type.getEnclosingClass() == null || Modifier.isStatic(type.getModifiers()),
						type.getName() + " is an inner class, so it holds its driver");
		for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
			for (final var field : c.getDeclaredFields()) {
				if (!Modifier.isStatic(field.getModifiers())) {
					field.setAccessible(true);
					assertNotSame(driver, field.get(function), "field " + field.getName() + " holds the driver");
				}
			}
		}
	}

	/** Waits, with a bounded number of collections, until every driver is gone. */
	private static void awaitCollected(final List<WeakReference<Object>> drivers) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(COLLECT_SECONDS);
		while (drivers.stream().anyMatch(ref -> ref.get() != null) && System.nanoTime() < deadline) {
			System.gc();
			Thread.sleep(50);
		}
		final var alive = drivers.stream().filter(ref -> ref.get() != null).count();
		assertEquals(0, alive, alive + " of " + drivers.size() + " closed drivers are still reachable");
	}
}
