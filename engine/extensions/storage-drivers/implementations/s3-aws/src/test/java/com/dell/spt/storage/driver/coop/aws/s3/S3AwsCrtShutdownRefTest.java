package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * A driver the factory builds takes one CRT shutdown reference for its clients and releases it
 * exactly once, after they are closed, on every close path and on a failed build. An acquire the
 * CRT refuses, because its shutdown has begun, leaves the driver built without a reference.
 */
@Timeout(60)
final class S3AwsCrtShutdownRefTest {

	/** No request is made: the clients are built and closed only. */
	private static final int UNUSED_PORT = 1;

	@Test
	void aBuiltDriverHoldsOneReferenceUntilItClosesAndReleasesItOnce(@TempDir final Path home) throws Exception {
		final var refs = new CountingRefs();
		final S3AwsStorageDriver<Item, Operation<Item>> driver = S3AwsDriverHarness.driver(
						home, UNUSED_PORT, Map.of(), refs, false);
		try {
			assertEquals(1, refs.acquired.get());
			assertEquals(0, refs.released.get(), "the reference must be held while the driver is open");
		} finally {
			driver.close();
		}
		assertEquals(1, refs.released.get());

		driver.close();
		assertEquals(1, refs.acquired.get());
		assertEquals(1, refs.released.get(), "a repeated close must not release again");
	}

	@Test
	void aCloseOnAnInterruptedThreadReleasesTheReference(@TempDir final Path home) throws Exception {
		final var refs = new CountingRefs();
		final S3AwsStorageDriver<Item, Operation<Item>> driver = S3AwsDriverHarness.driver(
						home, UNUSED_PORT, Map.of(), refs, false);
		driver.start();
		Thread.currentThread().interrupt();
		try {
			driver.close();
		} finally {
			// Never leave the interrupt to later tests on this thread.
			Thread.interrupted();
		}
		assertEquals(1, refs.acquired.get());
		assertEquals(1, refs.released.get());
	}

	@Test
	void aDriverThatFailsToBuildAfterItsClientsReleasesTheReference(@TempDir final Path home) {
		final var refs = new CountingRefs();
		// The legacy verify flag is refused once the clients are built.
		assertThrows(IllegalConfigurationException.class,
						() -> S3AwsDriverHarness.driver(home, UNUSED_PORT, Map.of(), refs, true));
		assertEquals(1, refs.acquired.get());
		assertEquals(1, refs.released.get());
	}

	@Test
	void anAcquireRefusedAtShutdownStillBuildsTheDriverAndReleasesNothing(@TempDir final Path home) throws Exception {
		final var refs = new CountingRefs();
		refs.refuse = true;
		final S3AwsStorageDriver<Item, Operation<Item>> driver = S3AwsDriverHarness.driver(
						home, UNUSED_PORT, Map.of(), refs, false);
		driver.close();
		driver.close();
		assertEquals(1, refs.acquired.get(), "the acquire was attempted");
		assertEquals(0, refs.released.get(), "a reference never taken must not be released");
	}

	@Test
	void aClientCloseThatThrowsStillReleasesTheReference(@TempDir final Path home) throws Exception {
		final var refs = new CountingRefs();
		final Config storage = S3AwsMergedConfig.create(home, UNUSED_PORT, Map.of()).configVal("storage");
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final var closeFailure = new LinkageError("injected client close failure");
		final var releasedWhenClientClosed = new AtomicInteger(-1);
		doAnswer(invocation -> {
			releasedWhenClientClosed.set(refs.released.get());
			throw closeFailure;
		}).when(client).close();
		final S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
						"s3-aws-ref",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage,
						false,
						16,
						client,
						() -> client,
						() -> {
							throw new AssertionError("no standalone DELETE is made");
						},
						100 * 1024L,
						8 * 1024 * 1024L,
						CrtShutdownRef.acquire(refs));

		assertSame(closeFailure, assertThrows(LinkageError.class, driver::close));
		assertEquals(1, refs.acquired.get());
		assertEquals(0, releasedWhenClientClosed.get(), "the reference must be held until the client is closed");
		assertEquals(1, refs.released.get());
	}

	@Test
	void aReleaseIsMadeAtMostOnceAndNeverThrows() {
		final var refs = new CountingRefs();
		refs.failRelease = true;
		final CrtShutdownRef ref = CrtShutdownRef.acquire(refs);
		assertTrue(ref.held());

		ref.release();
		ref.release();

		assertFalse(ref.held());
		assertEquals(1, refs.released.get());
	}

	@Test
	void aRefusedAcquireHoldsNothing() {
		final var refs = new CountingRefs();
		refs.refuse = true;
		final CrtShutdownRef ref = CrtShutdownRef.acquire(refs);

		assertFalse(ref.held());
		ref.release();
		assertEquals(0, refs.released.get());
	}

	@Test
	void theCrtCountTakesAndReleasesAReference() {
		final CrtShutdownRef ref = CrtShutdownRef.acquire(CrtShutdownRef.CRT_COUNTER);
		assertTrue(ref.held(), "the CRT refused a reference before JVM shutdown");
		ref.release();
		assertFalse(ref.held());
	}

	/** A stand-in for the CRT's count that records its calls. */
	private static final class CountingRefs implements CrtShutdownRef.Counter {
		final AtomicInteger acquired = new AtomicInteger();
		final AtomicInteger released = new AtomicInteger();
		volatile boolean refuse;
		volatile boolean failRelease;

		@Override
		public void acquire() {
			acquired.incrementAndGet();
			if (refuse) {
				// What the CRT throws once its own shutdown hook has run.
				throw new CrtRuntimeException("Cannot acquire CRT shutdown when ref count is non-positive");
			}
		}

		@Override
		public void release() {
			released.incrementAndGet();
			if (failRelease) {
				throw new CrtRuntimeException("Cannot release CRT shutdown when ref count is non-positive");
			}
		}
	}
}
