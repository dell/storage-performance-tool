package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import io.netty.channel.epoll.Epoll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code storage.net.timeoutMilliSec} is an inactivity timeout on the default route: a request
 * whose bytes keep moving outlives it, a silent response fails about {@code t} after its last
 * request byte is handed to the socket, and a stalled upload fails within {@code 2t} of its last
 * progress, also when it stalls after several periods of progress. The cases run on NIO, as the
 * other driver tests do, and the steady and late-stall PUTs also on epoll, the shipped transport,
 * where it is available.
 */
@Timeout(120)
final class S3NetInactivityTimeoutTest {

	private static final int TIMEOUT_MS = 1_500;
	/** Allowance for a loaded test host on top of each bound. */
	private static final long MARGIN_MS = 1_500;
	private static final int MIB = 1 << 20;
	/** Each paced transfer takes about 4 s, beyond {@code 2t}. */
	private static final long PACED_BYTES = 6L * MIB;
	private static final long RATE = 3L * MIB / 2;
	/** The pooled test idles this long, then the stub answers after {@link #DELAY_MS}. */
	private static final long IDLE_MS = 1_200;
	private static final long DELAY_MS = 750;
	/**
	 * Allowance on the {@code 2t} stall bound, measured from when the stub stopped reading: the
	 * socket buffers take a few more bytes, and the timer and result hand-off take a few ms.
	 */
	private static final long STALL_MARGIN_MS = 300;

	@TempDir
	Path tmp;

	@Test
	void aPutThatKeepsMovingOutlivesTheTimeout() throws Exception {
		assertSteadyPut(null);
	}

	@Test
	void aTlsPutThatKeepsMovingOutlivesTheTimeout() throws Exception {
		assertSteadyPut(PacedS3Stub.selfSignedTls(tmp));
	}

	@Test
	void aGetThatKeepsMovingOutlivesTheTimeout() throws Exception {
		try (var stub = new PacedS3Stub(null, RATE, DELAY_MS, PACED_BYTES);
						var driver = driver(stub, false)) {
			final var result = driver.run(op(OpType.READ, "paced-get", PACED_BYTES));
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			assertTrue(result.elapsedMillis > 2L * TIMEOUT_MS, () -> "too fast to show the fix: " + result);
		}
	}

	@Test
	void aPooledConnectionGivesItsNextRequestAFullTimeout() throws Exception {
		try (var stub = new PacedS3Stub(null, RATE, DELAY_MS, 0);
						var driver = driver(stub, false)) {
			assertEquals(Operation.Status.SUCC, driver.run(op(OpType.CREATE, "quick-first", 1024)).status());
			// Idle for most of the timeout, then a response that takes the rest of it and more.
			Thread.sleep(IDLE_MS);
			final var result = driver.run(op(OpType.CREATE, "delay-second", 1024));
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			assertEquals(stub.connectionOf("quick-first"), stub.connectionOf("delay-second"),
							"the second request reused the pooled connection");
		}
	}

	@Test
	void aSilentResponseFailsAfterTheTimeout() throws Exception {
		try (var stub = new PacedS3Stub(null, RATE, DELAY_MS, 0);
						var driver = driver(stub, false)) {
			final var result = driver.run(op(OpType.CREATE, "silent-put", 1024));
			assertEquals(Operation.Status.FAIL_IO, result.status(), result::toString);
			assertTrue(result.elapsedMillis >= TIMEOUT_MS - 100, result::toString);
			assertTrue(result.elapsedMillis <= TIMEOUT_MS + MARGIN_MS, result::toString);
		}
	}

	@Test
	void aStalledUploadFailsWithinTwiceTheTimeout() throws Exception {
		assertStalledPut(null);
	}

	@Test
	void aStalledTlsUploadFailsWithinTwiceTheTimeout() throws Exception {
		assertStalledPut(PacedS3Stub.selfSignedTls(tmp));
	}

	@Test
	void anUploadThatStallsAfterProgressFailsWithinTwiceTheTimeout() throws Exception {
		assertLateStall(null, "nio");
	}

	@Test
	void aTlsUploadThatStallsAfterProgressFailsWithinTwiceTheTimeout() throws Exception {
		assertLateStall(PacedS3Stub.selfSignedTls(tmp), "nio");
	}

	@Test
	void onEpollAPutThatKeepsMovingOutlivesTheTimeout() throws Exception {
		assumeTrue(Epoll.isAvailable(), "epoll is not available here");
		try (var stub = new PacedS3Stub(null, RATE, DELAY_MS, 0);
						var driver = driver(stub, false, "epoll")) {
			final var result = driver.run(op(OpType.CREATE, "paced-put", PACED_BYTES));
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			assertTrue(result.elapsedMillis > 2L * TIMEOUT_MS, () -> "too fast to show the fix: " + result);
		}
	}

	@Test
	void onEpollAnUploadThatStallsAfterProgressFailsWithinTwiceTheTimeout() throws Exception {
		assumeTrue(Epoll.isAvailable(), "epoll is not available here");
		assertLateStall(null, "epoll");
	}

	/**
	 * The upload progresses across several timer periods (6 MiB at 1.5 MiB/s, {@code t} = 1.5 s),
	 * then the target stops reading: it fails within {@code 2t} of that stop.
	 */
	private void assertLateStall(final SSLContext tls, final String transport) throws Exception {
		try (var stub = new PacedS3Stub(tls, RATE, DELAY_MS, 0);
						var driver = driver(stub, tls != null, transport)) {
			final var result = driver.run(op(OpType.CREATE, "late-stall-put", 32L * MIB));
			assertEquals(Operation.Status.FAIL_IO, result.status(), result::toString);
			assertTrue(stub.stoppedReadingNanos() > 0, "the stub never stopped reading");
			final long afterStallMs = (result.endNanos - stub.stoppedReadingNanos()) / 1_000_000;
			System.out.println("late stall tls=" + (tls != null) + " transport=" + transport + ": failed "
							+ afterStallMs + " ms after the target stopped reading");
			assertTrue(afterStallMs <= 2L * TIMEOUT_MS + STALL_MARGIN_MS,
							() -> "failed " + afterStallMs + " ms after the stall, beyond 2t + margin: " + result);
		}
	}

	private void assertStalledPut(final SSLContext tls) throws Exception {
		try (var stub = new PacedS3Stub(tls, RATE, DELAY_MS, 0);
						var driver = driver(stub, tls != null)) {
			final var result = driver.run(op(OpType.CREATE, "stall-put", 32L * MIB));
			assertEquals(Operation.Status.FAIL_IO, result.status(), result::toString);
			assertTrue(result.elapsedMillis >= TIMEOUT_MS - 100, result::toString);
			assertTrue(result.elapsedMillis <= 2L * TIMEOUT_MS + MARGIN_MS, result::toString);
		}
	}

	@Test
	void anIdlePooledConnectionStaysOpenAndUncounted() throws Exception {
		try (var stub = new PacedS3Stub(null, RATE, DELAY_MS, 0);
						var driver = driver(stub, false)) {
			assertEquals(Operation.Status.SUCC, driver.run(op(OpType.CREATE, "quick-first", 1024)).status());
			// Several idle periods pass with the connection in the pool and no operation.
			Thread.sleep(5L * TIMEOUT_MS / 2);
			assertNull(driver.results.poll(0), "an idle connection published no result");
			assertEquals(Operation.Status.SUCC, driver.run(op(OpType.CREATE, "quick-second", 1024)).status());
			assertEquals(stub.connectionOf("quick-first"), stub.connectionOf("quick-second"),
							"the idle connection stayed open and was reused");
		}
	}

	private void assertSteadyPut(final SSLContext tls) throws Exception {
		try (var stub = new PacedS3Stub(tls, RATE, DELAY_MS, 0);
						var driver = driver(stub, tls != null)) {
			final var result = driver.run(op(OpType.CREATE, "paced-put", PACED_BYTES));
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			assertTrue(result.elapsedMillis > 2L * TIMEOUT_MS, () -> "too fast to show the fix: " + result);
		}
	}

	private static DataOperation<DataItem> op(final OpType type, final String name, final long size) {
		return new DataOperationImpl<>(0, type, new DataItemImpl(name, 0, size), null, "/bucket", CREDENTIAL, null, 0);
	}

	private record Outcome(Operation.Status status, long elapsedMillis, long endNanos, String op) {
		@Override
		public String toString() {
			return status + " after " + elapsedMillis + " ms: " + op;
		}
	}

	private static final class Driver implements AutoCloseable {
		final S3StorageDriver<DataItem, Operation<DataItem>> driver;
		final EndpointSelectionTestSupport.Results results = new EndpointSelectionTestSupport.Results();

		Driver(final Config storage) throws Exception {
			driver = new S3StorageDriverExtension<DataItem, Operation<DataItem>, S3StorageDriver<DataItem, Operation<DataItem>>>()
							.create("net-inactivity", new SeedDataInput(1, MIB, 1, true), storage, false, 16);
			driver.operationResultOutput(results);
			driver.start();
		}

		Outcome run(final DataOperation<DataItem> op) throws Exception {
			final long start = System.nanoTime();
			assertTrue(driver.put(op));
			final var result = results.poll(30_000);
			assertNotNull(result, "no result for " + op);
			final long end = System.nanoTime();
			return new Outcome(result.status(), (end - start) / 1_000_000, end, result.toString());
		}

		@Override
		public void close() throws Exception {
			driver.close();
		}
	}

	private static Driver driver(final PacedS3Stub stub, final boolean tls) throws Exception {
		return driver(stub, tls, "nio");
	}

	private static Driver driver(final PacedS3Stub stub, final boolean tls, final String transport)
					throws Exception {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		root.val("storage-net-transport", transport);
		root.val("storage-net-interfaces", List.of());
		root.val("storage-net-node-addrs", List.of("127.0.0.1:" + stub.port()));
		root.val("storage-net-timeoutMilliSec", TIMEOUT_MS);
		root.val("storage-net-http-headers", Map.of());
		// Small socket buffers keep the client's writes in step with the stub's reads. With the
		// shipped auto-sized buffers, several MiB can sit in the kernel when the last body byte
		// leaves the outbound buffer; from then on the timer counts a silent response, so a slow
		// tail could time out even though the target is still reading.
		root.val("storage-net-sndBuf", 64 * 1024);
		root.val("storage-net-rcvBuf", 64 * 1024);
		root.val("storage-driver-limit-concurrency", 1);
		root.val("storage-driver-threads", 1);
		if (tls) {
			root.val("storage-net-ssl-enabled", true);
			root.val("storage-net-ssl-provider", "JDK");
			root.val("storage-net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
			root.val("storage-net-ssl-pqcMode", "off");
		}
		return new Driver(root.configVal("storage"));
	}
}
