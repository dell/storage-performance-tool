package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.storage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A retried part starts from no bytes, no data-response start and the first range of its cursor. A
 * range of a split read whose attempt received bytes and failed (an error body, or a body cut short)
 * is retried by the driver's part retry: the retry's success counts only its own bytes, verifies
 * against the item's data from its start, and records its own time to first byte. A retried upload part counts its bytes once, as before.
 */
@Timeout(120)
final class S3PartRetryByteCountTest {

	private static final String ERROR_BODY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SlowDown</Code>"
					+ "<Message>Please reduce your request rate.</Message><RequestId>r-1</RequestId></Error>";
	private static final int RANGES = 3;
	private static final int UPLOAD_PARTS = 3;
	private static final long QUIET_NANOS = 1_000_000_000L;
	private static final long DEADLINE_NANOS = 20_000_000_000L;

	/** How the first range's failing attempts are answered. */
	private enum Fault {
		/** 503 with an {@code <Error>} body. */
		REFUSE,
		/**
		 * 206 with half the range's body the first time, three quarters of it after that, then the
		 * connection is closed. A later cut gets past where the first one stopped.
		 */
		CUT
	}

	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final AtomicInteger faultsLeft = new AtomicInteger();
	private volatile byte[] content;
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final NicSetLoopback stub = new NicSetLoopback();
	private volatile Fault fault;
	private HttpServer server;

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
		stub.close();
		awaitNoNicIoThreads();
	}

	@Test
	void anUnverifiedRangeRefusedOnceWithAnErrorBodyCountsOnlyItsRetrysBytes() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.REFUSE, 1, false);
	}

	@Test
	void anUnverifiedRangeCutMidBodyOnceCountsOnlyItsRetrysBytes() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 1, false);
	}

	@Test
	void aVerifiedRangeCutMidBodyOnceVerifiesAndSucceeds() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 1, true);
	}

	@Test
	void anUnverifiedRangeCutMidBodyTwiceCountsOnlyItsLastRetrysBytes() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 2, false);
	}

	@Test
	void aVerifiedRangeCutMidBodyTwiceVerifiesAndSucceeds() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 2, true);
	}

	/**
	 * An item above layer 0 is verified range by range, through the operation's range cursor: the
	 * retry verifies its range from the start, not from where the failed attempt stopped.
	 */
	@Test
	void aVerifiedRangeOfALayer1ItemWithNoFaultSucceeds() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 0, true, 1);
	}

	@Test
	void aVerifiedRangeOfALayer1ItemCutMidBodyOnceVerifiesAndSucceeds() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 1, true, 1);
	}

	@Test
	void aVerifiedRangeOfALayer1ItemCutMidBodyTwiceVerifiesAndSucceeds() throws Exception {
		assertRetriedRangeCountsItsOwnBytes(Fault.CUT, 2, true, 1);
	}

	@Test
	void aRangeRefusedOnceWithAnErrorBodyRecordsItsRetrysTimeToFirstByte() throws Exception {
		final var retried = assertRetriedRangeCountsItsOwnBytes(Fault.REFUSE, 1, false);
		assertTrue(retried.dataLatency() > 0, () -> "the retry's time to first byte: " + retried.dataLatency() + " µs, "
						+ "data start " + retried.respDataTimeStart() + ", request done " + retried.reqTimeDone());
	}

	/**
	 * An upload part refused once and then accepted counts its part's bytes once: the upload's
	 * successful parts count what an upload with no refusal counts.
	 */
	@Test
	void anUploadWhosePartIsRefusedOnceCountsTheSameBytesAsOneWithNoRefusal() throws Exception {
		final var driver = new S3StorageDriver<DataItem, Operation<DataItem>>("part-retry-bytes-upload",
						new SeedDataInput(1, ITEM_SIZE, 1, true), storage(stub.listeners(1), List.of(), 1), false, 16);
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();

		final var clean = parts(upload(driver, results, "clean"));
		// The second upload's first part to arrive is refused once, with the stub's error body.
		final var refusalsLeft = new AtomicInteger(1);
		stub.failRequest = request -> request.isPart() && request.path().endsWith("/refused")
						&& refusalsLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
		final var refused = parts(upload(driver, results, "refused"));

		assertEquals(UPLOAD_PARTS, clean.size(), clean::toString);
		assertEquals(UPLOAD_PARTS + 1, refused.size(), refused::toString);
		final var failed = refused.stream().filter(part -> part.status() != Operation.Status.SUCC).toList();
		assertEquals(1, failed.size(), refused::toString);
		assertEquals(successBytes(clean), successBytes(refused), () -> clean + " against " + refused);
		assertEquals((long) UPLOAD_PARTS * ITEM_SIZE, successBytes(refused), refused::toString);
		for (final var part : refused) {
			if (part.status() == Operation.Status.SUCC) {
				assertEquals(ITEM_SIZE, part.countBytesDone(), part::toString);
			}
		}
	}

	private PartialDataOperation<?> assertRetriedRangeCountsItsOwnBytes(final Fault how, final int faults, final boolean verify)
					throws Exception {
		return assertRetriedRangeCountsItsOwnBytes(how, faults, verify, 0);
	}

	/**
	 * Reads an item at {@code layer} in {@link #RANGES} ranges, its first range failing {@code faults}
	 * times with {@code how}, and checks that the read succeeds and that each successful range counts
	 * exactly its own bytes. Returns the first range's successful retry, or its only attempt when
	 * {@code faults} is 0.
	 */
	private PartialDataOperation<?> assertRetriedRangeCountsItsOwnBytes(final Fault how, final int faults, final boolean verify,
					final int layer) throws Exception {
		fault = how;
		faultsLeft.set(faults);

		final List<Operation<?>> results = read(verify, layer);

		final var composite = results.get(results.size() - 1);
		assertTrue(composite instanceof CompositeDataOperation<?>, results::toString);
		assertEquals(Operation.Status.SUCC, composite.status(), () -> requests + " " + results);
		final var faulted = requests.stream().filter(request -> request.endsWith("-> " + how)).count();
		assertEquals(faults, faulted, requests::toString);

		final var ranges = parts(results);
		assertEquals(RANGES + faults, ranges.size(), ranges::toString);
		PartialDataOperation<?> retried = null;
		for (final var range : ranges) {
			if (range.status() == Operation.Status.SUCC) {
				assertEquals(ITEM_SIZE, range.countBytesDone(), () -> "range #" + range.partNumber() + " retries "
								+ range.retryCount() + ": " + ranges);
				if (range.partNumber() == 0) {
					retried = range;
				}
			} else {
				// Each failed attempt fails by the fault itself: a retry verified from the failed
				// attempt's place would be reported corrupt, even if a later retry then succeeds.
				assertEquals(how == Fault.REFUSE ? Operation.Status.RESP_FAIL_SVC : Operation.Status.FAIL_IO, range.status(),
								() -> "range #" + range.partNumber() + " retries " + range.retryCount() + ": " + ranges);
			}
		}
		assertNotNull(retried, ranges::toString);
		assertEquals(faults, retried.retryCount(), ranges::toString);
		return retried;
	}

	/** Every result of a split read of an item at {@code layer}, in order, ending with the composite's last result. */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private List<Operation<?>> read(final boolean verify, final int layer) throws Exception {
		content = itemContent(layer);
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();

		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", 5_000);
		config.val("storage-net-node-addrs", List.of("127.0.0.1:" + server.getAddress().getPort()));
		config.val("storage-net-endpoint-selection", "round-robin");
		config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		final S3StorageDriver<DataItem, Operation<DataItem>> driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"part-retry-bytes-read", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), verify, 16);
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();

		final var item = new DataItemImpl("ranged", 9, (long) RANGES * ITEM_SIZE, layer);
		assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE)));

		// The composite's result comes last: wait for it, then for the driver to be idle and quiet.
		final List<Operation<?>> collected = new ArrayList<>();
		CompositeDataOperationImpl<?> composite = null;
		long quietSince = System.nanoTime();
		final long deadline = System.nanoTime() + DEADLINE_NANOS;
		while (System.nanoTime() < deadline) {
			final var result = results.poll(50);
			if (result != null) {
				quietSince = System.nanoTime();
				collected.add(result);
				if (result instanceof CompositeDataOperationImpl<?> parent) {
					composite = parent;
				}
			} else if (composite != null && composite.allSubOperationsDone() && driver.activeOpCount() == 0
							&& System.nanoTime() - quietSince > QUIET_NANOS) {
				return collected;
			}
		}
		throw new AssertionError("the read did not finish: " + requests + " " + collected);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var range = exchange.getRequestHeaders().getFirst("Range");
			if (!"GET".equals(exchange.getRequestMethod()) || range == null) {
				requests.add(exchange.getRequestMethod() + " -> 200");
				exchange.sendResponseHeaders(200, -1);
				return;
			}
			final var bounds = range.substring("bytes=".length()).split("-", -1);
			final int first = Integer.parseInt(bounds[0]);
			final int last = Integer.parseInt(bounds[1]);
			final int length = last - first + 1;
			if (first == 0 && faultsLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
				requests.add("GET " + range + " -> " + fault);
				if (fault == Fault.REFUSE) {
					final byte[] error = ERROR_BODY.getBytes(StandardCharsets.UTF_8);
					exchange.getResponseHeaders().set("Content-Type", "application/xml");
					exchange.sendResponseHeaders(503, error.length);
					exchange.getResponseBody().write(error);
				} else {
					exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
					exchange.sendResponseHeaders(206, length);
					final boolean firstCut = requests.stream().filter(request -> request.endsWith("-> " + Fault.CUT)).count() == 1;
					exchange.getResponseBody().write(content, first, firstCut ? length / 2 : length * 3 / 4);
					exchange.getResponseBody().flush();
					// Closing the exchange with its body short closes the connection.
				}
				return;
			}
			requests.add("GET " + range + " -> 206");
			exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
			exchange.sendResponseHeaders(206, length);
			exchange.getResponseBody().write(content, first, length);
		}
	}

	/** Every result of an upload of {@link #UPLOAD_PARTS} parts named {@code name}, up to its final result. */
	private static List<Operation<?>> upload(final S3StorageDriver<DataItem, Operation<DataItem>> driver,
					final EndpointSelectionTestSupport.Results results, final String name) throws InterruptedException {
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE,
						new DataItemImpl(name, 7, (long) UPLOAD_PARTS * ITEM_SIZE), null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		assertTrue(driver.put(upload));
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		final List<Operation<?>> collected = new ArrayList<>();
		while (System.nanoTime() < deadline) {
			final var result = results.poll();
			if (result != null) {
				assertTrue(result.item().name().endsWith("/" + name), result::toString);
				collected.add(result);
			} else if (!collected.isEmpty() && collected.get(collected.size() - 1) instanceof CompositeDataOperation<?> last
							&& last.allSubOperationsDone() && driver.activeOpCount() == 0) {
				assertEquals(Operation.Status.SUCC, last.status(), collected::toString);
				return collected;
			}
		}
		throw new AssertionError("the upload did not finish: " + collected);
	}

	private static List<PartialDataOperation<?>> parts(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
	}

	private static long successBytes(final List<PartialDataOperation<?>> parts) {
		return parts.stream().filter(part -> part.status() == Operation.Status.SUCC)
						.mapToLong(PartialDataOperation::countBytesDone).sum();
	}

	/** The bytes a verified read of the item at {@code layer} expects. */
	private static byte[] itemContent(final int layer) {
		final var item = new DataItemImpl("ranged", 9, (long) RANGES * ITEM_SIZE, layer);
		item.dataInput(new SeedDataInput(1, ITEM_SIZE, 1, true));
		final ByteBuffer bytes = ByteBuffer.allocate(RANGES * ITEM_SIZE);
		while (bytes.hasRemaining()) {
			item.read(bytes);
		}
		return bytes.array();
	}
}
