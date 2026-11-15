package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A read split into ranges whose first range is refused once with an error body, then answered
 * correctly, succeeds through the real driver, with verification on and off. The refused body is
 * not item data: verifying it, or counting it, must not make the range's retry fail verification.
 */
@Timeout(60)
final class S3VerifiedRangeRetryTest {

	private static final String ERROR_BODY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SlowDown</Code>"
					+ "<Message>Please reduce your request rate.</Message><RequestId>r-1</RequestId></Error>";
	private static final int RANGES = 3;
	private static final long QUIET_NANOS = 1_000_000_000L;
	private static final long DEADLINE_NANOS = 20_000_000_000L;

	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final AtomicInteger refusalsLeft = new AtomicInteger();
	private final byte[] content = itemContent();
	private volatile int refusalStatus;
	private HttpServer server;
	private S3StorageDriver<DataItem, Operation<DataItem>> driver;

	@AfterEach
	void stop() throws Exception {
		if (driver != null) {
			driver.close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
	}

	@Test
	void aVerifiedRangeRefusedOnceWith503ThenAnsweredSucceeds() throws Exception {
		assertRangeRefusedThenAnsweredSucceeds(true, 503, 1);
	}

	@Test
	void aVerifiedRangeRefusedOnceWith404ThenAnsweredSucceeds() throws Exception {
		assertRangeRefusedThenAnsweredSucceeds(true, 404, 1);
	}

	@Test
	void aVerifiedRangeReadWithNoRefusalSucceeds() throws Exception {
		assertRangeRefusedThenAnsweredSucceeds(true, 503, 0);
	}

	@Test
	void anUnverifiedRangeRefusedOnceWith503ThenAnsweredSucceeds() throws Exception {
		assertRangeRefusedThenAnsweredSucceeds(false, 503, 1);
	}

	private void assertRangeRefusedThenAnsweredSucceeds(final boolean verify, final int status, final int refusals)
					throws Exception {
		refusalStatus = status;
		refusalsLeft.set(refusals);

		assertEquals(Operation.Status.SUCC, read(verify), requests::toString);

		final var refused = requests.stream().filter(request -> request.endsWith("-> " + status)).count();
		assertEquals(refusals, refused, requests::toString);
		assertEquals(RANGES, requests.stream().filter(request -> request.endsWith("-> 206")).count(), requests::toString);
	}

	/** Reads the item in {@link #RANGES} ranges and returns its composite's final status. */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private Operation.Status read(final boolean verify) throws Exception {
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
		driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"verified-range-retry", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), verify, 16);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();

		final var item = new DataItemImpl("ranged", 9, (long) RANGES * ITEM_SIZE);
		assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE)));

		// The composite's result comes last: wait for it, then for the driver to be idle and quiet.
		CompositeDataOperationImpl<?> composite = null;
		long quietSince = System.nanoTime();
		final long deadline = System.nanoTime() + DEADLINE_NANOS;
		while (System.nanoTime() < deadline) {
			final var result = results.poll(50);
			if (result != null) {
				quietSince = System.nanoTime();
				if (result instanceof CompositeDataOperationImpl<?> parent) {
					composite = parent;
				}
			} else if (composite != null && composite.allSubOperationsDone() && driver.activeOpCount() == 0
							&& System.nanoTime() - quietSince > QUIET_NANOS) {
				break;
			}
		}
		assertNotNull(composite, "the composite's result: " + requests);
		return composite.status();
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
			if (first == 0 && refusalsLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
				requests.add("GET " + range + " -> " + refusalStatus);
				final byte[] error = ERROR_BODY.getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/xml");
				exchange.sendResponseHeaders(refusalStatus, error.length);
				exchange.getResponseBody().write(error);
				return;
			}
			requests.add("GET " + range + " -> 206");
			exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
			exchange.sendResponseHeaders(206, last - first + 1);
			exchange.getResponseBody().write(content, first, last - first + 1);
		}
	}

	/** The bytes a verified read of the item expects. */
	private static byte[] itemContent() {
		final var item = new DataItemImpl("ranged", 9, (long) RANGES * ITEM_SIZE);
		item.dataInput(new SeedDataInput(1, ITEM_SIZE, 1, true));
		final ByteBuffer bytes = ByteBuffer.allocate(RANGES * ITEM_SIZE);
		while (bytes.hasRemaining()) {
			item.read(bytes);
		}
		return bytes.array();
	}
}
