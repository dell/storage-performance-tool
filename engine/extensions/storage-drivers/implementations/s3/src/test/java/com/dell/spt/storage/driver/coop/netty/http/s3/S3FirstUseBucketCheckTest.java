package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Characterizes the Netty driver's own first-use bucket initialization, which the controller's
 * provisioning does not replace and the multi-bucket feature keeps unchanged
 * ({@code S3StorageDriver.requestNewPath} through {@code StorageDriverBase.prepare}):
 *
 * <ul>
 *   <li>a successful check, an HTTP error status and a response timeout return the path, which is
 *       cached, so that driver instance never checks the bucket again;
 *   <li>only a connection failure of the initial HEAD returns null and caches nothing; the object
 *       operation is still submitted and its own request decides its outcome;
 *   <li>with versioning, initialization adds GET ?versioning and, to reconcile, PUT ?versioning;
 *   <li>an interrupt ends a pending helper request.
 * </ul>
 */
@Timeout(60)
final class S3FirstUseBucketCheckTest {

	private record Captured(String method, String path, String query) {}

	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<HttpServer> servers = new ArrayList<>();
	private volatile int headStatus = 200;
	private volatile long headDelayMillis;
	private volatile CountDownLatch headArrived = new CountDownLatch(1);

	@AfterEach
	void tearDown() throws Exception {
		for (final var resource : resources) {
			resource.close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	@Test
	void aSuccessfulCheckReturnsThePathAndIsCachedForLaterOperations() throws Exception {
		final var driver = driver(List.of(listener()), "default", 5_000, false);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));
		requests.clear();
		putTwice(driver, "/bench-0/p");

		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
	}

	@Test
	void anHttpErrorStatusIsCachedAndNotRetried() throws Exception {
		headStatus = 403;
		final var driver = driver(List.of(listener()), "default", 5_000, false);

		assertEquals("/bench-1/p", driver.requestNewPath("/bench-1/p"));
		requests.clear();
		final var outcomes = putTwice(driver, "/bench-1/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes);
		assertEquals(1, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
	}

	@Test
	void aResponseTimeoutIsCachedAndNotRetried() throws Exception {
		headDelayMillis = 3_000;
		final var driver = driver(List.of(listener()), "default", 300, false);

		final long start = System.nanoTime();
		assertEquals("/bench-2/p", driver.requestNewPath("/bench-2/p"));
		final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		assertTrue(millis < 2_500, "the wait ends at the helper timeout, not the response: " + millis + " ms");
	}

	@Test
	void aConnectionFailureOfTheHeadIsRetriedButDoesNotFailTheOperation() throws Exception {
		// Round robin alternates between a closed port and a listener: every bucket check lands on the
		// closed port, every object request on the listener.
		final var driver = driver(List.of("127.0.0.1:" + closedPort(), listener()), "round-robin", 5_000, false);

		final var outcomes = putTwice(driver, "/bench-3/p");

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.SUCC), outcomes,
						"each operation's own request decides its outcome");
		assertEquals(0, requests.stream().filter(r -> "HEAD".equals(r.method())).count(), requests::toString);
		assertEquals(2, requests.stream().filter(r -> "PUT".equals(r.method())).count(), requests::toString);
		final var counters = ((S3EndpointSelectionDriver<?, ?>) driver).counters().snapshot();
		assertEquals(2, counters.connectsFailed(), "the uncached check was attempted again: " + counters);
	}

	@Test
	void versioningAddsTheVersioningCheckAndReconciliation() throws Exception {
		final var driver = driver(List.of(listener()), "default", 5_000, true);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));

		assertEquals(List.of(
						new Captured("HEAD", "/bench-0", null),
						new Captured("GET", "/bench-0", "versioning"),
						new Captured("PUT", "/bench-0", "versioning")), requests);
	}

	@Test
	void anInterruptEndsAPendingCheck() throws Exception {
		headDelayMillis = 30_000;
		final var driver = driver(List.of(listener()), "default", 60_000, false);
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final var thread = Thread.ofPlatform().start(() -> {
			try {
				driver.requestNewPath("/bench-0/p");
			} catch (final Throwable e) {
				failure.set(e);
			}
		});
		assertTrue(headArrived.await(10, TimeUnit.SECONDS), "the check reached the listener");

		thread.interrupt();
		thread.join(5_000);

		assertFalse(thread.isAlive(), "the interrupt ended the wait");
		assertInstanceOf(InterruptedException.class, failure.get());
		driver.close();
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private List<Operation.Status> putTwice(final S3StorageDriver driver, final String path) throws Exception {
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
		final var outcomes = new ArrayList<Operation.Status>();
		for (int i = 0; i < 2; i++) {
			final Operation<DataItem> op = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("k" + i, i, ITEM_SIZE), null, path, CREDENTIAL, null, 0);
			assertTrue(driver.put(op));
			outcomes.add(results.await().status());
		}
		return outcomes;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private S3StorageDriver driver(final List<String> endpoints, final String mode, final int timeoutMillis,
					final boolean versioning) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(versioning, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", timeoutMillis);
		if ("round-robin".equals(mode)) {
			config.val("storage-net-node-addrs", endpoints);
			config.val("storage-net-endpoint-selection", "round-robin");
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		} else {
			final var endpoint = endpoints.get(0);
			config.val("storage-net-node-port", Integer.parseInt(endpoint.substring(endpoint.indexOf(':') + 1)));
		}
		final var driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"first-use", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), false, 16);
		resources.add(driver);
		driver.start();
		return driver;
	}

	private String listener() throws IOException {
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
		servers.add(server);
		return "127.0.0.1:" + server.getAddress().getPort();
	}

	private static int closedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var method = exchange.getRequestMethod();
			requests.add(new Captured(method, uri.getRawPath(), uri.getRawQuery()));
			if ("HEAD".equals(method)) {
				headArrived.countDown();
				if (headDelayMillis > 0) {
					try {
						Thread.sleep(headDelayMillis);
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}
				}
				exchange.sendResponseHeaders(headStatus, -1);
				return;
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag\"");
			}
			final byte[] body = "GET".equals(method) ? "<VersioningConfiguration/>".getBytes(StandardCharsets.UTF_8) : new byte[0];
			exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
			if (body.length > 0) {
				exchange.getResponseBody().write(body);
			}
		}
	}
}
