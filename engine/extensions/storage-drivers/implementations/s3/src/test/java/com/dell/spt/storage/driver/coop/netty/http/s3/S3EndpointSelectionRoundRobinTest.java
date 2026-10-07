package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real-driver round-robin selection against loopback listeners that record every request. */
@Timeout(60)
final class S3EndpointSelectionRoundRobinTest {

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final int ITEM_SIZE = 1024;
	private static final long RESULT_TIMEOUT_SECONDS = 10;
	private static final String HOSTNAME = "s3.example.test";

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private volatile boolean failFirstPart;
	private volatile int completeStatus = 200;

	private record Captured(int listener, String method, String path, String query, String host, int clientPort) {}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	@Test
	void operationsRotateFromTheFirstEndpointAndRecordTheSelectedAddress() throws Exception {
		final var endpoints = listeners(3);
		final var run = driver(endpoints, null, 1);
		// The first operation's bucket check is a helper request; it takes the first turn.

		final List<String> selected = new ArrayList<>();
		for (var i = 0; i < 7; i++) {
			final var result = run.execute(dataOp(OpType.CREATE, "object-" + i));
			assertEquals(Operation.Status.SUCC, result.status());
			selected.add(result.nodeAddr());
		}

		final List<String> expected = new ArrayList<>();
		for (var turn = 1; turn <= 7; turn++) {
			expected.add(endpoints.get(turn % 3));
		}
		assertEquals(expected, selected);
		assertEquals(List.of(0, 1, 2, 0, 1, 2, 0, 1), listenerSequence());
		// Without a logical hostname, Host is the selected address as before.
		for (final var request : requests.subList(1, requests.size())) {
			assertEquals(endpoints.get(request.listener()), request.host());
		}
	}

	@Test
	void requestKindsAndHelpersShareOneRotationWithTheLogicalHostname() throws Exception {
		final var endpoints = listeners(3);
		final var run = driver(endpoints, HOSTNAME, 1);

		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "kinds")).status());
		run.driver().requestNewPath("/other");
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.READ, "kinds")).status());
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.DELETE, "kinds")).status());

		assertEquals(List.of("HEAD", "PUT", "HEAD", "GET", "DELETE"), requests.stream().map(Captured::method).toList());
		assertEquals(List.of(0, 1, 2, 0, 1), listenerSequence());
		for (final var request : requests) {
			final var port = endpoints.get(request.listener()).split(":")[1];
			if ("HEAD".equals(request.method())) {
				// Helpers sign before a connection exists, so their Host carries the first endpoint's port.
				assertEquals(HOSTNAME + ":" + endpoints.get(0).split(":")[1], request.host());
			} else {
				assertEquals(HOSTNAME + ":" + port, request.host());
			}
		}
	}

	@Test
	void multipartUploadPhasesTakeSuccessiveTurns() throws Exception {
		final var endpoints = listeners(3);
		final var run = driver(endpoints, HOSTNAME, 1);
		final var item = new DataItemImpl("multipart", 7, 3L * ITEM_SIZE);
		final var upload = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(run.driver().put(upload));
		final var result = awaitMultipart(run);

		assertEquals(Operation.Status.SUCC, result.status());
		final var phases = requests.stream()
						.map(r -> r.method() + (r.query() == null ? ""
										: r.query().contains("uploads") ? " init"
														: r.query().contains("partNumber") ? " part" : " complete"))
						.toList();
		assertEquals(List.of("HEAD", "POST init", "PUT part", "PUT part", "PUT part", "POST complete"), phases);
		assertEquals(List.of(0, 1, 2, 0, 1, 2), listenerSequence());
	}

	@Test
	void idleConnectionsAreReusedPerDestination() throws Exception {
		final var endpoints = listeners(3);
		final var run = driver(endpoints, null, 1);

		for (var i = 0; i < 9; i++) {
			assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "reuse-" + i)).status());
		}

		for (var listener = 0; listener < 3; listener++) {
			final var index = listener;
			final var clientPorts = requests.stream().filter(r -> "PUT".equals(r.method()) && r.listener() == index)
							.map(Captured::clientPort).distinct().count();
			assertEquals(1, clientPorts, "listener " + listener + " saw more than one connection");
		}
		assertEquals(3, run.driver().connectionPool().openChannelCount());
	}

	@Test
	void refusedEndpointFailsItsTurnsWithoutSkipping() throws Exception {
		final var live = listeners(2);
		final var refused = "127.0.0.1:" + unusedPort();
		final var run = driver(List.of(live.get(0), refused, live.get(1)), null, 1);

		final List<Operation.Status> statuses = new ArrayList<>();
		for (var i = 0; i < 6; i++) {
			statuses.add(run.execute(dataOp(OpType.CREATE, "refused-" + i)).status());
		}

		final var ok = Operation.Status.SUCC;
		final var failed = Operation.Status.FAIL_IO;
		// Turns 1..6 select refused, live 1, live 0, refused, live 1, live 0.
		assertEquals(List.of(failed, ok, ok, failed, ok, ok), statuses);
		assertEquals(List.of(0, 1, 0, 1, 0), listenerSequence());
		assertEquals(0, run.driver().activeOpCount());
	}

	@Test
	void slowConnectDoesNotBlockRequestsToOtherDestinations() throws Exception {
		final var live = listeners(1);
		// TEST-NET-1 address: connects stall until the timeout, or fail at once when unroutable.
		final var blackhole = "192.0.2.1:9020";
		final var run = driver(List.of(live.get(0), blackhole), null, 4);

		final var started = System.nanoTime();
		for (var i = 0; i < 4; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "blackhole-" + i)));
		}
		final List<Operation<?>> results = new ArrayList<>();
		long liveDoneMillis = 0;
		for (var i = 0; i < 4; i++) {
			final var result = run.results().await();
			results.add(result);
			if (result.status() == Operation.Status.SUCC) {
				liveDoneMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
			}
		}

		final var liveResults = results.stream().filter(r -> r.status() == Operation.Status.SUCC).toList();
		assertEquals(2, liveResults.size());
		assertTrue(liveResults.stream().allMatch(r -> live.get(0).equals(r.nodeAddr())));
		assertEquals(2, results.stream().filter(r -> r.status() == Operation.Status.FAIL_IO).count());
		// Both live requests finish before the stalled connects' 1 s timeout could release the dispatcher.
		assertTrue(liveDoneMillis < 800, "live requests took " + liveDoneMillis + " ms");
		assertEquals(3, requests.size());
		assertEquals(0, run.driver().activeOpCount());
	}

	@Test
	void closeReleasesEveryConnection() throws Exception {
		final var endpoints = listeners(2);
		final var run = driver(endpoints, null, 2);
		for (var i = 0; i < 4; i++) {
			assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "close-" + i)).status());
		}
		final var pool = run.driver().connectionPool();
		assertEquals(2, pool.openChannelCount());

		run.driver().close();

		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (pool.openChannelCount() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, pool.openChannelCount());
		assertEquals(0, pool.idleChannelCount());
	}

	@Test
	void tlsCarriesTheLogicalHostnameAsSniForJdkProviders() throws Exception {
		for (final var pqcMode : List.of("off", "prefer")) {
			try (final var hello = new ClientHelloListener()) {
				final var run = driver(List.of("127.0.0.1:" + hello.port()), HOSTNAME, 1, config -> {
					config.val("storage-net-ssl-enabled", true);
					config.val("storage-net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
					config.val("storage-net-ssl-provider", "JDK");
					config.val("storage-net-ssl-pqcMode", pqcMode);
					config.val("storage-net-ssl-jsseProvider", "BCJSSE");
				});

				final var result = run.execute(dataOp(OpType.CREATE, "tls-" + pqcMode));

				assertTrue(result.status() != Operation.Status.SUCC);
				assertEquals(HOSTNAME, hello.serverName(), "pqcMode " + pqcMode);
			}
		}
	}

	@Test
	void tlsWithoutLogicalHostnameSendsNoSni() throws Exception {
		try (final var hello = new ClientHelloListener()) {
			final var run = driver(List.of("127.0.0.1:" + hello.port()), null, 1, config -> {
				config.val("storage-net-ssl-enabled", true);
				config.val("storage-net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
				config.val("storage-net-ssl-provider", "JDK");
				config.val("storage-net-ssl-pqcMode", "off");
			});

			run.execute(dataOp(OpType.CREATE, "tls-no-sni"));

			assertTrue(hello.helloReceived());
			assertNull(hello.serverName());
		}
	}

	@Test
	void compositeReadRangeRequestsTakeTurnsWithoutSyntheticPasses() throws Exception {
		final var endpoints = listeners(3);
		final var run = driver(endpoints, null, 1);
		final var item = new DataItemImpl("ranged", 9, 3L * ITEM_SIZE);
		final var read = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(run.driver().put(read));
		awaitRequests(4);
		final var result = awaitComposite(run);

		assertEquals(Operation.Status.SUCC, result.status());
		assertEquals(List.of("HEAD", "GET", "GET", "GET"), requests.stream().map(Captured::method).toList());
		assertEquals(List.of(0, 1, 2, 0), listenerSequence());
	}

	@Test
	void retriedPartTakesALaterTurn() throws Exception {
		failFirstPart = true;
		final var endpoints = listeners(3);
		final var run = driver(endpoints, null, 1);
		final var item = new DataItemImpl("retried", 7, 3L * ITEM_SIZE);
		final var upload = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(run.driver().put(upload));
		final var result = awaitMultipart(run);

		assertEquals(Operation.Status.SUCC, result.status());
		final var parts = requests.stream().filter(r -> r.query() != null && r.query().contains("partNumber="))
						.map(r -> r.query().replaceAll(".*partNumber=(\\d+).*", "$1")).toList();
		// Parts are released one at a time here, so the retry precedes parts 2 and 3.
		assertEquals(List.of("1", "1", "2", "3"), parts);
		// HEAD, INIT, part 1 (fails), part 1 retry on the next turn, part 2, part 3, complete.
		assertEquals(List.of(0, 1, 2, 0, 1, 2, 0), listenerSequence());
	}

	@Test
	void abortAfterFailedCompletionFollowsTheRotation() throws Exception {
		completeStatus = 500;
		final var endpoints = listeners(3);
		final var run = driver(endpoints, HOSTNAME, 1);
		final var item = new DataItemImpl("aborted", 5, 3L * ITEM_SIZE);
		final var upload = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(run.driver().put(upload));
		awaitRequests(7);

		final var abort = requests.get(6);
		assertEquals("DELETE", abort.method());
		assertTrue(abort.query().contains("uploadId=upload-1"), abort.query());
		// HEAD, INIT, three parts, failed complete, abort on the next turn.
		assertEquals(List.of(0, 1, 2, 0, 1, 2, 0), listenerSequence());
		assertEquals(HOSTNAME + ":" + endpoints.get(0).split(":")[1], abort.host());
	}

	@Test
	void everyDriverInstanceStartsAtTheFirstEndpoint() throws Exception {
		final var endpoints = listeners(3);
		final var first = driver(endpoints, null, 1);
		final var second = driver(endpoints, null, 1);

		assertEquals(Operation.Status.SUCC, first.execute(dataOp(OpType.CREATE, "first")).status());
		assertEquals(Operation.Status.SUCC, second.execute(dataOp(OpType.CREATE, "second")).status());

		assertEquals(List.of(0, 1, 0, 1), listenerSequence());
	}

	@Test
	void stopDuringPendingConnectFailsTheOperationOnceAndClosesConnections() throws Exception {
		final var live = listeners(1);
		final var run = driver(List.of(live.get(0), "192.0.2.1:9020"), null, 1);
		// The bucket check takes the live turn; the operation's connect to the blackhole stays pending.
		assertTrue(run.driver().put(dataOp(OpType.CREATE, "pending")));
		Thread.sleep(100);
		final var pool = run.driver().connectionPool();

		run.driver().close();

		// Like an in-flight request interrupted during shutdown, the pending setup fails once.
		final var result = run.results().poll(1_500);
		assertNotNull(result);
		assertEquals(Operation.Status.FAIL_IO, result.status());
		assertNull(run.results().poll(500));
		assertEquals(0, pool.openChannelCount());
		assertEquals(List.of(0), listenerSequence());
	}

	// ---------- fixtures ----------

	private interface ConfigTweak {
		void apply(Config config);
	}

	private record Run(S3EndpointSelectionDriver<DataItem, Operation<DataItem>> driver, Results results) {
		Operation<?> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}

	}

	private Run driver(final List<String> endpoints, final String hostname, final int concurrency) throws Exception {
		return driver(endpoints, hostname, concurrency, config -> {});
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private Run driver(final List<String> endpoints, final String hostname, final int concurrency,
					final ConfigTweak tweak) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", concurrency);
		config.val("storage-net-timeoutMilliSec", 5_000);
		config.val("storage-net-node-addrs", endpoints);
		config.val("storage-net-endpoint-selection", "round-robin");
		config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		if (hostname != null) {
			config.val("storage-net-endpoint-hostname", hostname);
		}
		tweak.apply(config);
		final var created = new S3StorageDriverExtension().create(
						"round-robin-test", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), false, 16);
		final var driver = assertInstanceOf(S3EndpointSelectionDriver.class, created);
		resources.add(driver);
		final var results = new Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	private static DataOperation<DataItem> dataOp(final OpType type, final String name) {
		return new DataOperationImpl<>(0, type, new DataItemImpl(name, name.hashCode() & 0xFFFF, ITEM_SIZE), null,
						"/bucket", CREDENTIAL, null, 0);
	}

	private List<String> listeners(final int count) throws IOException {
		final List<String> endpoints = new ArrayList<>();
		for (var i = 0; i < count; i++) {
			final var index = i;
			final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			server.createContext("/", exchange -> handle(index, exchange));
			server.setExecutor(serverThreads);
			server.start();
			servers.add(server);
			endpoints.add("127.0.0.1:" + server.getAddress().getPort());
		}
		return endpoints;
	}

	private void handle(final int listener, final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var method = exchange.getRequestMethod();
			final var query = uri.getRawQuery();
			requests.add(new Captured(listener, method, uri.getRawPath(), query,
							exchange.getRequestHeaders().getFirst("Host"), exchange.getRemoteAddress().getPort()));
			byte[] body = new byte[0];
			final var range = exchange.getRequestHeaders().getFirst("Range");
			if ("PUT".equals(method) && query != null && query.contains("partNumber=1&") && failFirstPart) {
				failFirstPart = false;
				exchange.sendResponseHeaders(500, -1);
				return;
			}
			if ("POST".equals(method) && query != null && !query.contains("uploads") && completeStatus != 200) {
				exchange.sendResponseHeaders(completeStatus, -1);
				return;
			}
			if ("GET".equals(method) && range != null) {
				final var bounds = range.substring("bytes=".length()).split("-");
				final var first = Long.parseLong(bounds[0]);
				final var last = Long.parseLong(bounds[1]);
				exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + 3 * ITEM_SIZE);
				exchange.sendResponseHeaders(206, last - first + 1);
				exchange.getResponseBody().write(new byte[(int) (last - first + 1)]);
				return;
			}
			if ("POST".equals(method) && query != null && query.contains("uploads")) {
				body = ("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>"
								+ "<UploadId>upload-1</UploadId></InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
			} else if ("POST".equals(method)) {
				body = "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>"
								.getBytes(StandardCharsets.UTF_8);
			} else if ("GET".equals(method)) {
				body = new byte[ITEM_SIZE];
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			}
			final var status = "DELETE".equals(method) ? 204 : 200;
			exchange.sendResponseHeaders(status, "HEAD".equals(method) || body.length == 0 ? -1 : body.length);
			if (body.length > 0 && !"HEAD".equals(method)) {
				exchange.getResponseBody().write(body);
			}
		}
	}

	/**
	 * Composite operations publish intermediate snapshots; the finished upload is the composite result
	 * published after the listener received the completion request.
	 */
	private Operation<?> awaitComposite(final Run run) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		Operation<?> last = null;
		while (System.nanoTime() < deadline) {
			final var result = run.results().poll();
			if (result != null) {
				last = result;
			} else if (last instanceof CompositeDataOperationImpl<?> composite && composite.allSubOperationsDone()) {
				return last;
			}
		}
		throw new AssertionError("composite operation did not finish: " + requests);
	}

	private void awaitRequests(final int count) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (requests.size() < count && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(count, requests.size(), () -> "requests: " + requests);
	}

	private Operation<?> awaitMultipart(final Run run) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			final var result = run.results().poll();
			final var completed = requests.stream()
							.anyMatch(r -> "POST".equals(r.method()) && r.query() != null && !r.query().contains("uploads"));
			if (result != null && completed) {
				return result;
			}
		}
		throw new AssertionError("multipart upload did not finish: " + requests);
	}

	private List<Integer> listenerSequence() {
		return requests.stream().map(Captured::listener).toList();
	}

	private static int unusedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	private static final class Results implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<DataItem>> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}

		Operation<?> poll() throws InterruptedException {
			return poll(10);
		}

		Operation<?> poll(final long millis) throws InterruptedException {
			return results.poll(millis, TimeUnit.MILLISECONDS);
		}

		Operation<?> await() throws InterruptedException {
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "no operation result before the timeout");
			return result;
		}
	}

	/** Accepts one TLS connection, records the SNI host name from its ClientHello and closes it. */
	private static final class ClientHelloListener implements AutoCloseable {

		private static final int HANDSHAKE = 22;
		private static final int CLIENT_HELLO = 1;
		private static final int SERVER_NAME_EXTENSION = 0;

		private final ServerSocket socket = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
		private final Thread acceptor;
		private volatile boolean helloReceived;
		private volatile String serverName;

		ClientHelloListener() throws IOException {
			acceptor = new Thread(this::acceptOne, "client-hello-listener");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		int port() {
			return socket.getLocalPort();
		}

		boolean helloReceived() throws InterruptedException {
			acceptor.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			return helloReceived;
		}

		String serverName() throws InterruptedException {
			acceptor.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			return serverName;
		}

		private void acceptOne() {
			try (final var connection = socket.accept()) {
				final var in = connection.getInputStream();
				final var header = in.readNBytes(5);
				if (header.length < 5 || header[0] != HANDSHAKE) {
					return;
				}
				final var record = in.readNBytes(((header[3] & 0xFF) << 8) | (header[4] & 0xFF));
				if (record.length > 0 && record[0] == CLIENT_HELLO) {
					helloReceived = true;
					serverName = parseServerName(record);
				}
			} catch (final IOException ignored) {
				// The test asserts on what was captured before the connection ended.
			}
		}

		private static String parseServerName(final byte[] hello) {
			var at = 4 + 2 + 32; // handshake header, client version, random
			at += 1 + (hello[at] & 0xFF); // session id
			at += 2 + u16(hello, at); // cipher suites
			at += 1 + (hello[at] & 0xFF); // compression methods
			final var end = at + 2 + u16(hello, at);
			at += 2;
			while (at + 4 <= end) {
				final var type = u16(hello, at);
				final var length = u16(hello, at + 2);
				if (type == SERVER_NAME_EXTENSION) {
					final var nameLength = u16(hello, at + 4 + 3);
					return new String(hello, at + 4 + 5, nameLength, StandardCharsets.US_ASCII);
				}
				at += 4 + length;
			}
			return null;
		}

		private static int u16(final byte[] data, final int at) {
			return ((data[at] & 0xFF) << 8) | (data[at + 1] & 0xFF);
		}

		@Override
		public void close() throws IOException {
			socket.close();
		}
	}
}
