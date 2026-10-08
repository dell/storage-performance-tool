package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.HOSTNAME;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer;
import com.dell.spt.storage.driver.coop.netty.endpoint.SelectingConnectionPool;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Reply;
import com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ClientHelloListener;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.dns.DnsResponseCode;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/** Partial-object Reads with endpoint selection, against receiving loopback listeners. */
@Timeout(60)
final class S3RangeEndpointSelectionTest {

	private static final List<String> DNS_NODES = List.of("127.0.0.1", "127.0.0.2", "127.0.0.3");
	private static final int BIND_ATTEMPTS = 16;
	private static final long OBJECT_SIZE = 10;
	/** Bytes 2-4 of every object, so each request is predictable. */
	private static final RangeReadPolicy FIXED = new RangeReadPolicy(3, 2L, 1);
	private static final String FIXED_RANGE = "bytes=2-4";

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final AtomicInteger requestCount = new AtomicInteger();
	private volatile IntPredicate failRequest = n -> false;

	private record Captured(String listener, String host, String range, String authorization, String connection,
					int clientPort) {}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	@Test
	void roundRobinRangeReadsRotateFromTheFirstEndpointWithTheLogicalAuthority() throws Exception {
		final var endpoints = listeners(3);
		final var run = run(roundRobin(endpoints, HOSTNAME), FIXED, false);
		final List<String> recorded = new ArrayList<>();
		for (var i = 0; i < 4; i++) {
			run.read("rotate-" + i);
			final var result = run.outcome();
			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(3, result.countBytesDone());
			recorded.add(result.nodeAddr());
		}

		final var expected = List.of(endpoints.get(0), endpoints.get(1), endpoints.get(2), endpoints.get(0));
		assertEquals(expected, requests.stream().map(Captured::listener).toList());
		assertEquals(expected, recorded, "the trace records the selected address");
		for (final var request : requests) {
			assertEquals(HOSTNAME + request.listener().substring(request.listener().lastIndexOf(':')), request.host());
			assertEquals(FIXED_RANGE, request.range());
			assertTrue(signedHeaders(request.authorization()).contains("host"), request.authorization());
		}
		run.close();
		assertTrue(run.runtime.snapshot().reconciled());
		assertEquals(4, run.runtime.snapshot().logical().accepted());
	}

	@Test
	void roundRobinRetryTakesTheNextTurnAndKeepsTheRange() throws Exception {
		final var endpoints = listeners(3);
		failRequest = n -> n == 1;
		final var run = run(roundRobin(endpoints, null), new RangeReadPolicy(3, null, 1), true);

		run.read("retry");
		final var result = run.outcome();

		assertEquals(Operation.Status.SUCC, result.status());
		assertEquals(List.of(endpoints.get(0), endpoints.get(1)), requests.stream().map(Captured::listener).toList());
		assertEquals(requests.get(0).range(), requests.get(1).range(), "the retry keeps the selected range");
		assertEquals(endpoints.get(1), requests.get(1).host(), "without a hostname, Host is the selected address");
		assertEquals(endpoints.get(1), result.nodeAddr());
		run.close();
		final var snapshot = run.runtime.snapshot();
		assertEquals(2, snapshot.requestsSent());
		assertEquals(1, snapshot.httpAttemptFailures());
		assertEquals(1, snapshot.logical().accepted());
		assertTrue(snapshot.reconciled());
	}

	@Test
	void roundRobinReusesIdleConnectionsPerDestination() throws Exception {
		final var endpoints = listeners(2);
		final var run = run(roundRobin(endpoints, null), FIXED, false);
		for (var i = 0; i < 4; i++) {
			run.read("reuse-" + i);
			assertEquals(Operation.Status.SUCC, run.outcome().status());
		}

		assertEquals(List.of(endpoints.get(0), endpoints.get(1), endpoints.get(0), endpoints.get(1)),
						requests.stream().map(Captured::listener).toList());
		assertEquals(requests.get(0).clientPort(), requests.get(2).clientPort());
		assertEquals(requests.get(1).clientPort(), requests.get(3).clientPort());
		assertNotEquals(requests.get(0).clientPort(), requests.get(1).clientPort());
		assertEquals(2, run.driver.connectionPool().openChannelCount());
	}

	@Test
	void unreachableDestinationFailsItsAttemptsAsIoFailuresWithoutSkipping() throws Exception {
		final var endpoints = new ArrayList<>(listeners(1));
		endpoints.add("127.0.0.1:" + unusedPort());
		final var run = run(roundRobin(endpoints, null), FIXED, false);
		final List<Operation.Status> outcomes = new ArrayList<>();
		for (var i = 0; i < 3; i++) {
			run.read("refused-" + i);
			outcomes.add(run.outcome().status());
		}

		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.FAIL_IO, Operation.Status.SUCC), outcomes);
		assertEquals(List.of(endpoints.get(0), endpoints.get(0)), requests.stream().map(Captured::listener).toList());
		assertEquals(0, run.driver.activeOpCount(), "every permit is released");
		run.close();
		final var snapshot = run.runtime.snapshot();
		assertEquals(2, snapshot.requestsSent());
		assertEquals(1, snapshot.transportFailures());
		assertEquals(2, snapshot.logical().accepted());
		assertEquals(1, snapshot.logical().failed());
		assertTrue(snapshot.reconciled());
	}

	@Test
	void tlsCarriesTheLogicalHostnameAsSni() throws Exception {
		try (final var hello = new ClientHelloListener()) {
			final var storage = roundRobin(List.of("127.0.0.1:" + hello.port()), HOSTNAME);
			storage.val("net-ssl-enabled", true);
			storage.val("net-ssl-protocols", List.of("TLSv1.3", "TLSv1.2"));
			storage.val("net-ssl-provider", "JDK");
			storage.val("net-ssl-pqcMode", "off");
			final var run = run(storage, FIXED, false);

			run.read("tls");

			assertNotEquals(Operation.Status.SUCC, run.outcome().status());
			assertEquals(HOSTNAME, hello.serverName());
		}
	}

	@Test
	@EnabledOnOs(OS.LINUX)
	void perRequestDnsResolvesEveryAttemptAndClosesEachConnection() throws Exception {
		final var port = dnsListeners();
		final var dns = rotatingDns();
		final var run = run(perRequestDns(dns, port, 1_000), FIXED, false);
		final List<String> recorded = new ArrayList<>();
		for (var i = 0; i < 3; i++) {
			run.read("dns-" + i);
			final var result = run.outcome();
			assertEquals(Operation.Status.SUCC, result.status());
			recorded.add(result.nodeAddr());
		}

		final var expected = DNS_NODES.stream().map(node -> node + ":" + port).toList();
		assertEquals(expected, requests.stream().map(Captured::listener).toList());
		assertEquals(expected, recorded);
		assertEquals(3, dns.received().size(), "one lookup per attempt");
		for (final var request : requests) {
			assertEquals(HOSTNAME + ":" + port, request.host());
			assertEquals("close", request.connection());
			assertTrue(signedHeaders(request.authorization()).contains("host"), request.authorization());
		}
		assertEquals(3, requests.stream().map(Captured::clientPort).distinct().count(), "a new connection per attempt");
		awaitNoOpenConnections(run);
	}

	@Test
	@EnabledOnOs(OS.LINUX)
	void perRequestDnsRetryResolvesAgain() throws Exception {
		final var port = dnsListeners();
		final var dns = rotatingDns();
		failRequest = n -> n == 1;
		final var run = run(perRequestDns(dns, port, 1_000), FIXED, true);

		run.read("dns-retry");

		assertEquals(Operation.Status.SUCC, run.outcome().status());
		assertEquals(List.of(DNS_NODES.get(0) + ":" + port, DNS_NODES.get(1) + ":" + port),
						requests.stream().map(Captured::listener).toList());
		assertEquals(2, dns.received().size());
		awaitNoOpenConnections(run);
	}

	@Test
	void perRequestDnsLookupFailureFailsTheAttemptAsIoFailure() throws Exception {
		final var dns = dns(q -> Reply.code(DnsResponseCode.NXDOMAIN));
		final var run = run(perRequestDns(dns, unusedPort(), 1_000), FIXED, false);

		run.read("not-found");

		assertEquals(Operation.Status.FAIL_IO, run.outcome().status());
		assertTrue(requests.isEmpty());
		assertEquals(0, run.driver.activeOpCount());
		assertEquals(Map.of("NOT_FOUND", 1L), run.driver.counters().snapshot().lookupFailures());
		run.close();
		assertEquals(1, run.runtime.snapshot().transportFailures());
		assertTrue(run.runtime.snapshot().reconciled());
	}

	@Test
	void perRequestDnsLetsTheServerCloseFirst() throws Exception {
		try (final var server = new CloseOrderServer()) {
			final var dns = dns(q -> Reply.answers(q.name(), "127.0.0.1"));
			final var run = run(perRequestDns(dns, server.port(), 1_000), FIXED, false);

			run.read("close-order");

			assertEquals(Operation.Status.SUCC, run.outcome().status());
			assertFalse(server.clientClosedFirst(), "the client closed before the server's close");
			awaitNoOpenConnections(run);
		}
	}

	@Test
	void stopDuringAPendingLookupLeavesTheAttemptUnattempted() throws Exception {
		for (final var drainFirst : List.of(true, false)) {
			final var dns = dns(q -> null);
			final var run = run(perRequestDns(dns, unusedPort(), 30_000), FIXED, true);
			final var pool = run.driver.connectionPool();
			run.read("pending");
			awaitPendingAcquisition(pool);

			if (drainFirst) {
				drainAndClose(run);
			} else {
				run.driver.close();
			}

			// Like a default lease fenced before handoff: never sent, so unattempted rather than failed.
			assertNull(run.terminal.poll(300, TimeUnit.MILLISECONDS), "nothing is published for unattempted work");
			assertEquals(0, pool.pendingAcquisitionCount());
			assertEquals(0, pool.openChannelCount());
			assertEquals(0, run.driver.activeOpCount(), "the setup's permit was not released");
			run.close();
			final var snapshot = run.runtime.snapshot();
			assertEquals(1, snapshot.logical().unattempted(), "drain first: " + drainFirst);
			assertEquals(0, snapshot.requestsSent());
			assertEquals(0, snapshot.transportAttemptFailures());
			assertTrue(snapshot.reconciled());
		}
	}

	@Test
	void lookupAnsweredAfterAdmissionClosesSendsNothing() throws Exception {
		final var port = listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
		final var dns = dns(q -> Reply.answers(q.name(), "127.0.0.1").delayed(400));
		final var run = run(perRequestDns(dns, port, 5_000), FIXED, true);
		final var pool = run.driver.connectionPool();
		run.read("late");
		awaitPendingAcquisition(pool);

		run.runtime.closeAdmission();
		run.driver.closeAdmission();
		run.runtime.closeRetries();
		// The answer arrives and connects; the fenced handoff must cancel the request.
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (pool.pendingAcquisitionCount() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, pool.pendingAcquisitionCount());
		run.driver.close();

		assertNull(run.terminal.poll(300, TimeUnit.MILLISECONDS));
		assertTrue(requests.isEmpty(), "a request was sent after admission closed");
		assertEquals(0, run.driver.activeRangeTransports());
		assertEquals(0, run.driver.activeOpCount());
		awaitNoOpenConnections(run);
		run.close();
		final var snapshot = run.runtime.snapshot();
		assertEquals(1, snapshot.logical().unattempted());
		assertEquals(0, snapshot.requestsSent());
		assertTrue(snapshot.reconciled());
	}

	@Test
	void drainPublishesTheRetainedFailureOfARetryStillInSetup() throws Exception {
		final var port = listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
		failRequest = n -> true;
		for (final var httpFailure : List.of(false, true)) {
			final var lookups = new AtomicInteger();
			// The first attempt fails; the retry's lookup is never answered.
			final var dns = dns(q -> lookups.incrementAndGet() > 1 ? null
							: httpFailure ? Reply.answers(q.name(), "127.0.0.1") : Reply.code(DnsResponseCode.NXDOMAIN));
			final var run = run(perRequestDns(dns, port, 30_000), FIXED, true);
			final var pool = run.driver.connectionPool();
			run.read("retry-in-setup");
			awaitRetryInSetup(lookups, pool);

			run.runtime.closeAdmission();
			run.driver.closeAdmission();
			run.runtime.closeRetries();
			assertTrue(run.driver.recoverQueuedOperations().isEmpty());

			assertRetainedFailurePublishedOnce(run, httpFailure);
		}
	}

	@Test
	void lateAnswerForARetryPublishesTheRetainedFailure() throws Exception {
		final var port = listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
		failRequest = n -> true;
		final var lookups = new AtomicInteger();
		// The retry's answer arrives after admission closed, so its fenced handoff settles it.
		final var dns = dns(q -> lookups.incrementAndGet() > 1
						? Reply.answers(q.name(), "127.0.0.1").delayed(400)
						: Reply.answers(q.name(), "127.0.0.1"));
		final var run = run(perRequestDns(dns, port, 5_000), FIXED, true);
		run.read("late-retry");
		awaitRetryInSetup(lookups, run.driver.connectionPool());

		run.runtime.closeAdmission();
		run.driver.closeAdmission();
		run.runtime.closeRetries();

		assertRetainedFailurePublishedOnce(run, true);
		assertEquals(1, requests.size(), "the retry must not be sent");
	}

	private static void awaitRetryInSetup(final AtomicInteger lookups, final SelectingConnectionPool pool)
					throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while ((lookups.get() < 2 || pool.pendingAcquisitionCount() == 0) && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(2, lookups.get());
		assertEquals(1, pool.pendingAcquisitionCount());
	}

	/** One terminal outcome and one result, nothing left pending, and no delivery failure at close. */
	private static void assertRetainedFailurePublishedOnce(final Run run, final boolean httpFailure) throws Exception {
		final var result = run.outcome();
		assertEquals(httpFailure ? Operation.Status.RESP_FAIL_SVC : Operation.Status.FAIL_IO, result.status());
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (run.runtime.hasPendingResults() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertFalse(run.runtime.hasPendingResults());
		run.close();
		assertTrue(run.terminal.isEmpty(), "no second outcome");
		assertTrue(run.results.isEmpty(), "no second result");
		final var snapshot = run.runtime.snapshot();
		assertEquals(1, snapshot.logical().failed());
		assertEquals(httpFailure ? 1 : 0, snapshot.requestsSent());
		assertTrue(snapshot.reconciled());
	}

	private static void awaitPendingAcquisition(final SelectingConnectionPool pool) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (pool.pendingAcquisitionCount() == 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(1, pool.pendingAcquisitionCount());
	}

	/** The load step's drain order: admission and retries close before the transport does. */
	private static void drainAndClose(final Run run) throws Exception {
		run.runtime.closeAdmission();
		run.driver.closeAdmission();
		run.runtime.closeRetries();
		run.driver.close();
	}

	/** Drives a range driver through its runtime, as the range test fixture does. */
	private static final class Run implements AutoCloseable {

		final S3RangeEndpointSelectionDriver driver;
		final RangeReadRuntime<DataItem> runtime;
		final RangeReadPolicy policy;
		final ScheduledExecutorService retryThread = Executors.newSingleThreadScheduledExecutor();
		final BlockingQueue<Operation.Status> terminal = new LinkedBlockingQueue<>();
		final BlockingQueue<RangeReadOperation<DataItem>> results = new LinkedBlockingQueue<>();
		private boolean closed;

		@SuppressWarnings("unchecked")
		Run(final Config storage, final RangeReadPolicy policy, final boolean retry) throws Exception {
			this.policy = policy;
			driver = assertInstanceOf(S3RangeEndpointSelectionDriver.class,
							new S3StorageDriverExtension<DataItem, RangeReadOperation<DataItem>, S3StorageDriver<DataItem, RangeReadOperation<DataItem>>>()
											.createRangeRead("range-selection", new SeedDataInput(1, 1024, 1, true), storage, 4, policy));
			runtime = driver.rangeReadRuntime();
			final LoadGenerator<DataItem, RangeReadOperation<DataItem>> generator = mock(LoadGenerator.class);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				final RangeReadOperation<DataItem> op = call.getArgument(0);
				final RangeReadAttempt attempt = call.getArgument(1);
				return op.circulation().claimRetryQueue(attempt) && runtime.admission().put(op);
			});
			driver.operationResultOutput(runtime.bind(generator, driver.operationLifecycle(), retry, 1,
							(delay, task) -> retryThread.schedule(task, delay, TimeUnit.MILLISECONDS),
							op -> terminal.add(op.status()), results::add));
			driver.start();
		}

		void read(final String name) {
			final var op = new RangeReadOperation<DataItem>(0, new DataItemImpl(name, 0, OBJECT_SIZE), "/bucket", "/bucket",
							null, policy);
			assertTrue(runtime.tracker().generatorBuffered(op));
			assertTrue(runtime.admission().put(op));
		}

		/** The next terminal outcome, as its trace result. */
		RangeReadOperation<DataItem> outcome() throws InterruptedException {
			final var status = terminal.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(status, () -> "No terminal result: " + runtime.snapshot());
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "Every determinate outcome reaches the trace callback");
			assertEquals(status, result.status());
			return result;
		}

		@Override
		public void close() throws Exception {
			if (closed) {
				return;
			}
			closed = true;
			try {
				runtime.closeAdmission();
				driver.closeAdmission();
				driver.close();
				runtime.close();
				assertEquals(0, driver.activeRangeTransports());
				assertNull(driver.terminalFailure());
				assertNull(runtime.failure());
			} finally {
				retryThread.shutdownNow();
			}
		}
	}

	private Run run(final Config storage, final RangeReadPolicy policy, final boolean retry) throws Exception {
		final var run = new Run(storage, policy, retry);
		resources.add(run);
		return run;
	}

	private static Config roundRobin(final List<String> endpoints, final String hostname) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		common(root);
		root.val("storage-net-node-addrs", endpoints);
		root.val("storage-net-endpoint-selection", "round-robin");
		if (hostname != null) {
			root.val("storage-net-endpoint-hostname", hostname);
		}
		return root.configVal("storage");
	}

	private static Config perRequestDns(final ScriptedDnsServer dns, final int port, final int dnsTimeoutMillis) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, HOSTNAME);
		common(root);
		root.val("storage-net-node-port", port);
		root.val("storage-net-endpoint-selection", "per-request-dns");
		root.val("storage-net-endpoint-dns-server", "127.0.0.1:" + dns.address().getPort());
		root.val("storage-net-endpoint-dns-timeoutMilliSec", dnsTimeoutMillis);
		return root.configVal("storage");
	}

	private static void common(final Config root) {
		root.val("storage-net-timeoutMilliSec", 2_000);
		root.val("storage-driver-limit-concurrency", 1);
		root.val("storage-driver-threads", 1);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
	}

	private ScriptedDnsServer rotatingDns() throws InterruptedException {
		final var next = new AtomicInteger();
		return dns(q -> Reply.answers(q.name(), DNS_NODES.get(next.getAndIncrement() % DNS_NODES.size())));
	}

	private ScriptedDnsServer dns(final Function<ScriptedDnsServer.Received, Reply> script) throws InterruptedException {
		final var dns = new ScriptedDnsServer(script);
		resources.add(dns);
		return dns;
	}

	private List<String> listeners(final int count) throws IOException {
		final List<String> endpoints = new ArrayList<>();
		for (var i = 0; i < count; i++) {
			endpoints.add("127.0.0.1:" + listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)));
		}
		return endpoints;
	}

	/** Listeners on every DNS node address, sharing one port. */
	private int dnsListeners() throws IOException {
		for (var attempt = 0; attempt < BIND_ATTEMPTS; attempt++) {
			try {
				var port = 0;
				for (final var node : DNS_NODES) {
					port = listen(new InetSocketAddress(node, port));
				}
				return port;
			} catch (final BindException e) {
				servers.forEach(server -> server.stop(0));
				servers.clear();
			}
		}
		throw new IllegalStateException("No free port on all loopback addresses");
	}

	private int listen(final InetSocketAddress address) throws IOException {
		final var server = HttpServer.create(address, 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
		servers.add(server);
		return server.getAddress().getPort();
	}

	/** Answers every range GET with the requested span of an {@code OBJECT_SIZE}-byte object. */
	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			final var headers = exchange.getRequestHeaders();
			final var local = exchange.getLocalAddress();
			final var range = headers.getFirst("Range");
			requests.add(new Captured(local.getAddress().getHostAddress() + ":" + local.getPort(), headers.getFirst("Host"),
							range, headers.getFirst("Authorization"), headers.getFirst("Connection"),
							exchange.getRemoteAddress().getPort()));
			if (failRequest.test(requestCount.incrementAndGet())) {
				final var error = "error".getBytes(StandardCharsets.US_ASCII);
				exchange.sendResponseHeaders(503, error.length);
				exchange.getResponseBody().write(error);
				return;
			}
			final var bounds = range.substring("bytes=".length()).split("-");
			final var length = Integer.parseInt(bounds[1]) - Integer.parseInt(bounds[0]) + 1;
			exchange.getResponseHeaders().set("Content-Range", "bytes " + bounds[0] + "-" + bounds[1] + "/" + OBJECT_SIZE);
			exchange.sendResponseHeaders(206, length);
			exchange.getResponseBody().write(new byte[length]);
		}
	}

	private static String signedHeaders(final String authorization) {
		final var start = authorization.indexOf("SignedHeaders=") + "SignedHeaders=".length();
		return authorization.substring(start, authorization.indexOf(',', start));
	}

	private static int unusedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	private static void awaitNoOpenConnections(final Run run) throws InterruptedException {
		final var pool = run.driver.connectionPool();
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (pool.openChannelCount() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, pool.openChannelCount());
	}

	/**
	 * Answers one range request with {@code Connection: close}, then holds the connection open for a
	 * while and records whether the client closed it first.
	 */
	private static final class CloseOrderServer implements AutoCloseable {

		private static final int HOLD_MILLIS = 300;

		private final ServerSocket socket = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
		private final Thread acceptor;
		private volatile boolean clientClosedFirst;

		CloseOrderServer() throws IOException {
			acceptor = new Thread(this::serveOne, "close-order-server");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		int port() {
			return socket.getLocalPort();
		}

		boolean clientClosedFirst() throws InterruptedException {
			acceptor.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			assertFalse(acceptor.isAlive());
			return clientClosedFirst;
		}

		private void serveOne() {
			try (final var connection = socket.accept()) {
				final var in = connection.getInputStream();
				var matched = 0;
				final var end = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
				while (matched < end.length) {
					final var b = in.read();
					if (b < 0) {
						return;
					}
					matched = b == end[matched] ? matched + 1 : (b == end[0] ? 1 : 0);
				}
				connection.getOutputStream().write(("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/" + OBJECT_SIZE
								+ "\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc").getBytes(StandardCharsets.US_ASCII));
				connection.getOutputStream().flush();
				connection.setSoTimeout(HOLD_MILLIS);
				try {
					clientClosedFirst = in.read() < 0;
				} catch (final SocketTimeoutException stillOpen) {
					clientClosedFirst = false;
				}
			} catch (final IOException ignored) {
				// The test asserts on what was observed before the connection ended.
			}
		}

		@Override
		public void close() throws IOException {
			socket.close();
		}
	}
}
