package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.HOSTNAME;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer.Reply;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.dns.DnsResponseCode;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real-driver per-request DNS against a scripted DNS server and listeners on several loopback
 * addresses sharing one port. Linux routes all of 127.0.0.0/8 to the loopback interface.
 */
@EnabledOnOs(OS.LINUX)
@Timeout(60)
final class S3EndpointSelectionDnsTest {

	private static final List<String> NODES = List.of("127.0.0.1", "127.0.0.2", "127.0.0.3");
	private static final int BIND_ATTEMPTS = 16;

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private int port;

	@TempDir
	Path dir;

	private record Captured(String node, String method, String host, String connection, int clientPort) {}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	@Test
	void everyAttemptResolvesAfreshAndUsesANewConnection() throws Exception {
		listeners();
		final var dns = rotatingDns();
		final var run = driver(dns, 1);

		final List<String> selected = new ArrayList<>();
		for (var i = 0; i < 4; i++) {
			final var result = run.execute(dataOp(OpType.CREATE, "fresh-" + i));
			assertEquals(Operation.Status.SUCC, result.status());
			selected.add(result.nodeAddr());
		}

		// The first operation's bucket check is a helper request; it resolves too.
		assertEquals(5, dns.received().size());
		assertEquals(List.of("HEAD", "PUT", "PUT", "PUT", "PUT"), requests.stream().map(Captured::method).toList());
		assertEquals(List.of("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.1", "127.0.0.2"),
						requests.stream().map(Captured::node).toList());
		assertEquals(List.of("127.0.0.2:" + port, "127.0.0.3:" + port, "127.0.0.1:" + port, "127.0.0.2:" + port),
						selected);
		assertEquals(5, requests.stream().map(r -> r.node() + ":" + r.clientPort()).distinct().count());
		for (final var request : requests) {
			assertEquals(HOSTNAME + ":" + port, request.host());
		}
		requests.stream().filter(r -> "PUT".equals(r.method()))
						.forEach(r -> assertEquals("close", r.connection()));
		awaitNoOpenConnections(run);
	}

	@Test
	void concurrentAttemptsEachResolveAndCloseTheirConnections() throws Exception {
		listeners();
		final var dns = rotatingDns();
		final var run = driver(dns, 8);

		for (var i = 0; i < 24; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "concurrent-" + i)));
		}
		for (var i = 0; i < 24; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}

		assertEquals(25, dns.received().size());
		assertEquals(25, requests.size());
		assertEquals(3, requests.stream().map(Captured::node).distinct().count());
		assertEquals(0, run.driver().activeOpCount());
		awaitNoOpenConnections(run);
	}

	@Test
	void lookupFailuresFailTheAttemptWithoutFallback() throws Exception {
		listeners();
		for (final Function<ScriptedDnsServer.Received, Reply> failure : List.<Function<ScriptedDnsServer.Received, Reply>> of(
						q -> Reply.code(DnsResponseCode.SERVFAIL),
						q -> Reply.code(DnsResponseCode.NXDOMAIN),
						q -> Reply.code(DnsResponseCode.NOERROR),
						q -> null)) {
			requests.clear();
			final var answered = new AtomicInteger();
			// Answer the bucket check, then fail every later lookup.
			final var dns = dns(q -> answered.getAndIncrement() == 0 ? Reply.answers(q.name(), "127.0.0.1") : failure.apply(q));
			final var run = driver(dns, 1);

			final var result = run.execute(dataOp(OpType.CREATE, "failing"));

			assertEquals(Operation.Status.FAIL_IO, result.status());
			assertEquals(List.of("HEAD"), requests.stream().map(Captured::method).toList());
			assertEquals(0, run.driver().activeOpCount());
		}
	}

	@Test
	void hostConfiguredModeReadsTheResolverConfiguration() throws Exception {
		final var resolvConf = Files.writeString(dir.resolve("resolv.conf"),
						"search corp.example.test\nnameserver fd00::53\nnameserver 192.0.2.53\nnameserver 192.0.2.54\n");
		final var config = config(null, 1);

		final S3EndpointSelectionDriver<DataItem, Operation<DataItem>> driver = S3EndpointSelectionDriver.create(
						"dns-host-test", new SeedDataInput(1, ITEM_SIZE, 1, true), config,
						false, 16, EndpointSelectionSettings.fromStorage(config), resolvConf);
		resources.add(driver);

		assertTrue(driver.hostConfiguredDns());
		assertEquals(List.of(new InetSocketAddress("192.0.2.53", 53), new InetSocketAddress("192.0.2.54", 53)),
						driver.dnsServers());
	}

	@Test
	void hostConfiguredModeWithoutIpv4ServersFailsBeforeConstruction() throws Exception {
		final var resolvConf = Files.writeString(dir.resolve("resolv.conf"), "nameserver ::1\n");
		final var config = config(null, 1);

		final var failure = assertThrows(IllegalConfigurationException.class, () -> S3EndpointSelectionDriver.create(
						"dns-host-test", new SeedDataInput(1, ITEM_SIZE, 1, true), config,
						false, 16, EndpointSelectionSettings.fromStorage(config), resolvConf));

		assertTrue(failure.getMessage().contains("host DNS configuration"), failure.getMessage());
	}

	@Test
	void stopDuringPendingLookupFailsTheOperationOnce() throws Exception {
		listeners();
		final var answered = new AtomicInteger();
		final var dns = dns(q -> answered.getAndIncrement() == 0 ? Reply.answers(q.name(), "127.0.0.1") : null);
		final var run = driver(dns, 1);
		assertTrue(run.driver().put(dataOp(OpType.CREATE, "pending")));
		Thread.sleep(100);

		final var started = System.nanoTime();
		run.driver().close();

		assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 10_000);
		final var result = run.results().poll(1_500);
		if (result != null) {
			assertEquals(Operation.Status.FAIL_IO, result.status());
		}
		assertNull(run.results().poll(500));
		assertEquals(0, run.driver().connectionPool().openChannelCount());
	}

	@Test
	void shortLoopbackStressRunLeavesNothingOpen() throws Exception {
		listeners();
		final var dns = rotatingDns();
		final var run = driver(dns, 8);
		final var operations = 300;

		final var started = System.nanoTime();
		for (var i = 0; i < operations; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "stress-" + i)));
		}
		for (var i = 0; i < operations; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		final var seconds = (System.nanoTime() - started) / 1e9;

		assertEquals(operations + 1, dns.received().size());
		assertEquals(operations + 1, new HashSet<>(requests.stream().map(r -> r.node() + ":" + r.clientPort()).toList()).size());
		awaitNoOpenConnections(run);
		System.out.printf("per-request DNS loopback: listener port %d, %d requests in %.2f s (%.0f req/s), concurrency 8%n", port,
						operations, seconds, operations / seconds);
	}

	// ---------- fixtures ----------

	private record Run(S3EndpointSelectionDriver<DataItem, Operation<DataItem>> driver,
					EndpointSelectionTestSupport.Results results) {
		Operation<?> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}
	}

	private ScriptedDnsServer rotatingDns() throws InterruptedException {
		final var next = new AtomicInteger();
		return dns(q -> Reply.answers(q.name(), NODES.get(next.getAndIncrement() % NODES.size())));
	}

	private ScriptedDnsServer dns(final Function<ScriptedDnsServer.Received, Reply> script) throws InterruptedException {
		final var dns = new ScriptedDnsServer(script);
		resources.add(dns);
		return dns;
	}

	private Config config(final ScriptedDnsServer dns, final int concurrency) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, HOSTNAME);
		root.val("storage-driver-limit-concurrency", concurrency);
		root.val("storage-net-timeoutMilliSec", 5_000);
		root.val("storage-net-node-port", port == 0 ? 9020 : port);
		root.val("storage-net-endpoint-selection", "per-request-dns");
		root.val("storage-net-endpoint-dns-timeoutMilliSec", 500);
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		if (dns != null) {
			root.val("storage-net-endpoint-dns-server", "127.0.0.1:" + dns.address().getPort());
		}
		return root.configVal("storage");
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private Run driver(final ScriptedDnsServer dns, final int concurrency) throws Exception {
		final var created = new S3StorageDriverExtension().create(
						"dns-test", new SeedDataInput(1, ITEM_SIZE, 1, true), config(dns, concurrency), false, 16);
		final var driver = assertInstanceOf(S3EndpointSelectionDriver.class, created);
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	private void listeners() throws IOException {
		for (var attempt = 0; attempt < BIND_ATTEMPTS; attempt++) {
			try {
				bindListeners();
				return;
			} catch (final BindException e) {
				servers.forEach(server -> server.stop(0));
				servers.clear();
			}
		}
		throw new IllegalStateException("No free port on all loopback addresses");
	}

	private void bindListeners() throws IOException {
		port = 0;
		for (final var node : NODES) {
			final var server = HttpServer.create(new InetSocketAddress(node, port), 0);
			server.createContext("/", this::handle);
			server.setExecutor(serverThreads);
			server.start();
			servers.add(server);
			port = server.getAddress().getPort();
		}
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			requests.add(new Captured(exchange.getLocalAddress().getAddress().getHostAddress(), exchange.getRequestMethod(),
							exchange.getRequestHeaders().getFirst("Host"), exchange.getRequestHeaders().getFirst("Connection"),
							exchange.getRemoteAddress().getPort()));
			if ("PUT".equals(exchange.getRequestMethod())) {
				exchange.getResponseHeaders().set("ETag", "\"etag\"");
			}
			exchange.sendResponseHeaders(200, -1);
		}
	}

	private static void awaitNoOpenConnections(final Run run) throws InterruptedException {
		final var pool = run.driver().connectionPool();
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (pool.openChannelCount() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, pool.openChannelCount());
	}
}
