package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.nic.Ipv4;
import com.dell.spt.base.nic.Ipv4Prefix;
import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.nic.NicBinding;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real S3 drivers with a NIC set against loopback listeners that record each request's source
 * address. Two loopback addresses stand in for two NICs: the fixture inventory gives each its own
 * address and port, and the source-address binding replaces the device binding, which loopback
 * cannot show.
 */
@Timeout(60)
final class S3NicSetDriverTest {

	private static final String NIC_A = "lo-a";
	private static final String NIC_B = "lo-b";
	private static final String ADDRESS_A = "127.0.0.2";
	private static final String ADDRESS_B = "127.0.0.3";
	private static final String KERNEL_SOURCE = "127.0.0.1";
	/** The fixture resolver answers it with the listeners' address. */
	private static final String HOSTNAME = "s3-b.example";
	private static final long OBJECT_SIZE = 10;
	/** Bytes 2-4 of every object. */
	private static final RangeReadPolicy FIXED = new RangeReadPolicy(3, 2L, 1);

	private static final NicInventory.Sysctls SYSCTLS = new NicInventory.Sysctls(1, 2, 2);
	private static final NicInventory INVENTORY = new NicInventory(
					List.of(nic(NIC_A, ADDRESS_A, "0000:01:00.0"), nic(NIC_B, ADDRESS_B, "0000:01:00.1")),
					List.of(),
					List.of(route(NIC_A), route(NIC_B)),
					SYSCTLS);
	private static final NicSetSupport.Environment ENVIRONMENT = new NicSetSupport.Environment(
					() -> INVENTORY,
					host -> HOSTNAME.equals(host) ? Ipv4.parse(KERNEL_SOURCE) : Ipv4.parse(host),
					NicBinding.SOURCE_ADDRESS);

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();

	private record Captured(int listener, String method, String host, String source, String range) {}

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
		awaitNoNicIoThreads();
	}

	@Test
	void theS3FactoryRoutesANicSetToTheNicSetDrivers() throws Exception {
		final var factory = new S3StorageDriverExtension<DataItem, Operation<DataItem>, S3StorageDriver<DataItem, Operation<DataItem>>>();
		assertInstanceOf(NicSetDriverFactory.class, factory);
		// Construction reads nothing from the host; the NICs are resolved at start.
		final var storage = storage(List.of("192.0.2.20:9000"), List.of("ens2np0"), 2);
		try (final var driver = factory.create("factory", new SeedDataInput(1, ITEM_SIZE, 1, true), storage, false, 16)) {
			assertInstanceOf(S3NicSetDriver.class, driver);
		}
		try (final var driver = new S3StorageDriverExtension<>().createRangeRead("factory", new SeedDataInput(1, ITEM_SIZE, 1, true),
						storage, 4, FIXED)) {
			assertInstanceOf(S3RangeNicSetDriver.class, driver);
		}
	}

	@Test
	void settingsThatCannotBindFailBeforeConstruction() {
		final var roundRobin = storage(List.of("192.0.2.20:9000", "192.0.2.21:9000"), List.of("ens2np0"), 2);
		roundRobin.val("net-endpoint-selection", "round-robin");
		final var nio = storage(List.of("192.0.2.20:9000"), List.of("ens2np0"), 2);
		nio.val("net-transport", "nio");
		final var attempts = storage(List.of("192.0.2.20:9000"), List.of("ens2np0"), 2);
		attempts.val("net-node-connAttemptsLimit", 3);

		for (final var bad : List.of(roundRobin, nio, attempts)) {
			assertThrows(IllegalConfigurationException.class,
							() -> new S3StorageDriverExtension<>().create("factory", null, bad, false, 1));
			assertThrows(IllegalConfigurationException.class,
							() -> new S3StorageDriverExtension<>().createRangeRead("factory", null, bad, 4, FIXED));
		}
	}

	@Test
	void eachRequestLeavesThroughItsPathsNicWithTheConfiguredEndpointAsHost() throws Exception {
		final var listeners = listeners(2);
		final var named = HOSTNAME + ":" + port(listeners.get(1));
		final var endpoints = List.of(listeners.get(0), named);
		final var run = started(endpoints, List.of(NIC_A, NIC_B), 2);

		final var pool = run.driver().connectionPool();
		assertEquals(List.of(NIC_A, NIC_B), pool.state().stream().map(NicSetConnectionPool.NicState::nic).toList());
		assertEquals(List.of(List.of(endpoints.get(0)), List.of(named)), pool.state().stream()
						.map(nic -> nic.paths().stream().map(NicSetConnectionPool.PathState::endpoint).toList()).toList(),
						"two NICs and two endpoints pair one to one");

		final List<String> recorded = new ArrayList<>();
		for (var i = 0; i < 6; i++) {
			final var result = run.execute(dataOp(OpType.CREATE, "nic-" + i));
			assertEquals(Operation.Status.SUCC, result.status());
			recorded.add(result.nodeAddr());
		}

		assertEquals(3, recorded.stream().filter(endpoints.get(0)::equals).count(), () -> "recorded: " + recorded);
		assertEquals(3, recorded.stream().filter(named::equals).count(), "the operation records the configured string");
		final var puts = requests.stream().filter(request -> "PUT".equals(request.method())).toList();
		assertEquals(6, puts.size(), () -> "requests: " + requests);
		for (final var put : puts) {
			if (put.listener() == 0) {
				assertEquals(ADDRESS_A, put.source(), "endpoint 0's Path is on " + NIC_A);
				assertEquals(endpoints.get(0), put.host());
			} else {
				assertEquals(ADDRESS_B, put.source(), "endpoint 1's Path is on " + NIC_B);
				assertEquals(named, put.host(), "Host is the configured string, not the frozen address");
			}
		}
		// The bucket check is a control request on the kernel route.
		final var helpers = requests.stream().filter(request -> !"PUT".equals(request.method())).toList();
		assertFalse(helpers.isEmpty(), () -> "requests: " + requests);
		helpers.forEach(helper -> assertEquals(KERNEL_SOURCE, helper.source(), helper.toString()));

		assertFalse(nicIoThreads(NIC_A).isEmpty());
		assertFalse(nicIoThreads(NIC_B).isEmpty());
		run.driver().close();
		awaitNoNicIoThreads();
		assertEquals(0, pool.state().stream().mapToInt(NicSetConnectionPool.NicState::open).sum());
	}

	@Test
	void aRefusedConnectFailsTheOperationAsAnIoFailureAndTheRotationGoesOn() throws Exception {
		final var listeners = listeners(2);
		final var run = started(listeners, List.of(NIC_A, NIC_B), 2);
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "before-0")).status());
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "before-1")).status());

		servers.get(1).stop(0);
		awaitRefused(listeners.get(1));
		final var pool = run.driver().connectionPool();
		waitFor(() -> pool.state().get(1).open() == 0, "the closed idle connection is dropped");

		final List<Operation.Status> statuses = new ArrayList<>();
		for (var i = 0; i < 4; i++) {
			statuses.add(run.execute(dataOp(OpType.CREATE, "after-" + i)).status());
		}

		final var ok = Operation.Status.SUCC;
		final var failed = Operation.Status.FAIL_IO;
		// The failing NIC keeps its turns: no rotation skip (P1).
		assertEquals(List.of(ok, failed, ok, failed), statuses);
		assertEquals(0, run.driver().activeOpCount(), "every failed setup released its permit");
		assertEquals(0, pool.pendingCount());
	}

	@Test
	void aNicTheHostLacksFailsTheStartAsAConfigurationError() throws Exception {
		final var driver = created(listeners(1), List.of(NIC_A, "lo-z"), 2);

		final var thrown = assertThrows(RuntimeException.class, driver::start);

		final var terminal = RunTerminalException.find(thrown);
		assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, terminal.category());
		assertTrue(terminal.getMessage().contains("lo-z"), terminal.getMessage());
		assertTrue(requests.isEmpty());
	}

	@Test
	void aRepeatedEndpointFailsTheStartAsAConfigurationError() throws Exception {
		final var endpoint = listeners(1).get(0);
		final var driver = created(List.of(endpoint, endpoint), List.of(NIC_A, NIC_B), 2);

		final var thrown = assertThrows(RuntimeException.class, driver::start);

		final var terminal = RunTerminalException.find(thrown);
		assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, terminal.category());
		assertTrue(terminal.getMessage().contains("endpoint " + endpoint + " is given more than once"), terminal.getMessage());
		awaitNoNicIoThreads();
		assertTrue(requests.isEmpty());
	}

	@Test
	void aPathThatRefusesFailsTheStartNamingTheNicAndTheEndpoint() throws Exception {
		final var refused = KERNEL_SOURCE + ":" + unusedPort();
		final var driver = created(List.of(listeners(1).get(0), refused), List.of(NIC_A, NIC_B), 2);

		final var thrown = assertThrows(RuntimeException.class, driver::start);

		final var terminal = RunTerminalException.find(thrown);
		assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, terminal.category());
		assertTrue(terminal.getMessage().contains("NIC " + NIC_B + " (" + ADDRESS_B + ") cannot connect to endpoint " + refused
						+ " (" + refused + "): connection refused"), terminal.getMessage());
		assertFalse(terminal.getMessage().contains("NIC " + NIC_A), terminal.getMessage());
		// The groups made for the start are shut down before it fails.
		awaitNoNicIoThreads();
		assertTrue(requests.isEmpty(), "the probe sends no request");
	}

	@Test
	void rangeReadsTakeTheirConnectionsFromThePathPools() throws Exception {
		final var listeners = listeners(2);
		final var named = HOSTNAME + ":" + port(listeners.get(1));
		final var endpoints = List.of(listeners.get(0), named);
		final var run = rangeRun(endpoints);

		final List<String> recorded = new ArrayList<>();
		for (var i = 0; i < 4; i++) {
			run.read("range-" + i);
			final var result = run.outcome();
			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(3, result.countBytesDone());
			recorded.add(result.nodeAddr());
		}

		assertEquals(2, recorded.stream().filter(named::equals).count(), () -> "recorded: " + recorded);
		assertEquals(4, requests.size(), () -> "requests: " + requests);
		for (final var request : requests) {
			assertEquals("bytes=2-4", request.range());
			assertEquals(request.listener() == 0 ? ADDRESS_A : ADDRESS_B, request.source());
			assertEquals(request.listener() == 0 ? endpoints.get(0) : named, request.host());
		}
		run.close();
		assertTrue(run.runtime.snapshot().reconciled());
		assertEquals(4, run.runtime.snapshot().logical().accepted());
	}

	private S3NicSetDriver<DataItem, Operation<DataItem>> created(final List<String> endpoints, final List<String> interfaces,
					final int concurrency) throws Exception {
		final var storage = storage(endpoints, interfaces, concurrency);
		final S3NicSetDriver<DataItem, Operation<DataItem>> driver = S3NicSetDriver.create("nic-set-test",
						new SeedDataInput(1, ITEM_SIZE, 1, true), storage, false, 16,
						NicSetSettings.fromStorage(storage).orElseThrow(), ENVIRONMENT);
		resources.add(driver);
		return driver;
	}

	private Run started(final List<String> endpoints, final List<String> interfaces, final int concurrency) throws Exception {
		final var driver = created(endpoints, interfaces, concurrency);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	private record Run(S3NicSetDriver<DataItem, Operation<DataItem>> driver, EndpointSelectionTestSupport.Results results) {

		Operation<?> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}
	}

	private RangeRun rangeRun(final List<String> endpoints) throws Exception {
		final var run = new RangeRun(storage(endpoints, List.of(NIC_A, NIC_B), 1));
		resources.add(run);
		return run;
	}

	/** Drives the range driver through its runtime, as the range endpoint-selection tests do. */
	private static final class RangeRun implements AutoCloseable {

		final S3RangeNicSetDriver driver;
		final RangeReadRuntime<DataItem> runtime;
		final ScheduledExecutorService retryThread = Executors.newSingleThreadScheduledExecutor();
		final BlockingQueue<Operation.Status> terminal = new LinkedBlockingQueue<>();
		final BlockingQueue<RangeReadOperation<DataItem>> results = new LinkedBlockingQueue<>();
		private boolean closed;

		@SuppressWarnings("unchecked")
		RangeRun(final Config storage) throws Exception {
			driver = S3RangeNicSetDriver.create("nic-set-range", new SeedDataInput(1, ITEM_SIZE, 1, true), storage, 4, FIXED,
							NicSetSettings.fromStorage(storage).orElseThrow(), ENVIRONMENT);
			runtime = driver.rangeReadRuntime();
			final LoadGenerator<DataItem, RangeReadOperation<DataItem>> generator = mock(LoadGenerator.class);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				final RangeReadOperation<DataItem> op = call.getArgument(0);
				final RangeReadAttempt attempt = call.getArgument(1);
				return op.circulation().claimRetryQueue(attempt) && runtime.admission().put(op);
			});
			driver.operationResultOutput(runtime.bind(generator, driver.operationLifecycle(), false, 1,
							(delay, task) -> retryThread.schedule(task, delay, TimeUnit.MILLISECONDS),
							op -> terminal.add(op.status()), results::add));
			driver.start();
		}

		void read(final String name) {
			final var op = new RangeReadOperation<DataItem>(0, new DataItemImpl(name, 0, OBJECT_SIZE), "/bucket", "/bucket",
							null, FIXED);
			assertTrue(runtime.tracker().generatorBuffered(op));
			assertTrue(runtime.admission().put(op));
		}

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

	private static Config storage(final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, KERNEL_SOURCE);
		root.val("storage-net-transport", "epoll");
		root.val("storage-net-interfaces", interfaces);
		root.val("storage-net-node-addrs", endpoints);
		root.val("storage-net-timeoutMilliSec", 2_000);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-driver-limit-concurrency", concurrency);
		root.val("storage-driver-threads", 1);
		return root.configVal("storage");
	}

	private static NicInventory.Nic nic(final String name, final String address, final String port) {
		return new NicInventory.Nic(name, NicInventory.Kind.PHYSICAL, null, true, List.of(Ipv4Prefix.parse(address + "/8")),
						25_000, 1_500, 0, port, List.of(), null, null, null, List.of(), SYSCTLS);
	}

	private static NicInventory.Route route(final String nic) {
		return new NicInventory.Route(nic, Ipv4.parse("127.0.0.0"), Ipv4.mask(8), 0, false);
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
			endpoints.add(KERNEL_SOURCE + ":" + server.getAddress().getPort());
		}
		return endpoints;
	}

	private void handle(final int listener, final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var method = exchange.getRequestMethod();
			final var range = exchange.getRequestHeaders().getFirst("Range");
			requests.add(new Captured(listener, method, exchange.getRequestHeaders().getFirst("Host"),
							exchange.getRemoteAddress().getAddress().getHostAddress(), range));
			if ("GET".equals(method) && range != null) {
				final var bounds = range.substring("bytes=".length()).split("-", -1);
				final var length = Integer.parseInt(bounds[1]) - Integer.parseInt(bounds[0]) + 1;
				exchange.getResponseHeaders().set("Content-Range", "bytes " + bounds[0] + "-" + bounds[1] + "/" + OBJECT_SIZE);
				exchange.sendResponseHeaders(206, length);
				exchange.getResponseBody().write(new byte[length]);
				return;
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			}
			exchange.sendResponseHeaders("DELETE".equals(method) ? 204 : 200, -1);
		}
	}

	private static int port(final String endpoint) {
		return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
	}

	private static int unusedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	/** A stopped listener's port can accept briefly after the close; wait until it refuses. */
	private static void awaitRefused(final String endpoint) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (System.nanoTime() < deadline) {
			try (final var socket = new Socket()) {
				socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port(endpoint)), 1_000);
			} catch (final ConnectException e) {
				return;
			} catch (final IOException e) {
				// retry
			}
			Thread.sleep(10);
		}
		fail(endpoint + " still accepts connections");
	}

	private static void waitFor(final BooleanSupplier condition, final String what) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), what);
	}

	private static List<String> nicIoThreads(final String nic) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.map(Thread::getName)
						.filter(name -> name.startsWith("ioWorker-" + nic))
						.toList();
	}

	private static void awaitNoNicIoThreads() throws InterruptedException {
		waitFor(() -> nicIoThreads(NIC_A).isEmpty() && nicIoThreads(NIC_B).isEmpty(),
						"the NICs' event loops stopped: " + nicIoThreads(NIC_A) + nicIoThreads(NIC_B));
	}
}
