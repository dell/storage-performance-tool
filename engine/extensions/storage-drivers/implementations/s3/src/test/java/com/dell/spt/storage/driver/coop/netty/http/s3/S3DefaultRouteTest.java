package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriverBase;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.MultiNodeConnPoolImpl;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.MultithreadEventLoopGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Without a NIC set, the s3 factory builds the 5.16 drivers (item 23): the library's pool, one
 * event-loop group of the configured size, direct dispatch as 5.16 has it, and a start that makes
 * no NIC group.
 */
@Timeout(60)
final class S3DefaultRouteTest {

	private static final int THREADS = 3;
	private static final RangeReadPolicy RANGE_POLICY = new RangeReadPolicy(3, 2L, 1);

	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final AtomicInteger puts = new AtomicInteger();
	private final AtomicInteger activePuts = new AtomicInteger();
	private final AtomicInteger peakPuts = new AtomicInteger();
	/** While set, each PUT waits for it before the stub answers. */
	private volatile CountDownLatch gate;
	private HttpServer server;

	@BeforeEach
	void listen() throws Exception {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				if ("PUT".equals(exchange.getRequestMethod())) {
					puts.incrementAndGet();
					peakPuts.accumulateAndGet(activePuts.incrementAndGet(), Math::max);
					try {
						final var held = gate;
						if (held != null) {
							held.await(5, TimeUnit.SECONDS);
						}
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					} finally {
						activePuts.decrementAndGet();
					}
					exchange.getResponseHeaders().set("ETag", "\"stub\"");
				}
				exchange.sendResponseHeaders(200, -1);
			}
		});
		server.setExecutor(serverThreads);
		server.start();
	}

	@AfterEach
	void stop() {
		final var held = gate;
		if (held != null) {
			held.countDown();
		}
		server.stop(0);
		serverThreads.shutdownNow();
	}

	@Test
	void anEmptyNicSetBuildsThe516DriverAndStartsIt() throws Exception {
		final var factory = new S3StorageDriverExtension<DataItem, Operation<DataItem>, S3StorageDriver<DataItem, Operation<DataItem>>>();
		try (final var driver = factory.create("default-route", new SeedDataInput(1, ITEM_SIZE, 1, true), storage(), false, 16)) {
			assertSame(S3StorageDriver.class, driver.getClass());
			assertFiveSixteenParts(driver, true);

			final var results = new EndpointSelectionTestSupport.Results();
			driver.operationResultOutput(results);
			driver.start();
			assertTrue(driver.put(dataOp(OpType.CREATE, "default-route")));
			assertEquals(Operation.Status.SUCC, results.await().status());

			assertFiveSixteenParts(driver, true);
			final var ioThreads = Thread.getAllStackTraces().keySet().stream()
							.filter(Thread::isAlive)
							.map(Thread::getName)
							.filter(name -> name.startsWith("ioWorker"))
							.toList();
			assertFalse(ioThreads.isEmpty());
			assertTrue(ioThreads.stream().noneMatch(name -> name.startsWith("ioWorker-")), () -> "a NIC group: " + ioThreads);
		}
	}

	@Test
	void theDefaultRouteKeepsItsPermitsAcrossDirectDispatch() throws Exception {
		final var factory = new S3StorageDriverExtension<DataItem, Operation<DataItem>, S3StorageDriver<DataItem, Operation<DataItem>>>();
		try (final var driver = factory.create("default-route", new SeedDataInput(1, ITEM_SIZE, 1, true), storage(), false, 16)) {
			final var results = new EndpointSelectionTestSupport.Results();
			driver.operationResultOutput(results);
			driver.start();
			gate = new CountDownLatch(1);
			for (var i = 0; i < 8; i++) {
				assertTrue(driver.put(dataOp(OpType.CREATE, "permits-" + i)));
			}

			NicSetLoopback.waitFor(() -> puts.get() == 2, "the first two PUTs reach the stub");
			Thread.sleep(300);
			assertEquals(2, puts.get(), "the two permits bound the requests in flight");
			gate.countDown();
			for (var i = 0; i < 8; i++) {
				assertEquals(Operation.Status.SUCC, results.await().status());
			}

			assertEquals(8, puts.get());
			assertEquals(2, peakPuts.get(), "a completion hands its permit on, never a second one");
			NicSetLoopback.waitFor(() -> driver.activeOpCount() == 0, "every permit came back");
			assertFiveSixteenParts(driver, true);
		}
	}

	@Test
	void anEmptyNicSetBuildsThe516RangeDriver() throws Exception {
		try (final var driver = new S3StorageDriverExtension<>().createRangeRead("default-route",
						new SeedDataInput(1, ITEM_SIZE, 1, true), storage(), 4, RANGE_POLICY)) {
			assertSame(S3RangeStorageDriver.class, driver.getClass());
			assertFiveSixteenParts(driver, false);
		}
	}

	private static void assertFiveSixteenParts(final Object driver, final boolean directDispatch) throws Exception {
		assertInstanceOf(MultiNodeConnPoolImpl.class, field(NettyStorageDriverBase.class, "connPool", driver));
		final var group = (MultithreadEventLoopGroup) field(NettyStorageDriverBase.class, "ioExecutor", driver);
		assertEquals(THREADS, group.executorCount(), "storage.driver.threads sizes the one group");
		assertEquals(List.of(), field(NettyStorageDriverBase.class, "extraIoGroups", driver));
		assertEquals(directDispatch, field(CoopStorageDriverBase.class, "directDispatchEnabled", driver));
	}

	private static Object field(final Class<?> owner, final String name, final Object target) throws ReflectiveOperationException {
		final var field = owner.getDeclaredField(name);
		field.setAccessible(true);
		return field.get(target);
	}

	private Config storage() {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		root.val("storage-net-interfaces", List.of());
		root.val("storage-net-node-addrs", List.of("127.0.0.1:" + server.getAddress().getPort()));
		root.val("storage-net-timeoutMilliSec", 2_000);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-driver-limit-concurrency", 2);
		root.val("storage-driver-threads", THREADS);
		return root.configVal("storage");
	}
}
