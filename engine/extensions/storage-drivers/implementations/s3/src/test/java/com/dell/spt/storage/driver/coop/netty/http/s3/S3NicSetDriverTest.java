package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.HOSTNAME;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.KERNEL_SOURCE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitRefused;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.port;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.rowsOf;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.stoppedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.storage;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.unusedPort;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.RangeRun;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Run;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real S3 drivers with a NIC set against loopback listeners that record each request's source
 * address ({@link NicSetLoopback}), on two of its loopback NICs.
 */
@Timeout(60)
final class S3NicSetDriverTest {

	private static final String NIC_A = NicSetLoopback.NICS.get(0);
	private static final String NIC_B = NicSetLoopback.NICS.get(1);
	private static final String ADDRESS_A = NicSetLoopback.ADDRESSES.get(0);
	private static final String ADDRESS_B = NicSetLoopback.ADDRESSES.get(1);
	private static final long OBJECT_SIZE = 10;
	/** Bytes 2-4 of every object. */
	private static final RangeReadPolicy FIXED = new RangeReadPolicy(3, 2L, 1);

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
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
		final var puts = stub.requests().stream().filter(request -> "PUT".equals(request.method())).toList();
		assertEquals(6, puts.size(), () -> "requests: " + stub.requests());
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
		final var helpers = stub.requests().stream().filter(request -> !"PUT".equals(request.method())).toList();
		assertFalse(helpers.isEmpty(), () -> "requests: " + stub.requests());
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

		stub.stop(1);
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
		// A refused connect fails on the Path chosen for it.
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(6L, 4L, 2L, 0L, 0L, 4L * ITEM_SIZE), totals(rows), rows::toString);
		final var refused = rowsOf(rows, NicPathRows.Kind.PATH).stream().filter(row -> NIC_B.equals(row.nic().iface()))
						.findFirst().orElseThrow();
		assertEquals(2, refused.failed(), refused::toString);
	}

	@Test
	void aNicTheHostLacksFailsTheStartAsAConfigurationError() throws Exception {
		final var driver = created(listeners(1), List.of(NIC_A, "lo-z"), 2);

		final var thrown = assertThrows(RuntimeException.class, driver::start);

		final var terminal = RunTerminalException.find(thrown);
		assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, terminal.category());
		assertTrue(terminal.getMessage().contains("lo-z"), terminal.getMessage());
		assertTrue(stub.requests().isEmpty());
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
		assertTrue(stub.requests().isEmpty());
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
		assertTrue(stub.requests().isEmpty(), "the probe sends no request");
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
		final var requests = stub.requests();
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
		final var driver = NicSetLoopback.driver(storage(endpoints, interfaces, concurrency), false);
		resources.add(driver);
		return driver;
	}

	private Run started(final List<String> endpoints, final List<String> interfaces, final int concurrency) throws Exception {
		return NicSetLoopback.started(created(endpoints, interfaces, concurrency));
	}

	private RangeRun rangeRun(final List<String> endpoints) throws Exception {
		stub.absentObjectSize = OBJECT_SIZE;
		final var run = new RangeRun(storage(endpoints, List.of(NIC_A, NIC_B), 1), FIXED, OBJECT_SIZE);
		resources.add(run);
		return run;
	}

	private List<String> listeners(final int count) throws IOException {
		return stub.listeners(count);
	}
}
