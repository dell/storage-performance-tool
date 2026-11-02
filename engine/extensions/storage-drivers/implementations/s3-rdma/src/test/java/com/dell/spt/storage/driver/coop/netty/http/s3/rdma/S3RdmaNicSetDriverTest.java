package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.ADDRESSES;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.BUCKET;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.DEVICES;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.KERNEL_SOURCE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.NICS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.closedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.laneThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.op;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.rdma;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Request;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Run;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The real S3-RDMA driver with a NIC set on {@link RdmaNicLoopback}'s loopback NICs: one context and
 * one NIC Pool per NIC, an attempt's NIC in its token and its Paths, and the concurrency rules.
 */
@Timeout(90)
final class S3RdmaNicSetDriverTest {

	private final RdmaNicLoopback stub = new RdmaNicLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		stub.release();
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
	}

	// ---------- the factory and the settings ----------

	@Test
	void theFactoryRoutesANicSetToTheNicSetDriverAndKeepsTheDefaultRouteOtherwise() throws Exception {
		final var factory = new S3RdmaStorageDriverExtension<DataItem, Operation<DataItem>, S3RdmaStorageDriver<DataItem, Operation<DataItem>>>();
		assertInstanceOf(NicSetDriverFactory.class, factory);
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), List.of("192.0.2.20:9000"), NICS.subList(0, 1), 2);
		// Construction opens nothing; the NICs and their contexts open at start.
		try (final var driver = factory.create("factory", RdmaNicLoopback.dataInput(), root.configVal("storage"), false, 16)) {
			assertInstanceOf(S3RdmaNicSetDriver.class, driver);
			assertNull(driver.bufferPool(), "the default route's resources stay unused");
		}
		final var plain = RdmaNicLoopback.root(rdma(true, true, 1), List.of("192.0.2.20:9000"), List.of(), 2);
		plain.val("storage-net-transport", "nio");
		try (final var driver = factory.create("factory", RdmaNicLoopback.dataInput(), plain.configVal("storage"), false, 16)) {
			assertEquals(S3RdmaStorageDriver.class, driver.getClass(), "no NIC set, the 5.16 driver");
			assertNotNull(driver.bufferPool());
		}
	}

	@Test
	void anExplicitRdmaDeviceOrLocalIpBesideANicSetIsAConfigurationError() throws Exception {
		final List<Extension> factories = List.of(new S3RdmaStorageDriverExtension<>());
		for (final var setting : List.of(List.of("rdma-device", "mlx5_0"), List.of("rdma-localIp", "192.0.2.7"))) {
			final var root = RdmaNicLoopback.root(rdma(false, true, 1), List.of("192.0.2.20:9000"), NICS.subList(0, 1), 2);
			final Config storage = root.configVal("storage");
			storage.val("driver-type", S3RdmaStorageDriverExtension.NAME);
			storage.val(setting.get(0), setting.get(1));

			final var failure = assertThrows(IllegalConfigurationException.class,
							() -> StorageDriver.<DataItem, Operation<DataItem>> instance(
											factories, storage, RdmaNicLoopback.dataInput(), false, 16, "step"));

			assertTrue(failure.getMessage().contains("storage.net.interfaces"), failure.getMessage());
		}
	}

	@Test
	void withoutANicSetAnExplicitDeviceKeepsItsMeaning() throws Exception {
		final var root = RdmaNicLoopback.root(new RdmaConfig(true, 1024, true, "mlx5_0", "", "WARN"), List.of("192.0.2.20:9000"), List.of(), 2);
		root.val("storage-net-transport", "nio");
		final var storage = root.configVal("storage");
		storage.val("driver-type", S3RdmaStorageDriverExtension.NAME);
		final List<Extension> factories = List.of(new S3RdmaStorageDriverExtension<>());
		try (final var driver = StorageDriver.<DataItem, Operation<DataItem>> instance(
						factories, storage, RdmaNicLoopback.dataInput(), false, 16, "step")) {
			assertEquals(S3RdmaStorageDriver.class, driver.getClass());
		}
	}

	// ---------- contexts ----------

	@Test
	void eachNicHasOneContextOpenedStrictlyAndOneAfterAnother() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);

		assertEquals(2, stub.transports.size(), "one context per NIC");
		for (var i = 0; i < 2; i++) {
			assertEquals(DEVICES.get(i), stub.transports.get(i).getStrictDevice());
			assertEquals(ADDRESSES.get(i), stub.transports.get(i).getStrictAddress());
		}
		assertFalse(stub.opensOverlapped, "contexts open one after another");
		assertEquals(NICS.subList(0, 2), run.driver().openRdmaResources().nics().stream().map(nic -> nic.name).toList());
		assertNotSame(run.nic(0).transport, run.nic(1).transport);
		assertNotSame(run.nic(0).pool, run.nic(1).pool);
		assertNotSame(run.nic(0).copier, run.nic(1).copier);
	}

	@Test
	void aFailureOpeningANicClosesThoseOpenedAndFailsTheStepWhateverTheFallback() throws Exception {
		stub.failOpen = DEVICES.get(1);
		final var root = RdmaNicLoopback.root(rdma(true, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		final var driver = stub.driver(root);
		resources.add(driver);

		final var thrown = assertThrows(RuntimeException.class, driver::start);

		final var terminal = RunTerminalException.find(thrown);
		assertNotNull(terminal, () -> "not a run terminal failure: " + thrown);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, terminal.category());
		assertTrue(terminal.getMessage().contains(NICS.get(1)), terminal.getMessage());
		assertTrue(terminal.getMessage().contains(DEVICES.get(1)), terminal.getMessage());
		assertTrue(terminal.getMessage().contains(ADDRESSES.get(1)), terminal.getMessage());
		assertEquals(2, stub.transports.size());
		stub.transports.forEach(transport -> assertTrue(transport.getCloseCount() >= 1, "every context is closed"));
		assertFalse(stub.transports.get(0).isAvailable());
		assertTrue(stub.objectRequests().isEmpty());
		assertTrue(laneThreads("rdma-nic-test").isEmpty());
	}

	@Test
	void aPlanWithoutAnRdmaDeviceFailsTheStepNamingTheNic() throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(1), List.of("lo-z"), 2);
		final var driver = stub.driver(root);
		resources.add(driver);

		final var terminal = RunTerminalException.find(assertThrows(RuntimeException.class, driver::start));

		assertNotNull(terminal);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, terminal.category());
		assertTrue(terminal.getMessage().contains("lo-z"), terminal.getMessage());
		assertTrue(stub.transports.isEmpty(), "no context opens without a plan");
	}

	@Test
	void withRdmaDisabledTheNicsCarryHttpAndNoContextOpens() throws Exception {
		final var config = new RdmaConfig(false, RdmaNicLoopback.THRESHOLD, false, "auto", "", "WARN");
		final var run = started(RdmaNicLoopback.root(config, stub.listeners(2), NICS.subList(0, 2), 2));

		assertTrue(stub.transports.isEmpty());
		assertNull(run.driver().openRdmaResources());
		for (var i = 0; i < 4; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "plain-" + i)).status());
		}
		assertEquals(Set.of(0, 1), puts().stream().map(Request::sourceNic).collect(Collectors.toSet()));
		puts().forEach(put -> assertNull(put.token()));
		assertEquals(4, run.driver().pathStats().httpRdmaUnavailable.sum());
		assertEquals(0, run.driver().driverStats().gauges().get(RdmaDriverStats.TRANSPORT_AVAILABLE));
	}

	// ---------- the NIC per attempt ----------

	@Test
	void tokensAndRequestsOfEachAttemptUseOneNic() throws Exception {
		final var listeners = stub.listeners(2);
		final var run = started(listeners, 2, 2);

		for (var i = 0; i < 6; i++) {
			final var name = "k" + i;
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, name)).status());
			assertArrayEquals(RdmaNicLoopback.expected(name, SIZE), stub.object(BUCKET + "/" + name));
		}

		final var puts = puts();
		assertEquals(6, puts.size(), () -> "requests: " + stub.requests());
		puts.forEach(put -> {
			assertNotNull(put.token(), put.toString());
			assertEquals(put.sourceNic(), put.tokenNic(), () -> "the token names the request's NIC: " + put);
			assertEquals(put.listener(), put.sourceNic(), () -> "two NICs and two endpoints pair one to one: " + put);
			assertEquals(listeners.get(put.listener()), put.host(), "the configured endpoint is the authority");
			assertEquals(0, put.bodyLength());
		});
		assertEquals(Set.of(0, 1), puts.stream().map(Request::sourceNic).collect(Collectors.toSet()));
		// The bucket check is a control request on the kernel route.
		final var control = stub.requests().stream().filter(request -> !request.isObjectRequest()).toList();
		assertFalse(control.isEmpty());
		control.forEach(request -> assertEquals(KERNEL_SOURCE, request.source(), request.toString()));
	}

	@Test
	void httpPayloadsUseTheirAttemptsNic() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(true, true, 1), stub.listeners(2), NICS.subList(0, 2), 2));

		// Below the threshold, a ranged read, and a declined GET whose body comes back over HTTP.
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "small", 512)).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "small2", 512)).status());
		stub.decline = request -> true;
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "declined")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "declined2")).status());

		final var objects = stub.objectRequests();
		assertEquals(4, objects.size());
		objects.forEach(request -> assertEquals(request.listener(), request.sourceNic(), () -> "on a Path of its NIC: " + request));
		assertEquals(Set.of(0, 1), objects.stream().map(Request::sourceNic).collect(Collectors.toSet()));
		objects.stream().filter(request -> "PUT".equals(request.method()))
						.forEach(request -> assertNull(request.token(), "below the threshold, no token"));
		objects.stream().filter(request -> "GET".equals(request.method()))
						.forEach(request -> assertEquals(request.sourceNic(), request.tokenNic(), request.toString()));
		assertEquals(2, run.driver().pathStats().httpBelowThreshold.sum());
		assertEquals(2, run.driver().pathStats().rdmaDeclinedServedOverHttp.sum());
	}

	@Test
	void aPreparationThatFallsBackSendsItsPayloadOverHttpOnItsNic() throws Exception {
		final var root = RdmaNicLoopback.root(rdma(true, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		final var run = started(root);
		final var failing = stub.transports.get(0);
		failing.setFailTokenGeneration(true);
		final var name = "fallback";

		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, name)).status());

		final var put = puts().get(0);
		assertNull(put.token());
		assertEquals(0, put.sourceNic(), "the attempt's NIC");
		assertEquals(SIZE, put.bodyLength());
		assertArrayEquals(RdmaNicLoopback.expected(name, SIZE), stub.object(BUCKET + "/" + name), "the rewound item sends its whole payload");
		assertEquals(1, run.nic(0).stats.httpFallback.sum());
		assertEquals(1, run.nic(0).stats.tokenGenerationFailed.sum());
		assertEquals(0, run.nic(1).stats.httpFallback.sum());
		assertEquals(1, run.nic(0).pool.idleBuffers(), "nothing was sent, so its buffer is reusable");
	}

	@Test
	void anAttemptKeepsItsNicThroughTheReaperAndItsBufferIsNotReused() throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1, 1_000), stub.listeners(2), NICS.subList(0, 2), 2);
		final var run = started(root);
		stub.hold();

		assertTrue(run.driver().put(op(OpType.CREATE, "timed-out")));
		waitFor(() -> stub.objectRequests().size() == 1, "the request reaches the stub");
		final int nic = stub.objectRequests().get(0).sourceNic();
		final var result = run.results().await();

		assertEquals(Operation.Status.FAIL_IO, result.status());
		assertEquals(1, run.nic(nic).stats.rdmaTimedOut.sum());
		assertEquals(0, run.nic(1 - nic).stats.rdmaTimedOut.sum());
		assertEquals(1, run.nic(nic).pool.discarded.sum(), "the server may still access it, so it goes");
		assertEquals(0, run.nic(nic).pool.liveBuffers());
		run.awaitIdle();
		stub.release();
		Thread.sleep(200);
		assertEquals(0, run.nic(nic).pool.idleBuffers(), "a late completion does not reuse the buffer");
		final var rows = closedRows(run.driver());
		final var failed = rows.rows().stream().filter(row -> row.failed() > 0).toList();
		assertEquals(1, failed.size(), rows::toString);
		assertEquals(NICS.get(nic), failed.get(0).nic().iface(), "the row of the attempt's NIC");
		assertEquals(NicPathRows.Kind.PATH, failed.get(0).kind());
		assertEquals(1, stub.transports.get(nic).getCloseCount());
	}

	@Test
	void aRetriedPartChoosesItsNicAgain() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		// One part at a time, so the rotation alone moves it: the initiation is object request 1, the
		// part's first attempt is request 2, and its retry takes the NIC after that attempt's.
		stub.failObjectRequest = n -> n == 2;

		assertTrue(run.driver().put(upload("retried", 2048, 1)));
		awaitUpload(run);

		final var parts = stub.objectRequests().stream().filter(request -> request.query() != null && request.query().contains("partNumber=")).toList();
		assertEquals(2, parts.size(), () -> "requests: " + stub.objectRequests());
		assertFalse(parts.get(0).sourceNic() == parts.get(1).sourceNic(), () -> "a retry chooses again: " + parts);
		parts.forEach(part -> assertEquals(part.sourceNic(), part.tokenNic(), part.toString()));
	}

	@Test
	void aPartWhoseConnectFailsIsSettledAndRetried() throws Exception {
		final var listeners = stub.listeners(2);
		final var run = started(listeners, 2, 2);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "before-0")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "before-1")).status());
		// One Path's connections go; the next acquisition on it fails its connect.
		stub.stop(1);
		waitFor(() -> run.pool().state().get(1).open() == 0, "the closed idle connection is dropped");

		final var upload = upload("failed-connect", 2048, 1);
		assertTrue(run.driver().put(upload));

		// The upload ends either way: its one part fails on the stopped Path or succeeds on the other,
		// and the parent's count of parts stays balanced, so it is never left waiting.
		final var results = new ArrayList<Operation<DataItem>>();
		waitFor(() -> {
			try {
				final var result = run.results().poll(50);
				if (result != null) {
					results.add(result);
				}
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return results.stream().anyMatch(result -> result instanceof com.dell.spt.base.item.op.composite.CompositeOperation<?> composite
							&& (composite.allSubOperationsDone() || composite.status() != Operation.Status.SUCC));
		}, "the upload reaches its end");
		run.awaitIdle();
	}

	// ---------- NIC pools ----------

	@Test
	void eachNicDrawsOnlyFromItsOwnPoolWhichRegistersOnlyOnItsContext() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);

		for (var i = 0; i < 4; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "pool-" + i)).status());
		}

		for (var i = 0; i < 2; i++) {
			final var nic = run.nic(i);
			assertEquals(1, nic.pool.created.sum(), nic.pool::summary);
			assertEquals(1, nic.pool.hits.sum(), "each NIC reuses its own buffer: " + nic.pool.summary());
			assertEquals(1, stub.transports.get(i).getActiveRegistrationCount(), "registered only on its NIC");
			assertEquals(1, nic.pool.idleBuffers());
		}
		assertEquals(2, run.driver().pathStats().rdmaTransferred.sum() / 2, "the driver's counts sum the NICs");
		final var stats = run.driver().driverStats();
		assertEquals(4, stats.counters().get(RdmaDriverStats.RDMA_TRANSFERRED));
		assertEquals(2, stats.counters().get(RdmaDriverStats.REGISTRATIONS_POOLED));
		assertEquals(2, stats.counters().get(RdmaDriverStats.POOL_HITS));
		assertEquals(2, stats.gauges().get(RdmaDriverStats.POOL_BUFFERS));
		assertEquals(1, stats.gauges().get(RdmaDriverStats.TRANSPORT_AVAILABLE));
	}

	@Test
	void theBudgetIsSplitEquallyAndTheClassCapFollowsTheMode() throws Exception {
		final var budget = RdmaBufferPool.defaultMaxPooledBytes();
		// P >= N: the share and one more.
		assertEquals(new RdmaNicPoolLimits(budget / 4, 24, 25), RdmaNicPoolLimits.of(96, 4, 0, budget, 256));
		assertEquals(1, RdmaNicPoolLimits.of(96, 4, 0, budget, 256).headroom());
		// The remainder goes to the first NICs.
		assertEquals(3, RdmaNicPoolLimits.of(7, 3, 0, budget, 256).minimum());
		assertEquals(2, RdmaNicPoolLimits.of(7, 3, 2, budget, 256).minimum());
		// 0 < P < N: P itself and one more.
		assertEquals(new RdmaNicPoolLimits(budget / 4, 3, 4), RdmaNicPoolLimits.of(3, 4, 3, budget, 256));
		// Unlimited: today's constant, with no minimum.
		assertEquals(new RdmaNicPoolLimits(budget / 4, 0, 256), RdmaNicPoolLimits.of(0, 4, 0, budget, 256));

		for (final var concurrency : List.of(5, 1, 0)) {
			final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), concurrency);
			final var run = started(root);
			final var limits = IntStream.range(0, 2)
							.mapToObj(i -> RdmaNicPoolLimits.of(concurrency, 2, i, budget, S3RdmaStorageDriver.UNLIMITED_CONCURRENCY_POOL_LIMIT))
							.toList();
			for (var i = 0; i < 2; i++) {
				assertEquals(limits.get(i).cap(), run.nic(i).pool.maxBuffersPerClass());
				assertEquals(budget / 2, run.nic(i).pool.levels().budgetBytes());
			}
			run.driver().close();
		}
	}

	@Test
	void twoDriversInOneJvmKeepTheirContextsAndPoolsApart() throws Exception {
		final var listeners = stub.listeners(1);
		final var first = started(RdmaNicLoopback.root(rdma(false, true, 1), listeners, NICS.subList(0, 2), 2));
		final var firstTransports = List.copyOf(stub.transports);
		final var second = started(RdmaNicLoopback.root(rdma(false, true, 1), listeners, NICS.subList(2, 4), 2));

		assertEquals(Operation.Status.SUCC, first.executeAlone(op(OpType.CREATE, "one")).status());
		assertEquals(Operation.Status.SUCC, second.executeAlone(op(OpType.CREATE, "two")).status());

		assertEquals(4, stub.transports.size());
		assertNotSame(first.nic(0).pool, second.nic(0).pool);
		assertEquals(1, first.nic(0).pool.created.sum());
		assertEquals(1, second.nic(0).pool.created.sum());
		firstTransports.forEach(transport -> assertTrue(Set.of(DEVICES.get(0), DEVICES.get(1)).contains(transport.getStrictDevice())));
		first.driver().close();
		assertEquals(Operation.Status.SUCC, second.executeAlone(op(OpType.CREATE, "after")).status(), "the other driver goes on");
	}

	@Test
	void aRegistrationFailureOnOneNicRollsBackItsReservationAndLeavesTheOtherWorking() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		final var failing = stub.transports.get(1);
		failing.setFailNextRegistrations(2);

		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "on-first")).status());
		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.CREATE, "on-second")).status());

		final var second = run.nic(1);
		assertEquals(1, second.pool.registrationFailed.sum());
		assertEquals(0, second.pool.liveBuffers(), "the failed registration gave back its reservation");
		assertEquals(0, second.pool.liveBytes());
		assertEquals(1, second.stats.prepareFailed.sum());
		assertEquals(1, second.stats.registrationFailedOnDemand.sum());
		assertEquals(0, run.nic(0).stats.prepareFailed.sum());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "first-again")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "second-again")).status());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(4L, 3L, 1L, 0L, 0L, 3L * SIZE), totals(rows), rows::toString);
		final var failedRow = rows.rows().stream().filter(row -> row.failed() > 0).findFirst().orElseThrow();
		assertEquals(NicPathRows.Kind.NIC, failedRow.kind(), "a preparation failure counts on the NIC's row");
		assertEquals(NICS.get(1), failedRow.nic().iface());
	}

	@Test
	void anEvictionsMemoryAndAnUnpooledRequestOnlyTouchItsOwnNicsPool() throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		final var run = started(root);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "fill-0")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "fill-1")).status());
		assertEquals(1, run.nic(0).pool.idleBuffers());
		assertEquals(1, run.nic(1).pool.idleBuffers());

		// A per-operation allocation that finds direct memory exhausted reclaims from its NIC only.
		run.nic(0).pool.allocator = size -> {
			throw new OutOfMemoryError("Direct buffer memory");
		};
		run.driver().directAllocator = new java.util.function.IntFunction<>() {
			private boolean failed;

			@Override
			public java.nio.ByteBuffer apply(final int size) {
				if (!failed) {
					failed = true;
					throw new OutOfMemoryError("Direct buffer memory");
				}
				return java.nio.ByteBuffer.allocateDirect(size);
			}
		};
		// A larger class, so the pool cannot serve it from its idle buffer.
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "unpooled", 2L << 20)).status());

		assertEquals(0, run.nic(0).pool.idleBuffers(), "reclaimed from the attempt's NIC");
		assertEquals(1, run.nic(1).pool.idleBuffers(), "the other NIC's pool is left alone");
		assertEquals(1, run.nic(0).stats.registrationsOnDemand.sum());
	}

	@Test
	void closingOneNicsContextLeavesTheOthersWorking() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "both-0")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "both-1")).status());

		run.nic(0).close();
		assertTrue(run.nic(0).closed());
		assertFalse(stub.transports.get(0).isAvailable());
		assertTrue(stub.transports.get(1).isAvailable());
		assertTrue(run.nic(1).pool.levels().buffers() > 0, "the other pool keeps its buffers");

		// The rotation still gives the first NIC its turn: its attempt fails, the other's succeeds.
		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.CREATE, "closed")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "open")).status());
	}

	@Test
	void repeatedStartAndStopLeaksNothing() throws Exception {
		final var listeners = stub.listeners(2);
		for (var i = 0; i < 5; i++) {
			final var run = started(RdmaNicLoopback.root(rdma(false, true, 2), listeners, NICS.subList(0, 2), 2));
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "cycle-" + i)).status());
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "cycle-" + i)).status());
			run.driver().close();
			final var open = run.driver().openRdmaResources();
			open.nics().forEach(nic -> {
				assertTrue(nic.closed());
				assertEquals(0, nic.pool.liveBuffers(), nic.pool::summary);
				assertTrue(nic.copier.isTerminated());
			});
			open.lanes().forEach(lane -> assertTrue(lane.exited()));
		}
		waitFor(() -> laneThreads("rdma-nic-test").isEmpty(), "no lane thread is left");
		assertEquals(10, stub.transports.size());
		stub.transports.forEach(transport -> {
			assertEquals(1, transport.getCloseCount(), "each context closes once");
			assertFalse(transport.wasClosedDuringACall());
			assertEquals(0, transport.getActiveRegistrationCount());
		});
		assertFalse(Thread.getAllStackTraces().keySet().stream().anyMatch(thread -> thread.isAlive() && thread.getName().contains("-rdma-copy-")),
						"no copy thread is left");
	}

	// ---------- concurrency ----------

	@Test
	void withSharesEachNicCarriesItsShareAndNoMore() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);
		final var admission = run.pool().admission();
		stub.hold();

		for (var i = 0; i < 16; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "share-" + i)));
		}
		waitFor(() -> stub.objectRequests().size() == 4, "the permits bound the attempts in flight");
		Thread.sleep(200);
		assertEquals(4, stub.objectRequests().size());
		assertEquals(List.of(2, 2), counts(run));
		assertEquals(0, admission.room());

		stub.release();
		for (var i = 0; i < 16; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(2, stub.peakConcurrency(ADDRESSES.get(0)));
		assertEquals(2, stub.peakConcurrency(ADDRESSES.get(1)));
		run.awaitIdle();
	}

	@Test
	void oneThreadWithFourNicsUsesEveryNicOverRdmaAndHttp() throws Exception {
		final var run = started(stub.listeners(4), 4, 1);

		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "rdma-" + i)).status());
		}
		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "http-" + i, 512)).status());
		}

		final var puts = puts();
		assertEquals(Set.of(0, 1, 2, 3), puts.stream().filter(put -> put.token() != null).map(Request::sourceNic).collect(Collectors.toSet()));
		assertEquals(Set.of(0, 1, 2, 3), puts.stream().filter(put -> put.token() == null).map(Request::sourceNic).collect(Collectors.toSet()));
		for (var i = 0; i < 4; i++) {
			assertEquals(1, stub.peakConcurrency(ADDRESSES.get(i)));
			assertEquals(1, run.pool().admission().limit(i), "the shared mode's bound");
		}
	}

	@Test
	void withoutALimitPreOpeningIsBoundedAndRequestsStayUnlimited() throws Exception {
		final var run = started(stub.listeners(2), 2, 0);
		final var admission = run.pool().admission();
		assertEquals(NicSetSettings.UNLIMITED_WAITING_LIMIT, admission.limit(0));
		run.pool().state().forEach(nic -> assertTrue(nic.open() <= nic.paths().size(), () -> "one connection per Path is pre-opened: " + nic));
		stub.hold();

		for (var i = 0; i < 30; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "unlimited-" + i)));
		}

		waitFor(() -> stub.objectRequests().size() == 30, "every attempt is in flight at once");
		assertEquals(List.of(0, 0), counts(run), "an attempt in flight no longer counts against its NIC");
		stub.release();
		for (var i = 0; i < 30; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		run.awaitIdle();
	}

	@Test
	void aSlowPathHoldsItsShareAndDoesNotSlowTheOtherNic() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);
		final int slow = 0;
		stub.hold(request -> request.sourceNic() == slow);

		for (var i = 0; i < 20; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "slow-" + i)));
		}

		// The fast NIC finishes everything but what waits behind the slow NIC's share.
		for (var i = 0; i < 18; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(2, stub.objectRequests().stream().filter(request -> request.sourceNic() == slow).count(),
						"the slow NIC holds its share and takes no more");
		assertEquals(2, run.pool().admission().count(slow));
		stub.release();
		for (var i = 0; i < 2; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		run.awaitIdle();
	}

	@Test
	void thePermitReturnsToItsNicOnceAfterEveryEnding() throws Exception {
		final var listeners = stub.listeners(2);
		final var run = started(listeners, 2, 2);

		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "completed-0")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "completed-1")).status());
		assertEquals(List.of(0, 0), counts(run));

		stub.transports.forEach(transport -> transport.setFailTokenGeneration(true));
		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.READ, "failed-preparation-0")).status());
		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.READ, "failed-preparation-1")).status());
		assertEquals(List.of(0, 0), counts(run));
		stub.transports.forEach(transport -> transport.setFailTokenGeneration(false));

		stub.failObjectRequest = n -> true;
		assertEquals(Operation.Status.RESP_FAIL_SVC, run.executeAlone(op(OpType.READ, "error-0")).status());
		assertEquals(Operation.Status.RESP_FAIL_SVC, run.executeAlone(op(OpType.READ, "error-1")).status());
		stub.failObjectRequest = n -> false;
		assertEquals(List.of(0, 0), counts(run));
		assertEquals(0, run.driver().activeOpCount());

		// A refused connect, on the Path of a stopped listener.
		stub.stop(1);
		waitFor(() -> run.pool().state().get(1).open() == 0, "the closed idle connection is dropped");
		final List<Operation.Status> statuses = new ArrayList<>();
		for (var i = 0; i < 2; i++) {
			statuses.add(run.executeAlone(op(OpType.READ, "connect-" + i)).status());
		}
		assertEquals(List.of(Operation.Status.SUCC, Operation.Status.FAIL_IO), statuses.stream().sorted().toList());
		assertEquals(List.of(0, 0), counts(run));
		assertEquals(0, run.driver().activeOpCount());
		assertEquals(0, run.pool().pendingCount());
	}

	private List<Integer> counts(final Run run) {
		final var admission = run.pool().admission();
		return IntStream.range(0, admission.size()).map(admission::count).boxed().toList();
	}

	private List<Request> puts() {
		return stub.objectRequests().stream().filter(request -> "PUT".equals(request.method())).toList();
	}

	private Run started(final List<String> endpoints, final int nics, final int concurrency) throws Exception {
		return started(RdmaNicLoopback.root(rdma(false, true, 1), endpoints, NICS.subList(0, nics), concurrency));
	}

	private Run started(final Config root) throws Exception {
		final var driver = stub.driver(root);
		resources.add(driver);
		return RdmaNicLoopback.started(driver);
	}

	@SuppressWarnings("unchecked")
	static Operation<DataItem> upload(final String name, final int partSize, final int parts) {
		return (Operation<DataItem>) (Operation<?>) new CompositeDataOperationImpl<>(0, OpType.CREATE,
						new DataItemImpl(name, 0, (long) partSize * parts), null, BUCKET, RdmaNicLoopback.CREDENTIAL, null, 0, partSize);
	}

	private static void awaitUpload(final Run run) throws Exception {
		while (true) {
			final var result = run.results().await();
			if (result instanceof com.dell.spt.base.item.op.composite.CompositeOperation<?> composite && composite.allSubOperationsDone()) {
				assertEquals(Operation.Status.SUCC, result.status());
				return;
			}
		}
	}

}
