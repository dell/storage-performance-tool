package com.dell.spt.storage.driver.coop.netty;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class NettyStorageDriverBaseTest {
	private static final String TEST_SEED = "7a42d9c483244167";

	private static class TestNettyDriver extends NettyStorageDriverBase<Item, Operation<Item>> {
		private static final ThreadLocal<NonBlockingConnPool> CONSTRUCTION_POOL = new ThreadLocal<>();
		private final AtomicInteger sentRequestCount = new AtomicInteger();
		private final AtomicReference<Operation<Item>> lastSentOperation = new AtomicReference<>();
		private volatile boolean publishCompletion;

		protected TestNettyDriver(
						final DataInput dataInput, final Config storageConfig) throws Exception {
			super("netty-admission-step", dataInput, storageConfig, false, 3);
		}

		private static TestNettyDriver create(
						final NonBlockingConnPool connPool, final int concurrencyLimit) throws Exception {
			TestNettyDriver.CONSTRUCTION_POOL.set(connPool);
			try {
				return new TestNettyDriver(
								DataInput.instance(null, TEST_SEED, new SizeInBytes("64KB"), 4, false, 0.0, true),
								storageConfig(concurrencyLimit));
			} finally {
				TestNettyDriver.CONSTRUCTION_POOL.remove();
			}
		}

		@Override
		protected NonBlockingConnPool createConnectionPool() {
			return CONSTRUCTION_POOL.get();
		}

		@Override
		protected void sendRequest(final Channel channel, final Operation<Item> op) {
			sentRequestCount.incrementAndGet();
			lastSentOperation.set(op);
		}

		@Override
		public void complete(final Channel channel, final Operation<Item> op) {
			if (publishCompletion) {
				super.complete(channel, op);
			}
		}

		@Override
		protected String requestNewPath(final String path) {
			return path;
		}

		@Override
		protected String requestNewAuthToken(final Credential credential) {
			return "token";
		}

		@Override
		public List<Item> list(
						final ItemFactory<Item> itemFactory, final String path, final String prefix,
						final int idRadix, final Item lastPrevItem, final int count) throws IOException {
			return List.of();
		}

		private void publishCompletion() {
			publishCompletion = true;
		}
	}

	private static final class LegacySubmitDriver extends TestNettyDriver {
		private final CountDownLatch submitCalled = new CountDownLatch(1);
		private final AtomicReference<Operation<Item>> submittedOperation = new AtomicReference<>();

		private LegacySubmitDriver(
						final DataInput dataInput, final Config storageConfig) throws Exception {
			super(dataInput, storageConfig);
		}

		private static LegacySubmitDriver create(
						final NonBlockingConnPool connPool) throws Exception {
			TestNettyDriver.CONSTRUCTION_POOL.set(connPool);
			try {
				return new LegacySubmitDriver(
								DataInput.instance(
												null, TEST_SEED, new SizeInBytes("64KB"), 4, false, 0.0, true),
								storageConfig(1));
			} finally {
				TestNettyDriver.CONSTRUCTION_POOL.remove();
			}
		}

		@Override
		protected boolean submit(final Operation<Item> op) {
			submittedOperation.set(op);
			submitCalled.countDown();
			return true;
		}
	}

	private TestNettyDriver driver;
	private NonBlockingConnPool connPool;
	private OperationLifecycleTracker<Operation<Item>> lifecycle;

	@BeforeEach
	void setUp() throws Exception {
		connPool = mock(NonBlockingConnPool.class);
		driver = TestNettyDriver.create(connPool, 1);
		lifecycle = driver.operationLifecycle();
		driver.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		driver.close();
	}

	private void replaceDriver(final int concurrencyLimit) throws Exception {
		driver.close();
		driver = TestNettyDriver.create(connPool, concurrencyLimit);
		lifecycle = driver.operationLifecycle();
		driver.start();
	}

	@Test
	void admissionClosureBeforeSubmissionRefusesAndRecoversTheExactOperation() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.READ, new DataItemImpl("closed-before-submit", 0, 1), null, "/bucket", null);
		assertTrue(lifecycle.driverQueued(op));

		driver.closeAdmission();

		assertFalse(driver.submit(op));
		final var recovered = driver.recoverQueuedOperations();
		assertEquals(1, recovered.size());
		assertTrue(recovered.get(0) == op);
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op));
		assertEquals(0, lifecycle.snapshot().dispatched());
		assertEquals(1, lifecycle.snapshot().unattempted());
	}

	@Test
	void legacySubmitOverrideRetainsSuccessfulReturnDispatchFallback() throws Exception {
		try (final var legacyDriver = LegacySubmitDriver.create(connPool)) {
			legacyDriver.start();
			final var legacyLifecycle = legacyDriver.operationLifecycle();
			final Operation<Item> op = new OperationImpl<>(
							0, OpType.READ, new DataItemImpl("legacy-netty-submit", 0, 1), null, "/bucket", null);

			assertTrue(legacyDriver.put(op));
			assertTrue(legacyDriver.submitCalled.await(2, TimeUnit.SECONDS));
			final var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			while ((legacyLifecycle.stateOf(op) != OperationLifecycleState.DISPATCHED
							|| legacyLifecycle.inFlightCount() != 1
							|| legacyDriver.scheduledOpCount() != 1)
							&& System.nanoTime() < deadlineNanos) {
				Thread.sleep(10);
			}

			assertTrue(legacyDriver.submittedOperation.get() == op);
			assertEquals(OperationLifecycleState.DISPATCHED, legacyLifecycle.stateOf(op));
			assertEquals(1, legacyLifecycle.inFlightCount());
			assertEquals(1, legacyDriver.scheduledOpCount());
		}
	}

	@Test
	void submit_retriesInactiveConnectionsFromPool() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.READ, new DataItemImpl("netty-dispatch", 0, 1), null, "/bucket", null);
		lifecycle.driverQueued(op);

		// 1. The first connection the pool yields is INACTIVE (simulating a silent drop)
		final Channel deadConn = mock(Channel.class);
		when(deadConn.isActive()).thenReturn(false);

		// 2. The second connection the pool yields is ACTIVE
		final Channel liveConn = mock(Channel.class);
		when(liveConn.isActive()).thenReturn(true);

		final Attribute<Boolean> releasedAttr = mock(Attribute.class);
		when(liveConn.attr(ATTR_KEY_RELEASED)).thenReturn(releasedAttr);

		final Attribute opAttr = mock(Attribute.class);
		when(liveConn.attr(NettyStorageDriverBase.ATTR_KEY_OPERATION)).thenReturn(opAttr);

		final Attribute<String> nodeAttr = mock(Attribute.class);
		when(nodeAttr.get()).thenReturn("node1");
		when(liveConn.attr(ATTR_KEY_NODE)).thenReturn(nodeAttr);

		// Configure pool to return dead first, then live
		when(connPool.lease()).thenReturn(deadConn, liveConn);

		// Act: submit the operation
		driver.submit(op);

		// Assert: The driver should have requested a lease twice
		verify(connPool, times(2)).lease();

		// Assert: The dead connection should have been closed and released back to the pool
		verify(deadConn).close();
		verify(connPool).release(deadConn);

		// Assert: The live connection was actually used to send the request
		verify(liveConn.attr(ATTR_KEY_RELEASED)).set(Boolean.FALSE);
		assertEquals(1, driver.sentRequestCount.get());
		assertTrue(driver.lastSentOperation.get() == op);
		assertEquals(OperationLifecycleState.DISPATCHED, op.lifecycle().state());
		assertEquals(1, lifecycle.inFlightCount());
	}

	@Test
	void connectionAcquisitionIsPartOfTheDispatchedAttempt() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.READ, new DataItemImpl("waiting-for-connection", 0, 1), null, "/bucket", null);
		assertTrue(lifecycle.driverQueued(op));
		final var leaseEntered = new CountDownLatch(1);
		final var releaseLease = new CountDownLatch(1);
		final Channel liveConn = mock(Channel.class);
		when(liveConn.isActive()).thenReturn(true);
		final Attribute<Boolean> releasedAttr = mock(Attribute.class);
		when(liveConn.attr(ATTR_KEY_RELEASED)).thenReturn(releasedAttr);
		final Attribute opAttr = mock(Attribute.class);
		when(liveConn.attr(NettyStorageDriverBase.ATTR_KEY_OPERATION)).thenReturn(opAttr);
		final Attribute<String> nodeAttr = mock(Attribute.class);
		when(nodeAttr.get()).thenReturn("node1");
		when(liveConn.attr(ATTR_KEY_NODE)).thenReturn(nodeAttr);
		when(connPool.lease()).thenAnswer(invocation -> {
			leaseEntered.countDown();
			assertTrue(releaseLease.await(2, TimeUnit.SECONDS));
			return liveConn;
		});

		final var submitting = Thread.ofVirtual().start(() -> driver.submit(op));
		assertTrue(leaseEntered.await(2, TimeUnit.SECONDS));
		assertEquals(OperationLifecycleState.DISPATCHED, op.lifecycle().state(),
						"the bounded operation attempt includes connection acquisition");
		assertEquals(1, lifecycle.inFlightCount());

		releaseLease.countDown();
		submitting.join();
		assertEquals(OperationLifecycleState.DISPATCHED, op.lifecycle().state());
		assertEquals(1, driver.sentRequestCount.get());
		assertTrue(driver.lastSentOperation.get() == op);
	}

	@Test
	void admissionClosureDuringRangeRecoversEveryMemberNotYetDispatched() throws Exception {
		replaceDriver(3);
		final List<Operation<Item>> ops = List.of(
						new OperationImpl<>(
										0, OpType.READ, new DataItemImpl("batch-lease-first", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.READ, new DataItemImpl("batch-lease-second", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.READ, new DataItemImpl("batch-lease-third", 0, 1), null, "/bucket", null));
		for (final var op : ops) {
			assertTrue(lifecycle.driverQueued(op));
		}
		final var leaseEntered = new CountDownLatch(1);
		final var releaseLease = new CountDownLatch(1);
		final Channel liveConn = mock(Channel.class);
		when(liveConn.isActive()).thenReturn(true);
		final Attribute<Boolean> releasedAttr = mock(Attribute.class);
		when(liveConn.attr(ATTR_KEY_RELEASED)).thenReturn(releasedAttr);
		final Attribute opAttr = mock(Attribute.class);
		when(liveConn.attr(NettyStorageDriverBase.ATTR_KEY_OPERATION)).thenReturn(opAttr);
		final Attribute<String> nodeAttr = mock(Attribute.class);
		when(nodeAttr.get()).thenReturn("node1");
		when(liveConn.attr(ATTR_KEY_NODE)).thenReturn(nodeAttr);
		when(connPool.lease()).thenAnswer(invocation -> {
			leaseEntered.countDown();
			assertTrue(releaseLease.await(2, TimeUnit.SECONDS));
			return liveConn;
		});

		final var acceptedCount = new AtomicInteger(-1);
		final var submitting = Thread.ofVirtual().start(
						() -> acceptedCount.set(driver.submit(ops, 0, ops.size())));
		assertTrue(leaseEntered.await(2, TimeUnit.SECONDS));
		assertEquals(OperationLifecycleState.DISPATCHED, ops.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, ops.get(1).lifecycle().state());
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, ops.get(2).lifecycle().state());
		assertEquals(1, lifecycle.inFlightCount());

		driver.closeAdmission();
		releaseLease.countDown();
		submitting.join();
		final var recovered = driver.recoverQueuedOperations();

		assertEquals(1, acceptedCount.get());
		assertEquals(2, recovered.size());
		assertTrue(recovered.stream().anyMatch(op -> op == ops.get(1)));
		assertTrue(recovered.stream().anyMatch(op -> op == ops.get(2)));
		assertEquals(OperationLifecycleState.DISPATCHED, ops.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, ops.get(1).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, ops.get(2).lifecycle().state());
		assertEquals(1, lifecycle.snapshot().dispatched());
		assertEquals(2, lifecycle.snapshot().unattempted());
	}

	@Test
	void noopPublicationFailureReleasesSinglePermitExactlyOnce() {
		final var writes = new AtomicInteger();
		driver.operationResultOutput(failFirstOutput(writes));
		driver.publishCompletion();
		final Operation<Item> failedOutput = new OperationImpl<>(
						0, OpType.NOOP, new DataItemImpl("noop-output-failure", 0, 1), null, "/bucket", null);
		assertTrue(lifecycle.driverQueued(failedOutput));

		driver.submit(failedOutput);

		assertEquals(1, writes.get());
		assertEquals(0, driver.activeOpCount());
	}

	@Test
	void noopBatchPublicationFailurePreservesPermitBoundAndTrailingProgress() throws Exception {
		replaceDriver(2);
		final var writes = new AtomicInteger();
		driver.operationResultOutput(failFirstOutput(writes));
		driver.publishCompletion();
		final List<Operation<Item>> ops = List.of(
						new OperationImpl<>(
										0, OpType.NOOP, new DataItemImpl("batch-noop-failure", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.NOOP, new DataItemImpl("batch-noop-trailing", 0, 1), null, "/bucket", null));
		for (final var op : ops) {
			assertTrue(lifecycle.driverQueued(op));
		}

		assertEquals(2, driver.submit(ops, 0, ops.size()));

		assertEquals(2, writes.get());
		assertEquals(0, driver.activeOpCount());
		assertEquals(OperationLifecycleState.TERMINAL, ops.get(1).lifecycle().state());
	}

	private static Config storageConfig(final int concurrencyLimit) {
		final var storageConfig = mock(Config.class);
		final var driverConfig = mock(Config.class);
		final var limitConfig = mock(Config.class);
		final var authConfig = mock(Config.class);
		final var integrityConfig = mock(Config.class);
		final var integrityInputConfig = mock(Config.class);
		final var netConfig = mock(Config.class);
		final var sslConfig = mock(Config.class);
		final var nodeConfig = mock(Config.class);

		when(storageConfig.configVal("driver")).thenReturn(driverConfig);
		when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		when(limitConfig.intVal("concurrency")).thenReturn(concurrencyLimit);
		when(driverConfig.intVal("threads")).thenReturn(1);
		when(storageConfig.stringVal("namespace")).thenReturn("test-ns");
		when(storageConfig.stringVal("driver-type")).thenReturn("netty-test");
		when(storageConfig.configVal("auth")).thenReturn(authConfig);
		when(authConfig.stringVal("uid")).thenReturn("user");
		when(authConfig.stringVal("secret")).thenReturn("secret");
		when(authConfig.stringVal("token")).thenReturn(null);
		when(storageConfig.configVal("integrity")).thenReturn(integrityConfig);
		when(integrityConfig.stringVal("mode")).thenReturn("none");
		when(integrityConfig.stringVal("algorithm")).thenReturn("sha256");
		when(integrityConfig.configVal("input")).thenReturn(integrityInputConfig);
		when(integrityInputConfig.stringVal("provenance")).thenReturn("none");
		when(integrityInputConfig.stringVal("expectedProducerId")).thenReturn("");
		when(storageConfig.intVal("driver-limit-queue-input")).thenReturn(16);
		when(storageConfig.intVal("driver-limit-multipart-objects")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-multipart-parts")).thenReturn(0);
		when(storageConfig.intVal("driver-threads")).thenReturn(1);

		when(storageConfig.configVal("net")).thenReturn(netConfig);
		when(netConfig.configVal("ssl")).thenReturn(sslConfig);
		when(sslConfig.boolVal("enabled")).thenReturn(false);
		when(netConfig.intVal("timeoutMilliSec")).thenReturn(1000);
		when(netConfig.configVal("node")).thenReturn(nodeConfig);
		when(nodeConfig.intVal("port")).thenReturn(9020);
		when(nodeConfig.intVal("connAttemptsLimit")).thenReturn(0);
		when(nodeConfig.<String> listVal("addrs")).thenReturn(List.of("127.0.0.1"));
		when(netConfig.stringVal("transport")).thenReturn("nio");
		when(netConfig.intVal("ioRatio")).thenReturn(50);
		when(netConfig.intVal("writeSpinCount")).thenReturn(1);
		when(netConfig.boolVal("keepAlive")).thenReturn(true);
		when(netConfig.intVal("linger")).thenReturn(0);
		when(netConfig.boolVal("reuseAddr")).thenReturn(true);
		when(netConfig.boolVal("tcpNoDelay")).thenReturn(true);
		return storageConfig;
	}

	private static Output<Operation<Item>> failFirstOutput(final AtomicInteger writes) {
		return new Output<>() {
			@Override
			public boolean put(final Operation<Item> val) {
				if (writes.getAndIncrement() == 0) {
					throw new IllegalStateException("output failed");
				}
				return true;
			}

			@Override
			public int put(final List<Operation<Item>> vals, final int from, final int to) {
				return 0;
			}

			@Override
			public int put(final List<Operation<Item>> vals) {
				return 0;
			}

			@Override
			public Input<Operation<Item>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
	}
}
