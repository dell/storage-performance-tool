package com.dell.spt.storage.driver.coop.netty;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class NettyStorageDriverBaseTest {

	private NettyStorageDriverBase<Item, Operation<Item>> driver;
	private Semaphore concurrencyThrottle;
	private NonBlockingConnPool connPool;
	private OperationLifecycleTracker<Operation<Item>> lifecycle;

	@BeforeEach
	void setUp() throws Exception {
		driver = mock(NettyStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// Set up concurrency throttle with 1 available permit
		concurrencyThrottle = new Semaphore(1, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, concurrencyThrottle);

		// Set up concurrencyLimit (needed by isStarted/submit checks)
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 1);
		lifecycle = new OperationLifecycleTracker<>();
		final var lifecycleField = StorageDriverBase.class.getDeclaredField("operationLifecycle");
		lifecycleField.setAccessible(true);
		lifecycleField.set(driver, lifecycle);
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);
		completedField.set(driver, new LongAdder());

		final var isStartedField = com.dell.spt.base.concurrent.AsyncRunnableBase.class.getDeclaredField("state");
		isStartedField.setAccessible(true);
		isStartedField.set(driver, com.dell.spt.base.concurrent.AsyncRunnable.State.STARTED);

		// Set up connection pool
		connPool = mock(NonBlockingConnPool.class);
		final var poolField = NettyStorageDriverBase.class.getDeclaredField("connPool");
		poolField.setAccessible(true);
		poolField.set(driver, connPool);

		// We do not want to execute real requests over the mocked channel
		doNothing().when(driver).sendRequest(any(Channel.class), any(Operation.class));
		doNothing().when(driver).complete(any(), any(Operation.class));
	}

	@Test
	void legacySubmitOverrideRetainsSuccessfulReturnDispatchFallback() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.READ, new DataItemImpl("legacy-netty-submit", 0, 1), null, "/bucket", null);
		assertTrue(lifecycle.driverQueued(op));
		final var dispatchToken = lifecycle.queuedDispatchToken(op);
		assertTrue(dispatchToken != null);
		doReturn(true).when(driver).submit(op);

		assertTrue(driver.submit(op));
		final var compatibilityBoundary = CoopStorageDriverBase.class.getDeclaredMethod(
						"successfulSubmitStartsTransport", Operation.class);
		compatibilityBoundary.setAccessible(true);
		assertTrue((boolean) compatibilityBoundary.invoke(driver, op));
		assertTrue(lifecycle.dispatched(dispatchToken));

		assertEquals(OperationLifecycleState.DISPATCHED, lifecycle.stateOf(op));
		assertEquals(1, lifecycle.inFlightCount());
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
		verify(driver).sendRequest(liveConn, op);
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
		verify(driver).sendRequest(liveConn, op);
	}

	@Test
	void blockedBatchLeaseDispatchesOnlyTheMemberWhoseAcquisitionStarted() throws Exception {
		concurrencyThrottle = new Semaphore(3, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, concurrencyThrottle);
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

		final var submitting = Thread.ofVirtual().start(() -> driver.submit(ops, 0, ops.size()));
		assertTrue(leaseEntered.await(2, TimeUnit.SECONDS));
		assertEquals(OperationLifecycleState.DISPATCHED, ops.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, ops.get(1).lifecycle().state());
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, ops.get(2).lifecycle().state());
		assertEquals(1, lifecycle.inFlightCount());

		releaseLease.countDown();
		submitting.join();
		assertTrue(ops.stream()
						.allMatch(op -> op.lifecycle().state() == OperationLifecycleState.DISPATCHED));
	}

	@Test
	void noopPublicationFailureReleasesSinglePermitExactlyOnce() {
		final var writes = new AtomicInteger();
		driver.operationResultOutput(failFirstOutput(writes));
		doCallRealMethod().when(driver).complete(any(), any(Operation.class));
		final Operation<Item> failedOutput = new OperationImpl<>(
						0, OpType.NOOP, new DataItemImpl("noop-output-failure", 0, 1), null, "/bucket", null);
		assertTrue(lifecycle.driverQueued(failedOutput));

		driver.submit(failedOutput);

		assertEquals(1, writes.get());
		assertEquals(1, concurrencyThrottle.availablePermits());
		assertEquals(0, driver.activeOpCount());
	}

	@Test
	void noopBatchPublicationFailurePreservesPermitBoundAndTrailingProgress() throws Exception {
		concurrencyThrottle = new Semaphore(2, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, concurrencyThrottle);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 2);
		final var writes = new AtomicInteger();
		driver.operationResultOutput(failFirstOutput(writes));
		doCallRealMethod().when(driver).complete(any(), any(Operation.class));
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
		assertEquals(2, concurrencyThrottle.availablePermits());
		assertEquals(0, driver.activeOpCount());
		assertEquals(OperationLifecycleState.TERMINAL, ops.get(1).lifecycle().state());
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
