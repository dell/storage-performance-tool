package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Completion-driven direct dispatch in {@link NettyStorageDriverBase#complete}: a successful
 * completion on an active channel hands its permit and channel to the next plain queued operation
 * instead of releasing both and waking the dispatcher.
 */
@SuppressWarnings("unchecked")
class NettyDirectDispatchTest {

	private NettyStorageDriverBase<Item, Operation<Item>> driver;
	private Semaphore concurrencyThrottle;
	private NonBlockingConnPool connPool;
	private Output<Operation<Item>> opResultOut;
	private ArrayBlockingQueue<Operation<Item>> inOpQueue;

	@BeforeEach
	void setUp() throws Exception {
		driver = mock(NettyStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		concurrencyThrottle = new Semaphore(1, true);
		concurrencyThrottle.acquire();
		set(CoopStorageDriverBase.class, "concurrencyThrottle", concurrencyThrottle);
		set(StorageDriverBase.class, "concurrencyLimit", 1);
		connPool = mock(NonBlockingConnPool.class);
		set(NettyStorageDriverBase.class, "connPool", connPool);
		set(StorageDriverBase.class, "stepId", "test-step");
		set(CoopStorageDriverBase.class, "completedOpCount", new LongAdder());
		set(CoopStorageDriverBase.class, "childOpQueue", new ArrayBlockingQueue<>(100));
		final var lock = new ReentrantLock();
		set(CoopStorageDriverBase.class, "dispatchLock", lock);
		set(CoopStorageDriverBase.class, "admissionLock", lock);
		set(CoopStorageDriverBase.class, "admissionOpen", true);
		inOpQueue = new ArrayBlockingQueue<>(100);
		set(CoopStorageDriverBase.class, "inOpQueue", inOpQueue);
		opResultOut = mock(Output.class);
		when(opResultOut.put(any(Operation.class))).thenReturn(true);
		set(StorageDriverBase.class, "opResultOut", opResultOut);
		set(CoopStorageDriverBase.class, "directDispatchEnabled", true);
		doReturn(true).when(driver).isStarted();
	}

	private void set(final Class<?> owner, final String name, final Object value) throws Exception {
		final var field = owner.getDeclaredField(name);
		field.setAccessible(true);
		field.set(driver, value);
	}

	private static EmbeddedChannel heldChannel() {
		final var channel = new EmbeddedChannel();
		channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.FALSE);
		channel.attr(NonBlockingConnPool.ATTR_KEY_NODE).set("test-node:9020");
		return channel;
	}

	private static Operation<Item> completedOp(final Operation.Status status) {
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(status);
		when(op.type()).thenReturn(OpType.CREATE);
		when(op.result()).thenReturn(mock(Operation.class));
		return op;
	}

	private static Operation<Item> nextOp() {
		final Operation<Item> op = mock(Operation.class);
		when(op.type()).thenReturn(OpType.CREATE);
		when(op.status()).thenReturn(Operation.Status.PENDING);
		when(op.result()).thenReturn(mock(Operation.class));
		return op;
	}

	@Test
	void successfulCompletionHandsPermitAndChannelToNextQueuedOperation() {
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.SUCC);
		final var next = nextOp();
		channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).set(done);
		inOpQueue.add(next);

		driver.complete(channel, done);

		assertTrue(inOpQueue.isEmpty(), "next op was taken by the completion");
		verify(opResultOut).put(any(Operation.class));
		assertEquals(0, concurrencyThrottle.availablePermits(), "permit is transferred, not released");
		verify(connPool, never()).release(channel);
		verify(driver).sendRequest(channel, next);
		verify(next).startRequest();
		verify(next).nodeAddr("test-node:9020");
		assertSame(next, channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).get());
		assertEquals(Boolean.FALSE, channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).get(),
						"the channel is held again on behalf of the next operation");
		assertTrue(channel.isActive());
		channel.close();
	}

	@Test
	void emptyQueueFallsBackToReleaseAndDispatcherWake() {
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.SUCC);

		driver.complete(channel, done);

		assertEquals(1, concurrencyThrottle.availablePermits());
		verify(connPool).release(channel);
		verify(driver, never()).sendRequest(any(), any());
		assertEquals(Boolean.TRUE, channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).get());
		channel.close();
	}

	@Test
	void disabledPropertyNeverPolls() throws Exception {
		set(CoopStorageDriverBase.class, "directDispatchEnabled", false);
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.SUCC);
		inOpQueue.add(nextOp());

		driver.complete(channel, done);

		assertEquals(1, inOpQueue.size(), "queued work stays for the dispatcher");
		verify(driver, never()).sendRequest(any(), any());
		assertEquals(1, concurrencyThrottle.availablePermits());
		verify(connPool).release(channel);
		channel.close();
	}

	@Test
	void failedCompletionNeverPollsAndClosesChannel() {
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.FAIL_IO);
		inOpQueue.add(nextOp());

		driver.complete(channel, done);

		assertEquals(1, inOpQueue.size());
		verify(driver, never()).sendRequest(any(), any());
		assertEquals(1, concurrencyThrottle.availablePermits());
		verify(connPool).release(channel);
		assertFalse(channel.isActive());
	}

	@Test
	void compositeCompletionKeepsDispatcherPath() {
		final var channel = heldChannel();
		final Operation<Item> done = mock(CompositeOperation.class);
		when(done.status()).thenReturn(Operation.Status.SUCC);
		when(done.type()).thenReturn(OpType.CREATE);
		when(done.result()).thenReturn(mock(Operation.class));
		inOpQueue.add(nextOp());

		driver.complete(channel, done);

		assertEquals(1, inOpQueue.size());
		verify(driver, never()).sendRequest(any(), any());
		assertEquals(1, concurrencyThrottle.availablePermits());
		verify(connPool).release(channel);
		channel.close();
	}

	@Test
	void sendFailureCompletesNextOperationAsFailedAndReleasesTransport() {
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.SUCC);
		final var next = nextOp();
		inOpQueue.add(next);
		doThrow(new IllegalStateException("send failed")).when(driver).sendRequest(channel, next);

		driver.complete(channel, done);

		verify(next).status(Operation.Status.FAIL_UNKNOWN);
		verify(driver).complete(channel, next);
		assertEquals(1, concurrencyThrottle.availablePermits(), "permit returns through the failure path");
		verify(connPool).release(channel);
		verify(opResultOut, times(2)).put(any(Operation.class));
	}

	@Test
	void resultPublicationFailureStillReleasesTransport() {
		final var channel = heldChannel();
		final var done = completedOp(Operation.Status.SUCC);
		inOpQueue.add(nextOp());
		when(opResultOut.put(any(Operation.class))).thenThrow(new IllegalStateException("output down"));

		assertThrows(IllegalStateException.class, () -> driver.complete(channel, done));

		assertEquals(1, concurrencyThrottle.availablePermits());
		verify(connPool).release(channel);
		verify(driver, never()).sendRequest(any(), any());
		assertEquals(1, inOpQueue.size(), "nothing is polled once completion has failed");
		channel.close();
	}
}
