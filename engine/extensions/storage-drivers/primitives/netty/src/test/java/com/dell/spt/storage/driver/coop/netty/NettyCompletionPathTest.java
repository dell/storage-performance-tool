package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
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
 * Characterization tests for {@link NettyStorageDriverBase#complete(io.netty.channel.Channel,
 * Operation)}. These capture the current completion path behavior as a safety net before
 * introducing fast-recycle dispatch.
 */
@SuppressWarnings("unchecked")
class NettyCompletionPathTest {

	private NettyStorageDriverBase<Item, Operation<Item>> driver;
	private Semaphore concurrencyThrottle;
	private NonBlockingConnPool connPool;
	private Output<Operation<Item>> opResultOut;

	@BeforeEach
	void setUp() throws Exception {
		driver = mock(NettyStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// concurrencyThrottle — 1 permit, acquired (simulating an in-flight op)
		concurrencyThrottle = new Semaphore(1, true);
		concurrencyThrottle.acquire(); // simulate op holding the permit
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, concurrencyThrottle);

		// concurrencyLimit (needed by activeOpCount)
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 1);

		// connPool — mock
		connPool = mock(NonBlockingConnPool.class);
		final var poolField = NettyStorageDriverBase.class.getDeclaredField("connPool");
		poolField.setAccessible(true);
		poolField.set(driver, connPool);

		// stepId
		final var stepIdField = StorageDriverBase.class.getDeclaredField("stepId");
		stepIdField.setAccessible(true);
		stepIdField.set(driver, "test-step");

		// completedOpCount
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);
		completedField.set(driver, new LongAdder());

		// childOpQueue
		final var childQueueField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childQueueField.setAccessible(true);
		childQueueField.set(driver, new ArrayBlockingQueue<>(100));

		// dispatch lock and condition
		final var lockField = CoopStorageDriverBase.class.getDeclaredField("dispatchLock");
		lockField.setAccessible(true);
		final var lock = new ReentrantLock();
		lockField.set(driver, lock);
		final var condField = CoopStorageDriverBase.class.getDeclaredField("dispatchReady");
		condField.setAccessible(true);
		condField.set(driver, lock.newCondition());

		// opResultOut — mock that accepts anything
		opResultOut = mock(Output.class);
		when(opResultOut.put(any(Operation.class))).thenReturn(true);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, opResultOut);
	}

	private EmbeddedChannel newChannelWithReleasedFlag() {
		final var channel = new EmbeddedChannel();
		channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.FALSE);
		return channel;
	}

	@Test
	void complete_releasesPermitAndChannel() throws Exception {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		assertEquals(0, concurrencyThrottle.availablePermits(), "precondition: permit held");

		driver.complete(channel, op);

		assertEquals(1, concurrencyThrottle.availablePermits(), "permit should be released");
		verify(connPool).release(channel);
		channel.close();
	}

	@Test
	void complete_releasesPermitBeforeHandleCompleted() throws Exception {
		// Verify the ordering guarantee: permit is freed before handleCompleted runs,
		// so child ops submitted by handleCompleted can re-acquire a permit.
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		final Operation<Item> resultCopy = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(resultCopy);

		// When opResultOut.put() is called (inside handleCompleted), the permit should
		// already be released.
		when(opResultOut.put(any(Operation.class))).thenAnswer(inv -> {
			assertEquals(1, concurrencyThrottle.availablePermits(),
							"permit must be released before handleCompleted processes the result");
			return true;
		});

		driver.complete(channel, op);

		verify(opResultOut).put(resultCopy);
		channel.close();
	}

	@Test
	void complete_callsFinishResponse() {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.complete(channel, op);

		verify(op).finishResponse();
		channel.close();
	}

	@Test
	void complete_closesChannelOnFailure() {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.FAIL_IO);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.complete(channel, op);

		// EmbeddedChannel.close() is asynchronous; check the state
		assertFalse(channel.isActive(), "channel should be closed on failure");
	}

	@Test
	void complete_doesNotCloseChannelOnSuccess() {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.complete(channel, op);

		assertTrue(channel.isOpen(), "channel should remain open on success");
		channel.close();
	}

	@Test
	void complete_idempotentRelease() throws Exception {
		// Simulates a scenario where complete() is called twice for the same op
		// (e.g., both success handler and exception handler fire).
		// The permit and channel should only be released once.
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.complete(channel, op);
		driver.complete(channel, op);

		// Permit starts at 0 (acquired), gets released once → 1
		// If released twice it would be 2, which is wrong.
		assertEquals(1, concurrencyThrottle.availablePermits(),
						"permit should only be released once even with double complete");
		verify(connPool, times(1)).release(channel);
		channel.close();
	}

	@Test
	void complete_handlesNullChannel() {
		// NOOP operations have a null channel
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		// Should not throw
		assertDoesNotThrow(() -> driver.complete(null, op));
		// Permit should NOT be released (null channel path skips release)
		assertEquals(0, concurrencyThrottle.availablePermits(),
						"null channel should not release permit");
	}

	@Test
	void complete_incrementsCompletedCount() {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		assertEquals(0, driver.completedOpCount(), "precondition: count starts at 0");

		driver.complete(channel, op);

		assertEquals(1, driver.completedOpCount(), "completedOpCount should be incremented");
		channel.close();
	}
}
