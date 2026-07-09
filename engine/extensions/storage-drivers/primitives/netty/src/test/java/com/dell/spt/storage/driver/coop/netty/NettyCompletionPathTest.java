package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

	private EmbeddedChannel newResubmitChannel() {
		final var channel = new EmbeddedChannel();
		channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.FALSE);
		channel.attr(NonBlockingConnPool.ATTR_KEY_NODE).set("test-node:9020");
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
	void complete_clearsOperationAttributeBeforeReleasingChannel() throws Exception {
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));
		channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).set(op);

		doAnswer(inv -> {
			assertNull(channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).get(),
							"a released channel must not retain its completed operation");
			return null;
		}).when(connPool).release(channel);

		driver.complete(channel, op);

		assertNull(channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).get());
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

	// ---------- Fast-recycle path tests ----------

	private void enableFastRecycle(int threshold) throws Exception {
		final var threshField = CoopStorageDriverBase.class.getDeclaredField("fastRecycleConcurrencyThreshold");
		threshField.setAccessible(true);
		threshField.set(driver, threshold);
		when(driver.isStarted()).thenReturn(true);
	}

	@Test
	void fastRecycle_setsDriverRecycledFlagOnOp() throws Exception {
		enableFastRecycle(4);
		// Use higher concurrency so we have room
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1); // 1 active op (under threshold=4)
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		final Operation<Item> resultCopy = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(resultCopy);

		// Mock connPool.lease() to return a new channel for re-submit
		final var resubmitChannel = newResubmitChannel();
		when(connPool.lease()).thenReturn(resubmitChannel);

		driver.complete(channel, op);

		// The op should have driverRecycled set to true before result() is called
		verify(op).driverRecycled(true);
		channel.close();
		resubmitChannel.close();
	}

	@Test
	void fastRecycle_keepsPermitAndReleasesChannel() throws Exception {
		enableFastRecycle(4);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		// Mock connPool.lease() for re-submit
		final var resubmitChannel = newResubmitChannel();
		when(connPool.lease()).thenReturn(resubmitChannel);

		driver.complete(channel, op);

		// Channel should be released to the pool
		verify(connPool).release(channel);
		// Permit should NOT be released — it's held for the re-submitted op.
		// Before complete: 3 available (4 total - 1 acquired).
		// After fast-recycle: still 3 available (permit retained for new submit).
		assertEquals(3, sem.availablePermits(),
						"permit should be retained for the re-submitted op");
		channel.close();
		resubmitChannel.close();
	}

	@Test
	void fastRecycle_resetsOpForResubmit() throws Exception {
		enableFastRecycle(4);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		final var resubmitChannel = newResubmitChannel();
		when(connPool.lease()).thenReturn(resubmitChannel);

		driver.complete(channel, op);

		// prepare() calls op.reset() internally — verify reset was called
		verify(op).reset();
		// Also verify the op was re-submitted (startRequest called on the re-prepared op)
		verify(op, atLeast(1)).startRequest();
		channel.close();
		resubmitChannel.close();
	}

	@Test
	void fastRecycle_fallsBackToNormalPathWhenNotEligible() throws Exception {
		// Fast-recycle enabled but op failed — should use normal path
		enableFastRecycle(4);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.FAIL_IO);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.complete(channel, op);

		// Normal path: permit released, no driverRecycled flag set
		verify(op, never()).driverRecycled(anyBoolean());
		assertEquals(4, sem.availablePermits(), "permit should be released in normal path");
		channel.close();
	}

	@Test
	void fastRecycle_releasesPermitOnLeaseFailure() throws Exception {
		enableFastRecycle(4);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		// connPool.lease() throws — simulating no connections available
		when(connPool.lease()).thenThrow(new java.net.ConnectException("no connections"));

		driver.complete(channel, op);

		// After lease failure, permit should be released
		assertEquals(4, sem.availablePermits(),
						"permit should be released after fast-recycle lease failure");
		channel.close();
	}

	// ---------- Timing accuracy tests ----------

	@Test
	void complete_finishResponseRecordsTimingBeforeRelease() throws Exception {
		// Verify that finishResponse() captures the timestamp at the actual
		// response completion moment — not deferred until after permit release.
		// Uses a real OperationImpl so timing fields are actually written.
		final var channel = newChannelWithReleasedFlag();
		final var item = new ItemImpl("timing-obj");
		final OperationImpl<ItemImpl> realOp = new OperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", null);
		realOp.startRequest();
		realOp.finishRequest();
		realOp.startResponse();
		realOp.status(Operation.Status.SUCC);

		final long beforeComplete = Operation.START_OFFSET_MICROS + System.nanoTime() / 1000;
		driver.complete(channel, (Operation<Item>) (Operation<?>) realOp);
		final long afterComplete = Operation.START_OFFSET_MICROS + System.nanoTime() / 1000;

		assertTrue(realOp.respTimeDone() >= beforeComplete,
						"respTimeDone must be >= time just before complete() call");
		assertTrue(realOp.respTimeDone() <= afterComplete,
						"respTimeDone must be <= time just after complete() call");
		assertTrue(realOp.duration() > 0,
						"duration (respTimeDone - reqTimeStart) should be positive");
		channel.close();
	}

	@Test
	void complete_resultCopyCarriesTimingFromRealOp() throws Exception {
		// Verify the result copy (sent to opResultOut) carries accurate timing
		// from a real OperationImpl, not zeros.
		final var channel = newChannelWithReleasedFlag();
		final var item = new ItemImpl("copy-timing-obj");
		final OperationImpl<ItemImpl> realOp = new OperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", null);
		realOp.startRequest();
		realOp.finishRequest();
		realOp.startResponse();
		realOp.status(Operation.Status.SUCC);

		// Capture the result copy sent to opResultOut
		final List<Operation<?>> capturedResults = new ArrayList<>();
		when(opResultOut.put(any(Operation.class))).thenAnswer(inv -> {
			capturedResults.add(inv.getArgument(0));
			return true;
		});

		driver.complete(channel, (Operation<Item>) (Operation<?>) realOp);

		assertEquals(1, capturedResults.size(), "exactly one result should be sent to output");
		final var resultCopy = capturedResults.get(0);
		assertTrue(resultCopy.reqTimeStart() > 0, "result copy reqTimeStart should be set");
		assertTrue(resultCopy.respTimeDone() > 0, "result copy respTimeDone should be set");
		assertTrue(resultCopy.duration() > 0, "result copy duration should be positive");
		channel.close();
	}

	// ---------- Mixed success/failure permit accounting ----------

	@Test
	void complete_mixedSuccessAndFailure_noPermitLeak() throws Exception {
		// Simulate interleaved completions: op1 succeeds, op2 fails, op3 succeeds.
		// All should release exactly one permit each (3 total).
		final int totalPermits = 4;
		final var sem = new Semaphore(totalPermits, true);
		sem.acquire(3); // 3 in-flight ops
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, totalPermits);

		// Op 1: success
		final var ch1 = newChannelWithReleasedFlag();
		final Operation<Item> op1 = mock(Operation.class);
		when(op1.status()).thenReturn(Operation.Status.SUCC);
		when(op1.result()).thenReturn(mock(Operation.class));
		driver.complete(ch1, op1);

		// Op 2: failure
		final var ch2 = newChannelWithReleasedFlag();
		final Operation<Item> op2 = mock(Operation.class);
		when(op2.status()).thenReturn(Operation.Status.FAIL_IO);
		when(op2.result()).thenReturn(mock(Operation.class));
		driver.complete(ch2, op2);

		// Op 3: success
		final var ch3 = newChannelWithReleasedFlag();
		final Operation<Item> op3 = mock(Operation.class);
		when(op3.status()).thenReturn(Operation.Status.SUCC);
		when(op3.result()).thenReturn(mock(Operation.class));
		driver.complete(ch3, op3);

		assertEquals(totalPermits, sem.availablePermits(),
						"all 3 permits should be released (no leak), returning to full capacity");
		assertEquals(3, driver.completedOpCount(),
						"all 3 ops should be counted as completed");
	}

	// ---------- Fast-recycle driverRecycled flag on result copy ----------

	@Test
	void fastRecycle_resultCopyCarriesDriverRecycledFlag() throws Exception {
		enableFastRecycle(4);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		final var sem = new Semaphore(4, true);
		sem.acquire(1);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);

		final var channel = newChannelWithReleasedFlag();

		// Use a real OperationImpl so driverRecycled flag is actually stored
		final var item = new ItemImpl("recycle-flag-obj");
		final OperationImpl<ItemImpl> realOp = new OperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", null);
		realOp.startRequest();
		realOp.finishRequest();
		realOp.startResponse();
		realOp.status(Operation.Status.SUCC);

		// Capture the result copy to verify the flag
		final List<Operation<?>> capturedResults = new ArrayList<>();
		when(opResultOut.put(any(Operation.class))).thenAnswer(inv -> {
			capturedResults.add(inv.getArgument(0));
			return true;
		});

		final var resubmitChannel = newResubmitChannel();
		when(connPool.lease()).thenReturn(resubmitChannel);

		driver.complete(channel, (Operation<Item>) (Operation<?>) realOp);

		assertEquals(1, capturedResults.size(), "one result should be captured");
		assertTrue(capturedResults.get(0).driverRecycled(),
						"result copy must carry driverRecycled=true from fast-recycle path");
		channel.close();
		resubmitChannel.close();
	}

	// ---------- Channel release ordering ----------

	@Test
	void complete_channelReleasedBeforeHandleCompletedOnNormalPath() throws Exception {
		// Verify the channel is returned to the pool before handleCompleted
		// processes the result. This ensures another submit can immediately
		// lease the channel without conflict.
		final var channel = newChannelWithReleasedFlag();
		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		final AtomicInteger channelReleaseOrder = new AtomicInteger(0);
		final AtomicInteger handleCompletedOrder = new AtomicInteger(0);
		final AtomicInteger orderCounter = new AtomicInteger(0);

		// Track when connPool.release is called
		doAnswer(inv -> {
			channelReleaseOrder.set(orderCounter.incrementAndGet());
			return null;
		}).when(connPool).release(channel);

		// Track when opResultOut.put is called (inside handleCompleted)
		when(opResultOut.put(any(Operation.class))).thenAnswer(inv -> {
			handleCompletedOrder.set(orderCounter.incrementAndGet());
			return true;
		});

		driver.complete(channel, op);

		assertTrue(channelReleaseOrder.get() > 0, "channel release should have been called");
		assertTrue(handleCompletedOrder.get() > 0, "handleCompleted should have been called");
		assertTrue(channelReleaseOrder.get() < handleCompletedOrder.get(),
						"channel must be released BEFORE handleCompleted processes the result");
		channel.close();
	}

	// ---------- Batch submit permit math ----------

	@Test
	void singleSubmit_releasesPermitOnConnectionFailure() throws Exception {
		// When connPool.lease() throws in single submit, the acquired permit
		// must be released so it doesn't leak.
		final var sem = new Semaphore(4, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);
		when(driver.isStarted()).thenReturn(true);

		final Operation<Item> op = mock(Operation.class);
		when(op.type()).thenReturn(OpType.CREATE);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		when(connPool.lease()).thenThrow(new java.net.ConnectException("no connections"));

		boolean submitted = driver.submit(op);

		assertFalse(submitted, "submit should return false on connection failure");
		assertEquals(4, sem.availablePermits(),
						"permit must be returned after connection failure in single submit");
	}

	@Test
	void batchSubmit_releasesExcessPermits() throws Exception {
		// When drainPermits() grabs more permits than needed, excess are returned.
		final var sem = new Semaphore(8, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 8);
		when(driver.isStarted()).thenReturn(true);

		// Create 2 NOOP ops (they complete synchronously in submit)
		final List<Operation<Item>> ops = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			final Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.NOOP);
			when(op.status()).thenReturn(Operation.Status.SUCC);
			when(op.result()).thenReturn(mock(Operation.class));
			ops.add(op);
		}

		// 8 permits available, submit only needs 2 → 6 should be returned
		final int submitted = driver.submit(ops, 0, 2);

		assertEquals(2, submitted, "should have submitted 2 ops");
		// NOOP ops release their permit inline during submit, so all 8 back
		assertEquals(8, sem.availablePermits(),
						"all permits should be available (NOOP releases inline)");
	}

	@Test
	void batchSubmit_releasesPermitsOnConnectionFailure() throws Exception {
		// When a connection lease fails mid-batch, all permits must be returned:
		// the failed op's permit is explicitly released (conn==null path),
		// and remaining unused permits are returned directly.
		final var sem = new Semaphore(8, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 8);
		when(driver.isStarted()).thenReturn(true);

		// First op is a normal CREATE (needs a connection)
		final List<Operation<Item>> ops = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			final Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.status()).thenReturn(Operation.Status.SUCC);
			when(op.result()).thenReturn(mock(Operation.class));
			ops.add(op);
		}

		// connPool.lease() fails immediately
		when(connPool.lease()).thenThrow(new java.net.ConnectException("no connections"));

		final int submitted = driver.submit(ops, 0, 3);

		// drainPermits=8, excess returned=5 (8-3), permits=3
		// First op: lease throws, conn==null → explicit release (1)
		// complete(null, op) → handleCompleted only (no channel release)
		// Catch: releases permits-n-1 = 2
		// Total returned: 5 (excess) + 1 (explicit) + 2 (remaining) = 8
		assertEquals(8, sem.availablePermits(),
						"all permits must be returned after connection failure");
	}

	// ---------- Concurrent completion tests ----------

	@Test
	void complete_concurrentCompletionsDoNotCorruptPermitCount() throws Exception {
		// Multiple threads calling complete() simultaneously should not
		// lose or duplicate permit releases.
		final int concurrency = 16;
		final var sem = new Semaphore(concurrency, true);
		sem.acquire(concurrency); // all permits held
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, sem);
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, concurrency);

		final var startLatch = new CountDownLatch(1);
		final var doneLatch = new CountDownLatch(concurrency);
		final var errors = new AtomicInteger(0);

		for (int i = 0; i < concurrency; i++) {
			Thread.ofVirtual().start(() -> {
				try {
					startLatch.await(5, TimeUnit.SECONDS);
					final var ch = newChannelWithReleasedFlag();
					final Operation<Item> op = mock(Operation.class);
					when(op.status()).thenReturn(Operation.Status.SUCC);
					when(op.result()).thenReturn(mock(Operation.class));
					driver.complete(ch, op);
					ch.close();
				} catch (final Exception e) {
					errors.incrementAndGet();
				} finally {
					doneLatch.countDown();
				}
			});
		}

		startLatch.countDown(); // release all threads at once
		assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "all threads should complete");
		assertEquals(0, errors.get(), "no errors should occur");
		assertEquals(concurrency, sem.availablePermits(),
						"all permits must be released (no leak, no double-release)");
		assertEquals(concurrency, driver.completedOpCount(),
						"all ops should be counted as completed");
	}
}
