package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Queue recovery at a stop visits each queued operation once, wherever it waits: the input queue,
 * the child queue, the dispatch task's buffer and its deferred uploads.
 */
@Timeout(30)
class QueueRecoveryVisitTest {
	private static final int BATCH_SIZE = 1;

	/** Never dispatches: no transport permit, and no multipart permit for an upload. */
	private static final class HoldingDriver extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		final OperationLifecycleTracker<Operation<DataItem>> tracker;
		final Map<Operation<DataItem>, Integer> hookVisits = new IdentityHashMap<>();
		final Set<Operation<DataItem>> hookRefuses = Collections.newSetFromMap(new IdentityHashMap<>());
		volatile List<Operation<DataItem>> extraQueue = List.of();

		HoldingDriver(final DataInput dataInput) throws Exception {
			super("queue-recovery-visit", dataInput, config(), false, BATCH_SIZE);
			tracker = spy(new OperationLifecycleTracker<Operation<DataItem>>());
			enableOperationLifecycle(tracker);
			concurrencyThrottle.drainPermits();
			mpuObjectThrottle = new Semaphore(0);
		}

		@Override
		protected List<Operation<DataItem>> recoverAdditionalQueuedOperations() {
			return extraQueue;
		}

		/** Counts visits; a refused operation is left driver-queued, as a hook may leave it. */
		@Override
		protected boolean recoverQueuedOperation(final Operation<DataItem> op) {
			synchronized (hookVisits) {
				hookVisits.merge(op, 1, Integer::sum);
			}
			return !hookRefuses.contains(op) && super.recoverQueuedOperation(op);
		}

		int visits(final Operation<DataItem> op) {
			synchronized (hookVisits) {
				return hookVisits.getOrDefault(op, 0);
			}
		}

		void queueChild(final Operation<DataItem> op) {
			assertTrue(operationLifecycle().driverQueued(op));
			assertTrue(childOpQueue.offer(op));
		}
	}

	@Test
	void everyQueuedOperationIsInTheRegistryAndSettlesOnceAsUnattempted() throws Exception {
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new HoldingDriver(dataInput);
		try {
			driver.start();
			final var deferredUploads = List.of(upload("deferred-0"), upload("deferred-1"));
			for (final var upload : deferredUploads) {
				assertTrue(driver.put(upload));
			}
			awaitCondition(() -> deferredMpuQueueSize(driver) == deferredUploads.size(),
							"uploads without a multipart permit are deferred");
			final var buffered = plain("buffered");
			assertTrue(driver.put(buffered));
			awaitCondition(() -> dispatchBufferSize(driver) == BATCH_SIZE,
							"the dispatch task holds one operation it cannot submit");
			final var inputQueued = List.of(plain("input-0"), plain("input-1"), plain("input-2"));
			for (final var op : inputQueued) {
				assertTrue(driver.put(op));
			}
			final var childQueued = List.of(plain("child-0"), plain("child-1"));
			childQueued.forEach(driver::queueChild);
			final var queued = new ArrayList<Operation<DataItem>>();
			queued.addAll(deferredUploads);
			queued.add(buffered);
			queued.addAll(inputQueued);
			queued.addAll(childQueued);
			assertRegistryHolds(driver, queued);

			driver.closeAdmission();
			// The stopped task dropped its buffer and deferred uploads; the registry still has them.
			assertEquals(0, deferredMpuQueueSize(driver));
			assertEquals(0, dispatchBufferSize(driver));
			assertRegistryHolds(driver, queued);
			final var recovered = driver.recoverQueuedOperations();

			assertEquals(queued.size(), recovered.size());
			final Set<Operation<DataItem>> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
			distinct.addAll(recovered);
			assertEquals(queued.size(), distinct.size(), "no operation is recovered twice");
			for (final var op : queued) {
				assertTrue(distinct.contains(op), () -> op.item().name() + " is recovered");
				assertEquals(OperationLifecycleState.UNATTEMPTED, op.lifecycle().state());
				verify(driver.tracker, times(1)).unattempted(op);
				assertEquals(1, driver.visits(op), () -> op.item().name() + " reaches the hook once");
			}
			final var snapshot = driver.operationLifecycle().snapshot();
			assertEquals(queued.size(), snapshot.unattempted());
			assertEquals(0, snapshot.driverQueued());
			assertEquals(0, snapshot.dispatched());
			assertTrue(driver.recoverQueuedOperations().isEmpty(), "queue recovery is idempotent");
			for (final var op : queued) {
				verify(driver.tracker, times(1)).unattempted(op);
			}
		} finally {
			driver.close();
		}
	}

	@Test
	void anExtraQueueListingAnOperationTwiceVisitsItsHookOnce() throws Exception {
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new HoldingDriver(dataInput);
		try {
			final var listedTwice = plain("listed-twice");
			assertTrue(driver.tracker.driverQueued(listedTwice));
			driver.extraQueue = List.of(listedTwice, listedTwice);
			driver.closeAdmission();

			assertEquals(List.of(listedTwice), driver.recoverQueuedOperations());
			assertEquals(1, driver.visits(listedTwice), "one hook visit per identity");
			assertEquals(OperationLifecycleState.UNATTEMPTED, listedTwice.lifecycle().state());
			assertEquals(1, driver.operationLifecycle().snapshot().unattempted());
		} finally {
			driver.extraQueue = List.of();
			driver.close();
		}
	}

	@Test
	void anOperationItsHookLeavesQueuedIsNotVisitedAgainFromTheRegistry() throws Exception {
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new HoldingDriver(dataInput);
		try {
			driver.start();
			final var buffered = plain("buffered");
			assertTrue(driver.put(buffered));
			awaitCondition(() -> dispatchBufferSize(driver) == BATCH_SIZE,
							"the dispatch task holds one operation it cannot submit");
			final var leftQueued = plain("left-queued");
			driver.hookRefuses.add(leftQueued);
			assertTrue(driver.put(leftQueued));
			assertRegistryHolds(driver, List.of(buffered, leftQueued));
			driver.closeAdmission();

			// The input queue's entry is visited first and stays driver-queued, so the registry
			// still lists it when the second pass settles the dispatch task's buffer.
			assertEquals(List.of(buffered), driver.recoverQueuedOperations());
			assertEquals(1, driver.visits(leftQueued), "a queued operation reaches the hook once");
			assertEquals(1, driver.visits(buffered));
			assertEquals(OperationLifecycleState.DRIVER_QUEUED, leftQueued.lifecycle().state());
			assertEquals(OperationLifecycleState.UNATTEMPTED, buffered.lifecycle().state());
			verify(driver.tracker, never()).unattempted(leftQueued);
		} finally {
			driver.hookRefuses.clear();
			driver.close();
		}
	}

	private static void assertRegistryHolds(
					final HoldingDriver driver, final List<Operation<DataItem>> queued) {
		final Set<Operation<DataItem>> registry = Collections.newSetFromMap(new IdentityHashMap<>());
		registry.addAll(driver.operationLifecycle().driverQueuedOperations());
		assertEquals(queued.size(), registry.size());
		for (final var op : queued) {
			assertTrue(registry.contains(op), () -> op.item().name() + " is in the registry");
		}
	}

	private static Operation<DataItem> plain(final String name) {
		return new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl(name, 0, 1), null, "/bucket", null, List.of(), 0);
	}

	@SuppressWarnings("unchecked")
	private static Operation<DataItem> upload(final String name) throws Exception {
		final var item = new DataItemImpl(name, 0, 2);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return (Operation<DataItem>) (Operation<?>) new CompositeDataOperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", null, null, 0, 1);
	}

	private static Object dispatchTask(final HoldingDriver driver) throws ReflectiveOperationException {
		final var field = CoopStorageDriverBase.class.getDeclaredField("opDispatchTask");
		field.setAccessible(true);
		return field.get(driver);
	}

	private static int deferredMpuQueueSize(final HoldingDriver driver) {
		try {
			final var field = OperationDispatchTask.class.getDeclaredField("deferredMpuQueue");
			field.setAccessible(true);
			return ((Queue<?>) field.get(dispatchTask(driver))).size();
		} catch (final ReflectiveOperationException e) {
			throw new AssertionError(e);
		}
	}

	private static int dispatchBufferSize(final HoldingDriver driver) {
		try {
			final var field = OperationDispatchTask.class.getDeclaredField("buff");
			field.setAccessible(true);
			final var buff = field.get(dispatchTask(driver));
			return (int) buff.getClass().getMethod("size").invoke(buff);
		} catch (final ReflectiveOperationException e) {
			throw new AssertionError(e);
		}
	}

	private static void awaitCondition(final BooleanSupplier condition, final String message)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), message);
	}

	private static Config config() {
		final var storage = mock(Config.class);
		final var driver = mock(Config.class);
		final var limit = mock(Config.class);
		final var auth = mock(Config.class);
		final var integrity = mock(Config.class);
		final var input = mock(Config.class);
		when(storage.configVal("driver")).thenReturn(driver);
		when(driver.configVal("limit")).thenReturn(limit);
		when(storage.stringVal("namespace")).thenReturn("test-ns");
		when(storage.configVal("auth")).thenReturn(auth);
		when(storage.configVal("integrity")).thenReturn(integrity);
		when(integrity.stringVal("mode")).thenReturn("none");
		when(integrity.stringVal("algorithm")).thenReturn("sha256");
		when(integrity.configVal("input")).thenReturn(input);
		when(input.stringVal("provenance")).thenReturn("none");
		when(input.stringVal("expectedProducerId")).thenReturn("");
		when(auth.stringVal("uid")).thenReturn("user");
		when(auth.stringVal("secret")).thenReturn("secret");
		when(storage.intVal("driver-limit-queue-input")).thenReturn(16);
		when(limit.intVal("concurrency")).thenReturn(1);
		return storage;
	}
}
