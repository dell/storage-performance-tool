package com.dell.spt.storage.driver.coop.netty.http.swift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.concurrent.AsyncRunnable;
import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

final class SwiftLifecycleTest {
	private static void setField(final Class<?> owner, final Object target, final String name, final Object value)
					throws ReflectiveOperationException {
		final var field = owner.getDeclaredField(name);
		field.setAccessible(true);
		field.set(target, value);
	}

	@Test
	@SuppressWarnings("unchecked")
	void compositeAdmissionClosureRecoversAllChildrenWithoutSubmissionSpin() throws Exception {
		final SwiftStorageDriver<DataItem, Operation<DataItem>> driver = mock(
						SwiftStorageDriver.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var stateField = AsyncRunnableBase.class.getDeclaredField("state");
		stateField.setAccessible(true);
		stateField.set(driver, AsyncRunnable.State.STARTED);
		final var lifecycleField = StorageDriverBase.class.getDeclaredField("operationLifecycle");
		lifecycleField.setAccessible(true);
		lifecycleField.set(driver, new OperationLifecycleTracker<Operation<DataItem>>());
		final var admissionLockField = CoopStorageDriverBase.class.getDeclaredField("admissionLock");
		admissionLockField.setAccessible(true);
		admissionLockField.set(driver, new ReentrantLock());
		final var admissionOpenField = CoopStorageDriverBase.class.getDeclaredField("admissionOpen");
		admissionOpenField.setAccessible(true);
		admissionOpenField.set(driver, false);

		final var dataItem = new DataItemImpl("closed-dlo", 0, 2048);
		final CompositeDataOperation<DataItem> dloOp = new CompositeDataOperationImpl<>(
						hashCode(), OpType.CREATE, dataItem, null, "/container", null,
						null, 0, 1024);
		final Operation<DataItem> parent = (Operation<DataItem>) dloOp;
		assertTrue(driver.operationLifecycle().driverQueued(parent));

		assertFalse(driver.submit(parent));

		assertEquals(OperationLifecycleState.UNATTEMPTED, parent.lifecycle().state(),
						"closed admission must reconcile the parent as well as its generated children");
		assertTrue(dloOp.subOperations().stream()
						.allMatch(child -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED));
		assertEquals(3, driver.operationLifecycle().snapshot().unattempted());
	}

	@Test
	@SuppressWarnings("unchecked")
	void partialTransportKeepsSwiftParentInFlightAndRemainingSiblingRecoverable() throws Exception {
		final SwiftStorageDriver<DataItem, Operation<DataItem>> driver = mock(
						SwiftStorageDriver.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		setField(AsyncRunnableBase.class, driver, "state", AsyncRunnable.State.STARTED);
		setField(StorageDriverBase.class, driver, "operationLifecycle",
						new OperationLifecycleTracker<Operation<DataItem>>());
		final var admissionLock = new ReentrantLock();
		setField(CoopStorageDriverBase.class, driver, "admissionLock", admissionLock);
		setField(CoopStorageDriverBase.class, driver, "admissionOpen", true);
		final var dispatchLock = new ReentrantLock();
		setField(CoopStorageDriverBase.class, driver, "dispatchLock", dispatchLock);
		setField(CoopStorageDriverBase.class, driver, "dispatchReady", dispatchLock.newCondition());
		setField(CoopStorageDriverBase.class, driver, "childOpQueue",
						new ArrayBlockingQueue<Operation<DataItem>>(4));

		final var dataItem = new DataItemImpl("partial-dlo", 0, 2048);
		final CompositeDataOperation<DataItem> dloOp = new CompositeDataOperationImpl<>(
						hashCode(), OpType.CREATE, dataItem, null, "/container", null,
						null, 0, 1024);
		final Operation<DataItem> parent = (Operation<DataItem>) dloOp;
		assertTrue(driver.operationLifecycle().driverQueued(parent));
		assertTrue(driver.submit(parent), "Swift expansion should claim and enqueue both segments");

		final var children = dloOp.subOperations();
		final var beginDispatch = CoopStorageDriverBase.class.getDeclaredMethod(
						"beginDispatch", Operation.class);
		beginDispatch.setAccessible(true);
		assertTrue((Boolean) beginDispatch.invoke(driver, children.get(0)));
		assertEquals(OperationLifecycleState.DISPATCHED, parent.lifecycle().state(),
						"the first segment transport must make its composite parent in flight");

		setField(CoopStorageDriverBase.class, driver, "admissionOpen", false);
		assertTrue(driver.operationLifecycle().unattempted((Operation<DataItem>) children.get(1)));
		assertEquals(2, driver.operationLifecycle().resolveOutstandingAsUnresolved(),
						"the transported child and its parent must both outlive the bound");

		assertEquals(OperationLifecycleState.UNRESOLVED, parent.lifecycle().state());
		assertEquals(OperationLifecycleState.UNRESOLVED, children.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, children.get(1).lifecycle().state());
		assertEquals(2, driver.operationLifecycle().snapshot().unresolved());
		assertEquals(1, driver.operationLifecycle().snapshot().unattempted());
	}
}
