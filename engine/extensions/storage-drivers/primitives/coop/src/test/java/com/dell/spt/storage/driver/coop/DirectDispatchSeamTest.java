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
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the completion-driven direct dispatch seam on {@link CoopStorageDriverBase}. The
 * caller of {@code pollForDirectDispatch()} models a transport completion which still holds a
 * concurrency permit and asks for the next plain operation to send on the channel it already has.
 */
@SuppressWarnings("unchecked")
class DirectDispatchSeamTest {

	/** Refuses every submit so the dispatcher never consumes queued work on its own. */
	private static final class RefusingDriver
					extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		private RefusingDriver(final Config storageConfig) throws Exception {
			super("direct-dispatch-step", dataInput(), storageConfig, false, 4);
		}

		@Override
		protected boolean submit(final Operation<DataItem> op) {
			return false;
		}

		@Override
		protected int submit(final List<Operation<DataItem>> ops, final int from, final int to) {
			return 0;
		}

		@Override
		protected int submit(final List<Operation<DataItem>> ops) {
			return 0;
		}
	}

	/** Steals the only permit inside its first submit so the dispatcher retains a backlog. */
	private static final class PermitStealingDriver
					extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		private boolean stolen;

		private PermitStealingDriver(final Config storageConfig) throws Exception {
			super("direct-dispatch-backlog-step", dataInput(), storageConfig, false, 4);
		}

		@Override
		protected boolean submit(final Operation<DataItem> op) {
			if (!stolen) {
				stolen = true;
				assertTrue(concurrencyThrottle.tryAcquire());
			}
			return false;
		}

		@Override
		protected int submit(final List<Operation<DataItem>> ops, final int from, final int to) {
			return submit(ops.get(from)) ? 1 : 0;
		}

		@Override
		protected int submit(final List<Operation<DataItem>> ops) {
			return submit(ops, 0, ops.size());
		}
	}

	private CoopStorageDriverBase<DataItem, Operation<DataItem>> driver;

	@BeforeEach
	void enableDirectDispatch() {
		System.setProperty(CoopStorageDriverBase.DIRECT_DISPATCH_PROPERTY, "true");
	}

	@AfterEach
	void closeDriver() throws Exception {
		System.clearProperty(CoopStorageDriverBase.DIRECT_DISPATCH_PROPERTY);
		if (driver != null) {
			driver.close();
		}
	}

	private static DataInput dataInput() throws Exception {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	private static Config storageConfig(final int concurrencyLimit) {
		final var storageConfig = mock(Config.class);
		final var driverConfig = mock(Config.class);
		final var limitConfig = mock(Config.class);
		final var authConfig = mock(Config.class);
		final var integrityConfig = mock(Config.class);
		final var integrityInputConfig = mock(Config.class);
		when(storageConfig.configVal("driver")).thenReturn(driverConfig);
		when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		when(storageConfig.stringVal("namespace")).thenReturn("test-ns");
		when(storageConfig.configVal("auth")).thenReturn(authConfig);
		when(storageConfig.configVal("integrity")).thenReturn(integrityConfig);
		when(integrityConfig.stringVal("mode")).thenReturn("none");
		when(integrityConfig.stringVal("algorithm")).thenReturn("sha256");
		when(integrityConfig.configVal("input")).thenReturn(integrityInputConfig);
		when(integrityInputConfig.stringVal("provenance")).thenReturn("none");
		when(integrityInputConfig.stringVal("expectedProducerId")).thenReturn("");
		when(authConfig.stringVal("uid")).thenReturn("user");
		when(authConfig.stringVal("secret")).thenReturn("secret");
		when(authConfig.stringVal("token")).thenReturn(null);
		when(limitConfig.intVal("concurrency")).thenReturn(concurrencyLimit);
		when(driverConfig.intVal("threads")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-queue-input")).thenReturn(16);
		when(storageConfig.intVal("driver-limit-multipart-objects")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-multipart-parts")).thenReturn(0);
		return storageConfig;
	}

	private static Operation<DataItem> plainOp(final String name) {
		return new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl(name, 0, 1), null, "/bucket", null, List.of(), 0);
	}

	private static Operation<DataItem> compositeOp(final String name) throws Exception {
		final var item = new DataItemImpl(name, 0, 4096);
		item.dataInput(dataInput());
		return new CompositeDataOperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", null, null, 0, 1024);
	}

	private static void awaitCondition(
					final BooleanSupplier condition, final String failureMessage) throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), failureMessage);
	}

	/** Starts a driver whose single permit is held by the caller, as a completing op would hold it. */
	private CoopStorageDriverBase<DataItem, Operation<DataItem>> startWithPermitHeld() throws Exception {
		final var started = new RefusingDriver(storageConfig(1));
		final Output<Operation<DataItem>> resultOutput = mock(Output.class);
		when(resultOutput.put(any(Operation.class))).thenReturn(true);
		started.operationResultOutput(resultOutput);
		started.start();
		assertTrue(started.concurrencyThrottle.tryAcquire());
		driver = started;
		return started;
	}

	@Test
	void pollsQueuedPlainOperationAndMarksItDispatched() throws Exception {
		final var driver = startWithPermitHeld();
		final var op = plainOp("plain");
		assertTrue(driver.put(op));
		awaitCondition(
						() -> driver.operationLifecycle().snapshot().driverQueued() == 1,
						"operation should be queued while the permit is held");

		assertSame(op, driver.pollForDirectDispatch());

		assertEquals(OperationLifecycleState.DISPATCHED, driver.operationLifecycle().stateOf(op));
		assertTrue(driver.operationLifecycle().hasExplicitDispatchBoundary(op));
		assertNull(driver.pollForDirectDispatch(), "the queue is now empty");
		driver.closeAdmission();
		assertTrue(driver.recoverQueuedOperations().isEmpty(), "a dispatched op is not recovered");
	}

	@Test
	void refusesWhenHeadIsCompositeAndLeavesItQueued() throws Exception {
		final var driver = startWithPermitHeld();
		final var composite = compositeOp("mpu");
		final var plain = plainOp("behind-composite");
		assertTrue(driver.put(composite));
		assertTrue(driver.put(plain));
		awaitCondition(
						() -> driver.operationLifecycle().snapshot().driverQueued() == 2,
						"both operations should be queued while the permit is held");

		assertNull(driver.pollForDirectDispatch(), "composite work belongs to the dispatcher");

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, driver.operationLifecycle().stateOf(composite));
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, driver.operationLifecycle().stateOf(plain));
		driver.closeAdmission();
		assertEquals(2, driver.recoverQueuedOperations().size());
	}

	@Test
	void refusesWhileChildOperationsArePending() throws Exception {
		final var driver = startWithPermitHeld();
		final var plain = plainOp("plain");
		assertTrue(driver.put(plain));
		awaitCondition(
						() -> driver.operationLifecycle().snapshot().driverQueued() == 1,
						"operation should be queued while the permit is held");
		final var child = plainOp("child");
		assertTrue(driver.operationLifecycle().driverQueued(child));
		assertTrue(driver.childOpQueue.offer(child));

		assertNull(driver.pollForDirectDispatch(), "child operations keep dispatcher priority");

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, driver.operationLifecycle().stateOf(plain));
	}

	@Test
	void refusesAfterAdmissionCloseAndRecoveryStaysLossless() throws Exception {
		final var driver = startWithPermitHeld();
		final var op = plainOp("late");
		assertTrue(driver.put(op));
		awaitCondition(
						() -> driver.operationLifecycle().snapshot().driverQueued() == 1,
						"operation should be queued while the permit is held");

		driver.closeAdmission();

		assertNull(driver.pollForDirectDispatch(), "nothing crosses dispatch after admission closes");
		assertEquals(List.of(op), driver.recoverQueuedOperations());
		assertEquals(OperationLifecycleState.UNATTEMPTED, driver.operationLifecycle().stateOf(op));
	}

	@Test
	void refusesWhileDispatcherRetainsBacklog() throws Exception {
		final var stealing = new PermitStealingDriver(storageConfig(1));
		driver = stealing;
		final Output<Operation<DataItem>> resultOutput = mock(Output.class);
		when(resultOutput.put(any(Operation.class))).thenReturn(true);
		stealing.operationResultOutput(resultOutput);
		stealing.start();
		final var buffered = plainOp("buffered");
		assertTrue(stealing.put(buffered));
		awaitCondition(() -> stealing.dispatcherBacklog() == 1, "dispatcher should retain the refused op");
		final var queued = plainOp("queued");
		assertTrue(stealing.put(queued));
		awaitCondition(
						() -> stealing.operationLifecycle().snapshot().driverQueued() == 2,
						"second operation should be queued");

		assertNull(stealing.pollForDirectDispatch(), "buffered dispatcher work gets the next permit");

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, stealing.operationLifecycle().stateOf(queued));
		stealing.closeAdmission();
		assertEquals(2, stealing.recoverQueuedOperations().size(), "both identities recovered");
	}
}
