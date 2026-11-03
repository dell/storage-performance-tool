package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;

/** A composite read through the driver's completion path, with a range about to retry. */
@SuppressWarnings("unchecked")
final class CompositeReadFailureDriverTest {

	private static final class D extends CoopStorageDriverMock<Item, Operation<Item>> {
		D(final DataInput in, final Config cfg) throws Exception {
			super("composite-read-step", in, cfg, false, 1);
		}
	}

	private static BlockingQueue<Operation<Item>> childQueueOf(final CoopStorageDriverBase<Item, Operation<Item>> driver) throws Exception {
		final var field = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		field.setAccessible(true);
		return (BlockingQueue<Operation<Item>>) field.get(driver);
	}

	private static Config cfg() {
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
		when(limitConfig.intVal("concurrency")).thenReturn(4);
		when(driverConfig.intVal("threads")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-queue-input")).thenReturn(100);
		when(storageConfig.intVal("driver-limit-multipart-objects")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-multipart-parts")).thenReturn(0);
		return storageConfig;
	}

	/**
	 * Three ranges: C already ran out of retries; A failed with retries left and has finished its
	 * response (so the pending count dropped) but not yet reached handleCompleted; B succeeds meanwhile.
	 */
	@Test
	void aRangeAboutToRetryDoesNotLetASiblingFinalizeTheRead() throws Exception {
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new D(dataInput, cfg());
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(out);
		try {
			final var baseItem = new DataItemImpl("r", 0, 3072);
			baseItem.dataInput(dataInput);
			final var parent = new CompositeDataOperationImpl<DataItem>(
							0, OpType.READ, baseItem, null, "/bucket", null, null, 0, 1024);
			parent.subOperations();
			final var parts = parent.nextSubOperations(3);
			final var a = (PartialDataOperationImpl<DataItem>) parts.get(0);
			final var b = (PartialDataOperationImpl<DataItem>) parts.get(1);
			final var c = (PartialDataOperationImpl<DataItem>) parts.get(2);

			// C: out of retries
			for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
				c.incrementRetryCount();
			}
			c.status(Operation.Status.FAIL_IO);
			parent.markSubTaskCompleted();
			assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) c));
			assertEquals("FAIL_IO", parent.get("mpuFailure"));

			// A: failed with retries left; finishResponse() has run, handleCompleted has not
			a.status(Operation.Status.FAIL_IO);
			parent.markSubTaskCompleted();

			// B: succeeds in the gap
			b.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted();
			assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) b));

			final var queue = childQueueOf(driver);
			assertFalse(queue.contains(parent), "a range is about to retry");

			// A now reaches handleCompleted and retries
			assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) a));
			assertEquals(1, a.retryCount());
			assertFalse(parent.allSubOperationsDone(), "A's retry is outstanding");

			// A's retry succeeds
			a.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted();
			assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) a));
			assertTrue(parent.allSubOperationsDone());
			assertTrue(queue.contains(parent), "the last completion finalizes the read");
		} finally {
			driver.close();
		}
	}

	/** The copy that a failed read's retry or recycle re-enters with has no failure marker. */
	@Test
	void theResultCopyOfACompositeCarriesNoContext() {
		final var item = new DataItemImpl("r", 0, 3072);
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", null, null, 0, 1024);
		parent.put("mpuFailure", "FAIL_IO");
		parent.put("sptFinalizationEnqueued", "true");
		final var copy = parent.result();
		assertNull(copy.get("mpuFailure"));
		assertNull(copy.get("sptFinalizationEnqueued"));
	}
}
