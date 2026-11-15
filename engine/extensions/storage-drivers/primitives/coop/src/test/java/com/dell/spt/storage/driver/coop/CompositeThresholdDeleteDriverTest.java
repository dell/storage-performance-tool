package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * A cleanup DELETE of an object larger than the part size (item.data.ranges.threshold): the builder
 * must make a plain DELETE, whose one completion is published as an ordinary result. A composite
 * DELETE has no parts, so its result was dropped as an unfinished composite and the step never ended.
 */
@SuppressWarnings("unchecked")
final class CompositeThresholdDeleteDriverTest {

	private static final class D extends CoopStorageDriverMock<Item, Operation<Item>> {
		D(final DataInput in, final Config cfg) throws Exception {
			super("threshold-delete-step", in, cfg, false, 1);
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

	@Test
	void aDeleteAboveThePartSizeIsPublishedOnceAsAPlainResult() throws Exception {
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new D(dataInput, cfg());
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(out);
		try {
			// threshold before op type, as LoadGeneratorBuilderImpl sets them
			final var builder = new DataOperationsBuilderImpl<DataItem, DataOperation<DataItem>>(0)
							.sizeThreshold(1024)
							.opType(OpType.DELETE)
							.inputPath("/bucket");
			final var item = new DataItemImpl("d", 0, 4096);
			item.dataInput(dataInput);
			final List<DataOperation<DataItem>> ops = new ArrayList<>();
			builder.buildOps(List.of(item), ops);
			assertEquals(1, ops.size());
			final var op = (Operation<Item>) (Operation<?>) ops.get(0);

			op.status(Operation.Status.SUCC);
			assertTrue(driver.handleCompleted(op));

			final ArgumentCaptor<Operation<Item>> published = ArgumentCaptor.forClass(Operation.class);
			verify(out, times(1)).put(published.capture());
			assertFalse(published.getValue() instanceof CompositeOperation,
							"a composite DELETE result is dropped by the step as an unfinished composite");
			assertEquals(OpType.DELETE, published.getValue().type());
			assertEquals(Operation.Status.SUCC, published.getValue().status());
			assertTrue(childQueueOf(driver).isEmpty(), "a DELETE has no part operations");
		} finally {
			driver.close();
		}
	}
}
