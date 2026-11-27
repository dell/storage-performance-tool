package com.dell.spt.storage.driver.coop.netty.mock;

import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_OPERATION;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A successful UPDATE on the mock records the item's size as the real drivers do: an overwrite
 * keeps it, an append grows it by the appended bytes only, once. A random-range UPDATE commits its
 * ranges once, so a layer switch advances the layer by one.
 */
class NettyStorageDriverMockUpdateSizeTest {

	private static final long ITEM_SIZE = 1024;

	@Test
	void aWholeObjectUpdateKeepsTheSize() throws Exception {
		assertEquals(1024, sizeAfterUpdate());
	}

	@Test
	void aLoneAppendGrowsTheSizeByTheAppend() throws Exception {
		assertEquals(1536, sizeAfterUpdate("-512-"));
	}

	@Test
	void anAppendWithAnOverwriteGrowsTheSizeByTheAppendOnly() throws Exception {
		assertEquals(1536, sizeAfterUpdate("0-99", "-512-"));
	}

	@Test
	void overwriteOnlyRangesKeepTheSize() throws Exception {
		assertEquals(1024, sizeAfterUpdate("0-99", "200-299"));
	}

	@Test
	void aRandomRangeUpdateWithoutALayerSwitchCommitsItsRanges() throws Exception {
		assertRandomRangeUpdate(0, 3, 0, 3);
	}

	@Test
	void aPartialLayerSwitchAdvancesTheLayerOnce() throws Exception {
		assertRandomRangeUpdate(10, 2, 1, 1);
	}

	@Test
	void aFullLayerSwitchAdvancesTheLayerOnce() throws Exception {
		assertRandomRangeUpdate(11, 1, 1, 1);
	}

	/**
	 * Runs one successful random-range UPDATE of {@code count} ranges on a 1 KiB item (11 ranges,
	 * {@code preset} of them recorded as updated) and checks what the item records: the partial
	 * switch (10 recorded, 2 ranges) draws one range in each layer, the full switch (all 11
	 * recorded, 1 range) one in the next layer only.
	 */
	private static void assertRandomRangeUpdate(
					final int preset, final int count, final int layer, final int updatedRanges) throws Exception {
		final var dataInput = dataInput();
		final var item = new DataItemImpl("obj", 0, ITEM_SIZE);
		item.dataInput(dataInput);
		if (preset > 0) {
			final var mask = new BitSet[]{new BitSet(), new BitSet()
			};
			mask[0].set(0, preset);
			item.commitUpdatedRanges(mask);
		}
		final var op = runUpdate(dataInput, item, null, count);
		if (preset == 10) {
			assertFalse(op.markedRangesMaskPair()[0].isEmpty(), "one range in the current layer");
			assertFalse(op.markedRangesMaskPair()[1].isEmpty(), "one range in the next layer");
		} else if (preset == 11) {
			assertTrue(op.markedRangesMaskPair()[0].isEmpty(), "no range in the current layer");
		}
		assertEquals(layer, item.layer(), "layer");
		int updated = 0;
		for (int i = 0; i < 11; i++) {
			if (item.isRangeUpdated(i)) {
				updated++;
			}
		}
		assertEquals(updatedRanges, updated, "ranges recorded as updated in the layer");
		assertEquals(ITEM_SIZE, item.size());
	}

	private static DataInput dataInput() throws Exception {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	/** Runs one successful UPDATE of a 1 KiB item through the mock and returns its recorded size. */
	private static long sizeAfterUpdate(final String... ranges) throws Exception {
		final var dataInput = dataInput();
		final var item = new DataItemImpl("obj", 0, ITEM_SIZE);
		item.dataInput(dataInput);
		final List<Range> fixedRanges = ranges.length == 0 ? null : Arrays.stream(ranges).map(Range::new).toList();
		runUpdate(dataInput, item, fixedRanges, 0);
		return item.size();
	}

	/** Runs one UPDATE of the item through the mock, asserts it succeeded and returns it. */
	private static DataOperationImpl<DataItemImpl> runUpdate(
					final DataInput dataInput, final DataItemImpl item, final List<Range> fixedRanges, final int randomRanges)
					throws Exception {
		try (final var driver = new NettyStorageDriverMock<DataItemImpl, DataOperation<DataItemImpl>>(
						"netty-mock-update-size", dataInput, NettyStorageDriverMockDispatchTest.storageConfig(), false, 1)) {
			@SuppressWarnings("unchecked")
			final Output<DataOperation<DataItemImpl>> results = mock(Output.class);
			when(results.put(any(DataOperation.class))).thenReturn(true);
			driver.operationResultOutput(results);

			final var op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, "/bucket", null, fixedRanges, randomRanges);
			op.reset();

			final var channel = new EmbeddedChannel();
			try {
				// Bind the operation to the channel as a lease does, the transport held.
				channel.attr(ATTR_KEY_NODE).set("127.0.0.1");
				channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
				channel.attr(ATTR_KEY_OPERATION).set(op);
				op.startRequest();
				driver.sendRequest(channel, op);
			} finally {
				channel.finishAndReleaseAll();
			}

			assertEquals(Operation.Status.SUCC, op.status());
			return op;
		}
	}
}
