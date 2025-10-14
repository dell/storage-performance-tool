package com.dell.spt.storage.driver.coop.netty.mock;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriverBase;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.math.Random;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import com.github.akurilov.netty.connection.pool.mock.MultiNodeConnPoolMock;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

public final class NettyStorageDriverMock<I extends Item, O extends Operation<I>>
				extends NettyStorageDriverBase<I, O> {

	private final Random rnd = new Random();

	public NettyStorageDriverMock(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
	}

	protected NonBlockingConnPool createConnectionPool() {
		return new MultiNodeConnPoolMock(
						storageNodeAddrs, bootstrap, this, storageNodePort, connAttemptsLimit);
	}

	@Override
	protected final void sendRequest(final Channel channel, final O op) {

		final OpType opType = op.type();

		try {
			if (OpType.CREATE.equals(opType)) {
				final I item = op.item();
				if (item instanceof DataItem) {
					final DataOperation dataOp = (DataOperation) op;
					if (!(dataOp instanceof CompositeDataOperation)) {
						final DataItem dataItem = (DataItem) item;
						dataOp.countBytesDone(dataItem.size());
					}
				}
			} else if (OpType.UPDATE.equals(opType)) {
				final I item = op.item();
				if (item instanceof DataItem) {

					final DataItem dataItem = (DataItem) item;
					final DataOperation dataOp = (DataOperation) op;

					final List<Range> fixedRanges = dataOp.fixedRanges();
					if (fixedRanges == null || fixedRanges.isEmpty()) {
						dataItem.commitUpdatedRanges(dataOp.markedRangesMaskPair());
					} else { // fixed byte ranges case
						dataItem.size(dataItem.size() + dataOp.markedRangesSize());
					}
					dataOp.countBytesDone(dataOp.markedRangesSize());
				}
			}
		} catch (final IOException ignored) {}

		op.status(Operation.Status.SUCC);
		try {
			reqSentCallback.operationComplete(channel.newSucceededFuture());
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			e.printStackTrace(System.err);
		}
		op.finishRequest();
		op.startResponse();
		complete(channel, op);
	}

	@Override
	protected String requestNewPath(final String path) {
		return path;
	}

	@Override
	protected final String requestNewAuthToken(final Credential credential) {
		return Long.toHexString(rnd.nextLong());
	}

	@Override
	public final List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws IOException {
		return Collections.emptyList();
	}
}
