package com.dell.spt.base.storage.driver;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.concurrent.AsyncRunnable;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.CloseableThreadContext;

/** Created on 11.07.16. */
public interface StorageDriver<I extends Item, O extends Operation<I>>
				extends Daemon, Output<O> {

	int BUFF_SIZE_MIN = 0x1_000;
	int BUFF_SIZE_MAX = 0x1_000_000;

	void operationResultOutput(final Output<O> opResultOut);

	List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws IOException;

	default List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count,
					final ListOptions options)
					throws IOException {
		return list(itemFactory, path, prefix, idRadix, lastPrevItem, count);
	}

	@Override
	boolean put(final O op);

	@Override
	int put(final List<O> ops, final int from, final int to);

	@Override
	int put(final List<O> ops);

	/** @return 0 if the concurrency is not limited */
	int concurrencyLimit();

	int activeOpCount();

	long scheduledOpCount();

	long completedOpCount();

	boolean isIdle();

	void adjustIoBuffers(final long avgTransferSize, final OpType opType);

	@Override
	AsyncRunnable stop();

	@Override
	void close() throws IOException;

	/**
	* @param extensions the resolved runtime extensions
	* @param storageConfig storage sub-config (also specifies the particular storage driver type)
	* @param dataInput the data input used to produce/reproduce the data
	* @param verifyFlag verify the data on read or not
	* @param stepId scenario step id for logging purposes
	* @param <I> item type
	* @param <O> load operation type
	* @param <T> storage driver type
	* @return the storage driver instance
	* @throws IllegalArgumentException if load config either storage config is null
	* @throws InterruptedException may be thrown by a specific storage driver constructor
	* @throws IllegalConfigurationException if no storage driver implementation was found
	*/
	@SuppressWarnings("unchecked")
	static <I extends Item, O extends Operation<I>, T extends StorageDriver<I, O>> T instance(
					final List<Extension> extensions,
					final Config storageConfig,
					final DataInput dataInput,
					final boolean verifyFlag,
					final int batchSize,
					final String stepId)
					throws IllegalArgumentException, InterruptedException, IllegalConfigurationException {
		try (final var ctx = CloseableThreadContext.put(KEY_STEP_ID, stepId)
						.put(KEY_CLASS_NAME, StorageDriver.class.getSimpleName())) {

			if (storageConfig == null) {
				throw new IllegalArgumentException("Null storage config");
			}

			final var factories = extensions.stream()
							.filter(ext -> ext instanceof StorageDriverFactory)
							.map(factory -> (StorageDriverFactory<I, O, T>) factory)
							.collect(Collectors.toList());

			final var driverType = storageConfig.stringVal("driver-type");

			final var selectedFactory = factories.stream()
							.filter(factory -> driverType.equals(factory.id()))
							.findFirst()
							.orElseThrow(
											() -> new IllegalConfigurationException(
															"Failed to create the storage driver for the type \""
																			+ driverType
																			+ "\", available types: "
																			+ Arrays.toString(
																							factories.stream().map(StorageDriverFactory::id).toArray())));
			Loggers.MSG.info(
							"{}: creating the storage driver instance for the type \"{}\"", stepId, driverType);

			return selectedFactory.create(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		}
	}
}
