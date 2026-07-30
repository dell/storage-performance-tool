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
import com.dell.spt.base.concurrent.AsyncRunnable;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.CloseableThreadContext;

/** Core contract for storage drivers that execute load operations. */
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

	/** Returns the maximum concurrent operations supported, or 0 when unlimited. */
	int concurrencyLimit();

	default String driverType() {
		return getClass().getSimpleName();
	}

	default boolean metadataIntegrityEnabled() {
		return false;
	}

	int activeOpCount();

	long scheduledOpCount();

	long completedOpCount();

	boolean isIdle();

	void adjustIoBuffers(final long avgTransferSize, final OpType opType);

	/**
	 * Enable the fast-recycle dispatch path.  When active and the current
	 * active-op count is at or below {@code concurrencyThreshold}, the driver
	 * will re-submit a successfully completed simple operation directly on the
	 * I/O thread instead of returning it through the LoadGenerator recycle queue.
	 * <p>
	 * The default implementation is a no-op; subclasses that support the
	 * optimisation (e.g. Netty-based drivers) override this.
	 *
	 * @param concurrencyThreshold maximum active-op count at which the
	 *                             fast-recycle path is used (0 = disabled)
	 */
	default void enableFastRecycle(final int concurrencyThreshold) {}

	/**
	 * Signal that the fast-recycle path is expected to handle most operations
	 * and the dispatch/generator VTs may quiesce (park on long waits).  Only
	 * called when the configured concurrency is low enough that fast-recycle
	 * dominates; at higher concurrency the normal pipeline needs to stay
	 * responsive.
	 * <p>
	 * Default is a no-op; cooperative drivers override to inform their
	 * dispatch task.
	 */
	default void enableFastRecycleQuiesce() {}

	@Override
	AsyncRunnable stop();

	@Override
	void close() throws IOException;

	/**
	 * Creates a storage driver instance using registered extensions.
	 *
	 * @param extensions the resolved runtime extensions
	 * @param storageConfig storage sub-config (also specifies the particular storage driver type)
	 * @param dataInput the data input used to produce/reproduce the data
	 * @param verifyFlag verify the data on read or not
	 * @param stepId scenario step id for logging purposes
	 * @param <I> item type
	 * @param <O> load operation type
	 * @return the storage driver instance
	 * @throws IllegalArgumentException if load config either storage config is null
	 * @throws InterruptedException may be thrown by a specific storage driver constructor
	 * @throws IllegalConfigurationException if no storage driver implementation was found
	 */
	@SuppressWarnings("unchecked")
	static <I extends Item, O extends Operation<I>> StorageDriver<I, O> instance(
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
							.filter(StorageDriverFactory.class::isInstance)
							.map(factory -> (StorageDriverFactory<I, O, ?>) factory)
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
