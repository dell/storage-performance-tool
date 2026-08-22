package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.TransferConvertBuffer;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.util.NoSuchElementException;

/** Parsed fail-closed engine settings for the standalone DELETE request spine. */
public final class StandaloneDeleteConfig {
	private static final String DELETE_CONFIG_PATH = "op-delete";
	private static final String STANDALONE_KEY = "standalone";
	private static final String BATCH_SIZE_KEY = "batchSize";
	private static final String OUTPUT_FILE_KEY = "output-file";

	private final boolean enabled;
	private final int batchSize;

	private StandaloneDeleteConfig(final boolean enabled, final int batchSize) {
		this.enabled = enabled;
		this.batchSize = batchSize;
	}

	/** Parses the optional standalone DELETE node, preserving disabled compatibility if absent. */
	public static StandaloneDeleteConfig from(final Config loadConfig) {
		if (loadConfig == null) {
			return new StandaloneDeleteConfig(false, 0);
		}
		try {
			final var deleteConfig = loadConfig.configVal(DELETE_CONFIG_PATH);
			if (deleteConfig == null || !deleteConfig.boolVal(STANDALONE_KEY)) {
				return new StandaloneDeleteConfig(false, 0);
			}
			return new StandaloneDeleteConfig(true, deleteConfig.intVal(BATCH_SIZE_KEY));
		} catch (final NoSuchElementException e) {
			// Compatibility with extension-supplied schemas created before this optional node.
			return new StandaloneDeleteConfig(false, 0);
		}
	}

	/** Returns whether the standalone request spine is explicitly enabled. */
	public boolean enabled() {
		return enabled;
	}

	/** Returns the validated target count requested per logical DELETE operation. */
	public int batchSize() {
		return batchSize;
	}

	/** Validates operation-type, item-type, recycle, retry, and cardinality settings. */
	public void validateSettings(
					final OpType opType,
					final ItemType itemType,
					final boolean recycle,
					final boolean retry) {
		if (!enabled) {
			return;
		}
		if (opType != OpType.DELETE || (itemType != null && itemType != ItemType.DATA)) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires load-op-type=delete and item-type=data");
		}
		if (batchSize < 1 || batchSize > DeleteRequest.MAX_TARGET_COUNT) {
			throw new IllegalConfigurationException(
							"load-op-delete-batchSize must be between 1 and "
											+ DeleteRequest.MAX_TARGET_COUNT);
		}
		if (recycle) {
			throw new IllegalConfigurationException("Standalone DELETE does not support recycle mode");
		}
		if (retry) {
			throw new IllegalConfigurationException(
							"Standalone DELETE does not support SPT operation retries");
		}
	}

	/** Validates that the step is terminal and its driver explicitly supports the request type. */
	public void validateTopology(
					final Config itemConfig, final Object itemInput, final Object operationOutput) {
		if (!enabled) {
			return;
		}
		if (itemInput instanceof TransferConvertBuffer<?, ?>) {
			throw new IllegalConfigurationException(
							"Standalone DELETE is terminal and cannot consume a single-item pipeline");
		}
		if (!(itemInput instanceof RemainingItemCountInput<?>)) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires a finite input with an exact remaining-item count");
		}
		final var itemOutputFile = itemConfig.stringVal(OUTPUT_FILE_KEY);
		if (itemOutputFile != null && !itemOutputFile.isEmpty()) {
			throw new IllegalConfigurationException(
							"Standalone DELETE cannot use ordinary successful-item output");
		}
		if (!(operationOutput instanceof StorageDriver<?, ?>)) {
			throw unsupportedDriver();
		}
		final var driver = (StorageDriver<?, ?>) operationOutput;
		if (!driver.supportsStandaloneDeleteRequests()) {
			throw unsupportedDriver();
		}
	}

	private static IllegalConfigurationException unsupportedDriver() {
		return new IllegalConfigurationException(
						"Configured storage driver does not support standalone DELETE requests");
	}
}
