package com.dell.spt.base.item.io;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.file.BinFileInput;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.apache.logging.log4j.Level;

public interface ItemInputFactory {

	static <I extends Item, O extends Operation<I>> Input<I> createItemInput(
					final Config itemConfig, final int batchSize, final StorageDriver<I, O> storageDriver) {
		Input<I> itemInput = null;

		final ItemType itemType = ItemType.valueOf(itemConfig.stringVal("type").toUpperCase(Locale.ROOT));
		@SuppressWarnings("unchecked")
		final ItemFactory<I> itemFactory = (ItemFactory<I>) ItemType.getItemFactory(itemType);
		final Config itemInputConfig = itemConfig.configVal("input");
		final String itemInputFile = itemInputConfig.stringVal("file");

		if (itemInputFile != null && !itemInputFile.isEmpty()) {
			itemInput = createFileItemInput(itemFactory, itemInputFile);
			Loggers.MSG.debug("Using the file \"{}\" as items input", itemInputFile);
		} else {
			final String itemInputPath = itemInputConfig.stringVal("path");
			if (itemInputPath != null && !itemInputPath.isEmpty()) {
				itemInput = createPathItemInput(itemConfig, batchSize, itemFactory, itemInputPath, storageDriver);
				Loggers.MSG.debug("Using the storage path \"{}\" as items input", itemInputPath);
			}
		}

		return itemInput;
	}

	static <I extends Item> Input<I> createFileItemInput(
					final ItemFactory<I> itemFactory, final String itemInputFile) {

		Input<I> fileItemInput = null;

		final Path itemInputFilePath = Paths.get(itemInputFile);
		try {
			if (itemInputFile.endsWith(".csv")) {
				if (IntegrityManifestItemInput.hasCanonicalHeader(itemInputFilePath)) {
					@SuppressWarnings("unchecked")
					final Input<I> integrityInput = (Input<I>) new IntegrityManifestItemInput(itemInputFilePath);
					fileItemInput = integrityInput;
				} else {
					try {
						fileItemInput = new CsvFileItemInput<>(itemInputFilePath, itemFactory);
					} catch (final NoSuchMethodException e) {
						throw new AssertionError(e);
					}
				}
			} else {
				fileItemInput = new BinFileInput<>(itemInputFilePath);
			}
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to open the item input file \"{}\"", itemInputFile);
		}

		return fileItemInput;
	}

	static <I extends Item, O extends Operation<I>> Input<I> createPathItemInput(
					final Config itemConfig,
					final int batchSize,
					final ItemFactory<I> itemFactory,
					final String itemInputPath,
					final StorageDriver<I, O> storageDriver) {
		Input<I> itemInput = null;
		try {
			final var namingConfig = itemConfig.configVal("naming");
			final var prefix = namingConfig.stringVal("prefix");
			final var radix = namingConfig.intVal("radix");
			itemInput = new StorageItemInput<>(
							storageDriver, batchSize, itemFactory, itemInputPath, prefix, radix);
		} catch (final IllegalStateException | IllegalArgumentException e) {
			LogUtil.exception(Level.WARN, e, "Failed to initialize the data input");
		}
		return itemInput;
	}
}
