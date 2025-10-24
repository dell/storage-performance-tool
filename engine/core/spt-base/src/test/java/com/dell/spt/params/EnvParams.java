package com.dell.spt.params;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class EnvParams {

	// prevent the singleton instantiation
	private EnvParams() {}

	public static final List<Object[]> PARAMS = new ArrayList<>();

	static {
		final Map<String, String> env = System.getenv();

		final List<StorageType> includedStorageTypes = new ArrayList<>();
		final String storageTypeValues = env.get(StorageType.KEY_ENV);
		for (int start = 0; start < storageTypeValues.length();) {
			final int separator = storageTypeValues.indexOf(',', start);
			final int end = separator >= 0 ? separator : storageTypeValues.length();
			final String storageTypeValue = storageTypeValues.substring(start, end);
			includedStorageTypes.add(StorageType.valueOf(storageTypeValue.toUpperCase(Locale.ROOT)));
			if (separator < 0) {
				break;
			}
			start = separator + 1;
		}

		final List<RunMode> includedRunModes = new ArrayList<>();
		final String nodeCountValues = env.get(RunMode.KEY_ENV);
		for (int start = 0; start < nodeCountValues.length();) {
			final int separator = nodeCountValues.indexOf(',', start);
			final int end = separator >= 0 ? separator : nodeCountValues.length();
			final String nodeCountValue = nodeCountValues.substring(start, end);
			includedRunModes.add(RunMode.valueOf(nodeCountValue.toUpperCase(Locale.ROOT)));
			if (separator < 0) {
				break;
			}
			start = separator + 1;
		}

		final List<Concurrency> includedConcurrencies = new ArrayList<>();
		final String concurrencyValues = env.get(Concurrency.KEY_ENV);
		for (int start = 0; start < concurrencyValues.length();) {
			final int separator = concurrencyValues.indexOf(',', start);
			final int end = separator >= 0 ? separator : concurrencyValues.length();
			final String concurrencyValue = concurrencyValues.substring(start, end);
			includedConcurrencies.add(Concurrency.valueOf(concurrencyValue.toUpperCase(Locale.ROOT)));
			if (separator < 0) {
				break;
			}
			start = separator + 1;
		}

		final List<ItemSize> includedItemSizes = new ArrayList<>();
		final String itemSizeValues = env.get(ItemSize.KEY_ENV);
		for (int start = 0; start < itemSizeValues.length();) {
			final int separator = itemSizeValues.indexOf(',', start);
			final int end = separator >= 0 ? separator : itemSizeValues.length();
			final String itemSizeValue = itemSizeValues.substring(start, end);
			includedItemSizes.add(ItemSize.valueOf(itemSizeValue.toUpperCase(Locale.ROOT)));
			if (separator < 0) {
				break;
			}
			start = separator + 1;
		}

		for (final StorageType storageType : includedStorageTypes) {
			for (final RunMode runMode : includedRunModes) {
				for (final Concurrency concurrency : includedConcurrencies) {
					for (final ItemSize itemSize : includedItemSizes) {
						PARAMS.add(new Object[]{storageType, runMode, concurrency, itemSize
						});
					}
				}
			}
		}
	}
}
