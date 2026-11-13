package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.item.naming.ItemNameInput.ItemNamingType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.el.ExpressionInput;
import com.github.akurilov.commons.math.MathUtil;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.apache.logging.log4j.Level;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import static com.dell.spt.base.config.el.Language.withLanguage;
import static com.dell.spt.base.item.naming.ItemNameInput.ItemNamingType.SERIAL;

public interface ConfigSliceUtil {

	static Config initSlice(final Config config) {
		final Config configSlice = new BasicConfig(config);
		final long runId = config.longVal("run-id");
		if (runId > 0L) {
			configSlice.val("run-id", runId);
		}
		// disable the distributed mode on the slave nodes
		configSlice.val("load-step-node-addrs", Collections.EMPTY_LIST);
		return configSlice;
	}

	static void sliceLongValue(
					final long configVal, final List<Config> configSlices, final String configPath) {
		final var sliceCount = configSlices.size();
		final var configValPerSlice = (long) Math.ceil(((double) configVal) / sliceCount);
		var remainingVal = configVal;
		for (var i = 0; i < sliceCount; i++) {
			final var configSlice = configSlices.get(i);
			if (remainingVal > configValPerSlice) {
				Loggers.MSG.debug("Config slice #{}: {} = {}", i, configPath, configValPerSlice);
				configSlice.val(configPath, configValPerSlice);
				remainingVal -= configValPerSlice;
			} else {
				Loggers.MSG.debug("Config slice #{}: {} = {}", i, configPath, remainingVal);
				configSlice.val(configPath, remainingVal);
				remainingVal = 0;
			}
		}
	}

	/**
	 * Slices a count limit, which a slice reads as no limit at 0. The split of
	 * {@link #sliceLongValue} stays wherever it gives every slice a count, so each of those slices
	 * keeps the count it had; where it would leave a slice at 0, the count is split as evenly as it
	 * goes. A count below the slice count leaves a slice at 0 either way, so the caller refuses it.
	 */
	static void sliceCountLimit(
					final long countLimit, final List<Config> configSlices, final String configPath) {
		sliceLongValue(countLimit, configSlices, configPath);
		if (configSlices.stream().anyMatch(configSlice -> configSlice.longVal(configPath) == 0)) {
			Loggers.MSG.debug("{} = {} leaves a slice at 0 when split by ceiling, so it is split evenly", configPath, countLimit);
			sliceLongValueBalanced(countLimit, configSlices, configPath);
		}
	}

	static void sliceLongValueBalanced(
					final long configVal, final List<Config> configSlices, final String configPath) {
		final var sliceCount = configSlices.size();
		final var configValPerSlice = configVal / sliceCount;
		final var remainder = configVal % sliceCount;
		for (var i = 0; i < sliceCount; i++) {
			final var configSlice = configSlices.get(i);
			final var sliceVal = configValPerSlice + (i < remainder ? 1 : 0);
			Loggers.MSG.debug("Config slice #{}: {} = {}", i, configPath, sliceVal);
			configSlice.val(configPath, sliceVal);
		}
	}

	static void sliceDoubleValue(
					final double configVal, final List<Config> configSlices, final String configPath) {
		final var sliceCount = configSlices.size();
		final var configValPerSlice = configVal / sliceCount;
		for (var i = 0; i < sliceCount; i++) {
			final var configSlice = configSlices.get(i);
			Loggers.MSG.debug("Config slice #{}: {} = {}", i, configPath, configValPerSlice);
			configSlice.val(configPath, configValPerSlice);
		}
	}

	static void sliceStorageNodeAddrs(
					final List<Config> configSlices, final List<String> storageNodeAddrs) {
		final var sliceCount = configSlices.size();
		final var storageNodeCount = storageNodeAddrs.size();
		if (storageNodeCount > 1) {
			final var gcd = MathUtil.gcd(sliceCount, storageNodeCount);
			final var sliceCountPerGcd = sliceCount / gcd;
			final var storageNodeCountPerGcd = storageNodeCount / gcd;
			for (var i = 0; i < gcd; i++) {
				final var storageNodeAddrsSlice = (Collection<String>) new ArrayList<String>(storageNodeCountPerGcd);
				for (var j = storageNodeCountPerGcd * i; j < storageNodeCountPerGcd * (i + 1); j++) {
					storageNodeAddrsSlice.add(storageNodeAddrs.get(j));
				}
				for (var j = i * sliceCountPerGcd; j < (i + 1) * sliceCountPerGcd; j++) {
					Loggers.MSG.info(
									"Load step slice #{} ({}) storage nodes: {}",
									j,
									(j == 0 ? "local" : "remote"),
									Arrays.toString(storageNodeAddrsSlice.toArray()));
					configSlices.get(j).val("storage-net-node-addrs", storageNodeAddrsSlice);
				}
			}
		}
	}

	/**
	 * Gives each slice its host override as its {@code storage.net.interfaces}, and removes the map
	 * from every slice: a slice has no node list to check the keys against. Slice 0 is the Entry
	 * Node. The selection is by address, so a prefix of the configured nodes changes no other slice.
	 *
	 * @param nodeAddrs the participating remote nodes, in slice order, starting with slice 1
	 */
	static void sliceNodeInterfaces(
					final Config config, final List<Config> configSlices, final List<String> nodeAddrs) {
		final var overrides = NicSetConfig.nodeInterfaces(config);
		if (overrides.isEmpty()) {
			return;
		}
		final var stepValue = NicSetConfig.interfaces(config.configVal("storage"));
		for (var i = 0; i < configSlices.size(); i++) {
			final var configSlice = configSlices.get(i);
			final var node = i == 0 ? NicSetConfig.ENTRY_KEY : nodeAddrs.get(i - 1);
			final var override = overrides.get(node);
			if (override != null) {
				configSlice.val(NicSetConfig.SLICE_INTERFACES_PATH, new ArrayList<>(override));
			}
			configSlice.val(NicSetConfig.SLICE_NODE_INTERFACES_PATH, new HashMap<String, Object>());
			final var effective = override != null ? override : stepValue;
			Loggers.MSG.info(
							"Load step slice #{} ({}) interfaces: {} ({})",
							i,
							node,
							effective.isEmpty() ? NicSetConfig.NONE : String.join(",", effective),
							override != null ? "host override" : "step");
		}
	}

	static void sliceItemNaming(final List<Config> configSlices) {
		if (configSlices == null || configSlices.isEmpty()) {
			Loggers.MSG.debug("Item naming slicing skipped: no config slices provided");
			return;
		}
		try {
			final var namingConfig = configSlices.get(0).configVal("item-naming");
			final var namingType = ItemNamingType.valueOf(namingConfig.stringVal("type").toUpperCase(Locale.ROOT));
			if (SERIAL.equals(namingType)) {
				final var sliceCount = configSlices.size();
				final var srcNamingSeedRaw = namingConfig.val("seed");
				long srcNamingSeed;
				try {
					srcNamingSeed = TypeUtil.typeConvert(srcNamingSeedRaw, long.class);
				} catch (final ClassCastException | NumberFormatException e) {
					if (srcNamingSeedRaw instanceof String) {
						try (
										final var in = withLanguage(ExpressionInput.builder())
														.expression((String) srcNamingSeedRaw)
														.<ExpressionInput<Long>> build()) {
							srcNamingSeed = in.get();
						}
					} else {
						throw new Exception(
										"Item naming seed value (" + srcNamingSeedRaw + ") should be an integer either expression");
					}
				}
				final var srcNamingStep = namingConfig.intVal("step");
				final var namingStepPerSlice = Math.toIntExact((long) srcNamingStep * sliceCount);
				for (var i = 0; i < sliceCount; i++) {
					final var configSlice = configSlices.get(i);
					final long namingOffset = (long) i * srcNamingStep;
					Loggers.MSG.debug(
									"Item naming slicing: slice #{}, seed: {}, step: {}", i, srcNamingSeed + namingOffset,
									namingStepPerSlice);
					configSlice.val("item-naming-seed", srcNamingSeed + namingOffset);
					configSlice.val("item-naming-step", namingStepPerSlice);
				}
			}
		} catch (final NoSuchElementException e) {
			Loggers.MSG.debug("Item naming slicing skipped: item-naming configuration missing", e);
		} catch (final Exception e) {
			LogUtil.exception(Level.ERROR, e, "Item naming slicing failure");
		}
	}
}
