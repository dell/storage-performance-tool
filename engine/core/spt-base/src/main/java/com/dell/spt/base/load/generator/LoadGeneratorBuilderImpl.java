package com.dell.spt.base.load.generator;

import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.item.DataItem.rangeCount;
import static com.dell.spt.base.storage.driver.StorageDriver.BUFF_SIZE_MIN;
import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.config.el.CompositeExpressionInputBuilder;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.TransferConvertBuffer;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.io.ListPathItemInput;
import com.dell.spt.base.item.io.ShardedListPathItemInput;
import com.dell.spt.base.item.naming.ItemNameInputFactory;
import com.dell.spt.base.item.io.NewDataItemInput;
import com.dell.spt.base.item.io.NewItemInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.data.DataOperationsBuilder;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestAssembler;
import com.dell.spt.base.item.op.deletion.StandaloneDeleteConfig;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.list.shard.ListShardingConfig;
import com.dell.spt.base.item.op.list.shard.ListShardingContext;
import com.dell.spt.base.item.op.list.shard.ListShardingMode;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorderImpl;
import com.dell.spt.base.item.op.path.PathOperationsBuilderImpl;
import com.dell.spt.base.item.op.token.TokenOperationsBuilderImpl;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.concurrent.throttle.IndexThrottle;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;

/** Created by andrey on 12.11.16. */
public class LoadGeneratorBuilderImpl<I extends Item, O extends Operation<I>, T extends LoadGeneratorImpl<I, O>>
				implements LoadGeneratorBuilder<I, O, T> {

	private Config itemConfig = null;
	private Config loadConfig = null;
	private ItemType itemType = null;
	private ItemFactory<I> itemFactory = null;
	private Config authConfig = null;
	private Output<O> opOutput = null;
	private Input<I> itemInput = null;
	private long sizeEstimate = -1;
	private int batchSize = -1;
	private int originIndex = -1;
	private final List<Object> throttles = new ArrayList<>();
	private ListShardingContext listShardingContext;

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> itemConfig(final Config itemConfig) {
		this.itemConfig = itemConfig;
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> loadConfig(final Config loadConfig) {
		this.loadConfig = loadConfig;
		this.batchSize = loadConfig.intVal("batch-size");
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> itemType(final ItemType itemType) {
		this.itemType = itemType;
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> itemFactory(final ItemFactory<I> itemFactory) {
		this.itemFactory = itemFactory;
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> authConfig(final Config authConfig) {
		this.authConfig = authConfig;
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> loadOperationsOutput(final Output<O> opOutput) {
		this.opOutput = opOutput;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public LoadGeneratorBuilderImpl<I, O, T> itemInput(final Input<I> itemInput) {
		this.itemInput = itemInput;
		// pipeline transfer buffer is not resettable
		if (!(itemInput instanceof TransferConvertBuffer)) {
			final var opType = OpType.valueOf(loadConfig.stringVal("op-type").toUpperCase(Locale.ROOT));
			// DELETE and STAT transfer 0 bytes — skip size estimation (also avoids blocking on
			// queue-backed inputs that are empty at init time, e.g. MixedLoad's DELETE queue)
			if (OpType.DELETE != opType && OpType.STAT != opType) {
				sizeEstimate = estimateTransferSize(null, opType, (Input<DataItem>) itemInput);
			}
		}
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> originIndex(final int originIndex) {
		this.originIndex = originIndex;
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> addThrottle(final Throttle throttle) {
		throttles.add(throttle);
		return this;
	}

	@Override
	public LoadGeneratorBuilderImpl<I, O, T> addThrottle(final IndexThrottle throttle) {
		throttles.add(throttle);
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T build() throws IllegalConfigurationException {
		// prepare
		listShardingContext = null;
		final OperationsBuilder<I, O> opsBuilder;
		if (loadConfig == null) {
			throw new IllegalConfigurationException("Load config is not set");
		}
		final var opConfig = loadConfig.configVal("op");
		final var countLimit = opConfig.longVal("limit-count");
		final var shuffleFlag = opConfig.boolVal("shuffle");
		if (itemConfig == null) {
			throw new IllegalConfigurationException("Item config is not set");
		}
		final var inputConfig = itemConfig.configVal("input");
		final var namingConfig = itemConfig.configVal("naming");
		final var rangesConfig = itemConfig.configVal("data-ranges");
		if (itemType == null) {
			throw new IllegalConfigurationException("Item type is not set");
		}
		if (originIndex < 0) {
			throw new IllegalConfigurationException("No origin index is set");
		}
		// init the op builder
		if (ItemType.DATA.equals(itemType)) {
			final var fixedRangesConfig = rangesConfig.<String> listVal("fixed");
			final List<Range> fixedRanges;
			if (fixedRangesConfig == null) {
				fixedRanges = Collections.EMPTY_LIST;
			} else {
				fixedRanges = fixedRangesConfig.stream().map(Range::new).collect(Collectors.toList());
			}
			final long sizeThreshold;
			final var sizeThresholdRaw = rangesConfig.val("threshold");
			if (sizeThresholdRaw instanceof String) {
				sizeThreshold = BinarySizeFormat.parseFixedSize((String) sizeThresholdRaw);
			} else {
				sizeThreshold = TypeUtil.typeConvert(sizeThresholdRaw, long.class);
			}
			if (sizeThreshold > 0 && batchSize > 1) {
				Loggers.MSG.warn(
								"Multipart upload threshold is set ({} bytes) but batch size is {}. "
												+ "Cooperative storage drivers apply MPU scheduling and bounded child-operation "
												+ "backpressure; load.batch.size=1 remains a conservative troubleshooting setting.",
								sizeThreshold, batchSize);
			}
			opsBuilder = (OperationsBuilder<I, O>) new DataOperationsBuilderImpl(originIndex)
							.fixedRanges(fixedRanges)
							.randomRangesCount(rangesConfig.intVal("random"))
							.sizeThreshold(sizeThreshold);
		} else if (ItemType.PATH.equals(itemType)) {
			opsBuilder = (OperationsBuilder<I, O>) new PathOperationsBuilderImpl(originIndex);
		} else {
			opsBuilder = (OperationsBuilder<I, O>) new TokenOperationsBuilderImpl(originIndex);
		}
		// determine the operations type
		final var opType = OpType.valueOf(opConfig.stringVal("type").toUpperCase(Locale.ROOT));
		opsBuilder.opType(opType);
		ListShardingConfig shardingConfig = null;
		if (opType == OpType.LIST && opsBuilder instanceof PathOperationsBuilderImpl) {
			configureListOptions(opConfig, (PathOperationsBuilderImpl<?, ?>) opsBuilder);
			shardingConfig = ListShardingConfig.parse(opConfig.configVal("list"));
		}
		// determine the input path
		var itemInputPath = inputConfig.stringVal("path");
		if (itemInputPath != null && itemInputPath.indexOf('/') != 0) {
			itemInputPath = '/' + itemInputPath;
		}
		opsBuilder.inputPath(itemInputPath);
		// determine the output path
		final Input<String> outputPathSupplier;
		if (OpType.CREATE.equals(opType) && ItemType.DATA.equals(itemType)) {
			outputPathSupplier = getOutputPathSupplier();
		} else {
			outputPathSupplier = null;
		}
		opsBuilder.outputPathInput(outputPathSupplier);
		// init the credentials, multi-user case support
		if (authConfig == null) {
			throw new IllegalConfigurationException("Storage auth config is not set");
		}
		final var authFile = authConfig.stringVal("file");
		if (authFile != null && !authFile.isEmpty()) {
			final var credentials = loadCredentialsByPath(authFile, (long) M);
			opsBuilder.credentialsByPath(credentials);
		} else {
			final var uid = authConfig.stringVal("uid");
			final var secret = authConfig.stringVal("secret");
			if (null == uid && null == secret) {
				opsBuilder.credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
			} else {
				opsBuilder.credentialInput(
								new ConstantValueInputImpl<>(Credential.getInstance(uid, secret)));
			}
		}
		// init the items input
		final var itemInputFile = inputConfig.stringVal("file");
		final var listPathWorkload = ItemType.PATH.equals(itemType) && OpType.LIST.equals(opType);
		ListShardingContext shardingContext = null;
		List<ListShard> staticShards = null;
		ListShardMetricsRecorder metricsRecorder = ListShardMetricsRecorder.NO_OP;
		if (itemInput == null) {
			if (listPathWorkload && (itemInputFile == null || itemInputFile.isEmpty())) {
				final var prefix = namingConfig.stringVal("prefix");
				@SuppressWarnings("unchecked")
				final var pathItemFactory = (ItemFactory<? extends PathItem>) itemFactory;

				// Determine runtime concurrency (0 means unlimited/unknown)
				int concurrency = 0;
				if (opOutput instanceof StorageDriver) {
					concurrency = ((StorageDriver<I, O>) opOutput).concurrencyLimit();
				}

				final var mode = shardingConfig == null ? ListShardingMode.AUTO : shardingConfig.mode();

				// AUTO: pick STATIC for multi-threaded, NONE for single-threaded
				if (mode == ListShardingMode.STATIC
								|| (mode == ListShardingMode.AUTO && concurrency > 1)) {
					// AUTO seeding: try delimiter-first using driver probe if available, else Base62 fallback
					staticShards = seedListShards(opConfig, shardingConfig, prefix, itemInputPath, concurrency, opOutput);
					metricsRecorder = new ListShardMetricsRecorderImpl(
									(shardingConfig != null ? shardingConfig.stallTimeout() : java.time.Duration.ofSeconds(30)),
									(shardingConfig != null ? shardingConfig.progressLogInterval() : java.time.Duration.ofSeconds(10)),
									(shardingConfig != null ? shardingConfig.splitLogRate() : 1),
									(shardingConfig != null && shardingConfig.summaryEnabled()),
									(shardingConfig != null ? shardingConfig.splitPages() : 50),
									(shardingConfig != null ? shardingConfig.delimiters() : "/-_."));
					shardingContext = new ListShardingContext(
									staticShards,
									metricsRecorder,
									(shardingConfig != null ? shardingConfig.adaptive() : ListShardingConfig.AdaptiveHeuristicsConfig.defaults()));
					itemInput = (Input<I>) new ShardedListPathItemInput<>(pathItemFactory, staticShards);
				} else {
					// NONE or AUTO with single-threaded: emit a single seed to avoid duplicate enumeration
					if (mode == ListShardingMode.NONE && concurrency > 1) {
						Loggers.MSG.warn("LIST sharding mode=none with concurrency={} will duplicate results; "
										+ "using a single seed to preserve correctness. Consider enabling static sharding.",
										concurrency);
					}
					final int seedCount = 1; // force single seed for correctness
					itemInput = (Input<I>) new ListPathItemInput<>(pathItemFactory, prefix, seedCount);
				}
			} else if ((itemInputFile == null || itemInputFile.isEmpty())
							&& (itemInputPath == null || itemInputPath.isEmpty())) {
				itemInput = newItemInput();
			} else if (opOutput instanceof StorageDriver) {
				itemInput = ItemInputFactory.createItemInput(itemConfig, batchSize, (StorageDriver<I, O>) opOutput);
			}
			if (itemInput == null) {
				throw new IllegalConfigurationException("No item input available");
			}
			if (ItemType.DATA.equals(itemType)) {
				sizeEstimate = estimateTransferSize(
								(DataOperationsBuilder) opsBuilder,
								opsBuilder.opType(),
								(Input<DataItem>) itemInput);
			} else {
				sizeEstimate = BUFF_SIZE_MIN;
			}
		}
		if (shardingContext == null && staticShards != null) {
			if (metricsRecorder == ListShardMetricsRecorder.NO_OP) {
				final var stallTimeout = shardingConfig != null
								? shardingConfig.stallTimeout()
								: java.time.Duration.ofSeconds(30);
				final var progressInterval = shardingConfig != null
								? shardingConfig.progressLogInterval()
								: java.time.Duration.ofSeconds(10);
				final int splitRate = shardingConfig != null ? shardingConfig.splitLogRate() : 1;
				final boolean summaryFlag = shardingConfig != null && shardingConfig.summaryEnabled();
				metricsRecorder = new ListShardMetricsRecorderImpl(
								stallTimeout,
								progressInterval,
								splitRate,
								summaryFlag,
								(shardingConfig != null ? shardingConfig.splitPages() : 50),
								(shardingConfig != null ? shardingConfig.delimiters() : "/-_."));
			}
			final var adaptiveConfig = shardingConfig != null
							? shardingConfig.adaptive()
							: ListShardingConfig.AdaptiveHeuristicsConfig.defaults();
			shardingContext = new ListShardingContext(staticShards, metricsRecorder, adaptiveConfig);
		}
		if (shardingContext != null && opsBuilder instanceof PathOperationsBuilderImpl) {
			((PathOperationsBuilderImpl<?, ?>) opsBuilder).listShardingContext(shardingContext);
		}
		listShardingContext = shardingContext;
		// check for the copy mode
		if (OpType.CREATE.equals(opType)
						&& ItemType.DATA.equals(itemType)
						&& !(itemInput instanceof NewItemInput)) {
			// intercept the items input for the storage side concatenation support
			final var itemDataRangesConcatConfig = rangesConfig.stringVal("concat");
			if (itemDataRangesConcatConfig != null) {
				final var srcItemsCountRange = new Range(itemDataRangesConcatConfig);
				final var srcItemsCountMin = srcItemsCountRange.getBeg();
				final var srcItemsCountMax = srcItemsCountRange.getEnd();
				if (srcItemsCountMin < 0) {
					throw new IllegalConfigurationException(
									"Source data items count min value should be more than 0");
				}
				if (srcItemsCountMax == 0 || srcItemsCountMax < srcItemsCountMin) {
					throw new IllegalConfigurationException(
									"Source data items count max value should be more than 0 and not less than min value");
				}
				final List<I> srcItemsBuff = new ArrayList<>((int) M);
				final int srcItemsCount;
				try {
					srcItemsCount = loadSrcItems(itemInput, srcItemsBuff, (int) M);
				} finally {
					try {
						itemInput.close();
					} catch (final Exception e) {
						Loggers.MSG.warn("Failed to close itemInput after loading concat source items; continuing with cleanup", e);
					}
				}
				// shoot the foot
				if (srcItemsCount == 0) {
					throw new IllegalConfigurationException(
									"Available source items count " + srcItemsCount + " should be more than 0");
				}
				if (srcItemsCount < srcItemsCountMin) {
					throw new IllegalConfigurationException(
									"Available source items count "
													+ srcItemsCount
													+ " is less than configured min "
													+ srcItemsCountMin);
				}
				if (srcItemsCount < srcItemsCountMax) {
					throw new IllegalConfigurationException(
									"Available source items count "
													+ srcItemsCount
													+ " is less than configured max "
													+ srcItemsCountMax);
				}
				// it's safe to cast to int here because the values will not be more than
				// srcItemsCount which is not more than the integer limit
				((DataOperationsBuilder) opsBuilder)
								.srcItemsCount((int) srcItemsCountMin, (int) srcItemsCountMax);
				((DataOperationsBuilder) opsBuilder).srcItemsForConcat(srcItemsBuff);
				itemInput = newItemInput();
			}
		}
		// adjust the storage drivers for the estimated transfer size
		if (opOutput == null) {
			throw new IllegalConfigurationException("Load operations output is not set");
		}
		if (sizeEstimate > 0 && ItemType.DATA.equals(itemType) && opOutput instanceof StorageDriver) {
			((StorageDriver) opOutput).adjustIoBuffers(sizeEstimate, opType);
		}
		final var recycleConfig = opConfig.configVal("recycle");
		final var recycleFlag = listPathWorkload || recycleConfig.boolVal("mode");
		final var retryFlag = opConfig.boolVal("retry");
		final var standaloneDelete = StandaloneDeleteConfig.from(loadConfig);
		standaloneDelete.validateSettings(opType, itemType, recycleFlag, retryFlag);
		standaloneDelete.validateTopology(itemConfig, itemInput, opOutput);
		final var recycleLimit = opConfig.intVal("limit-recycle");
		if (recycleLimit < 1) {
			throw new IllegalConfigurationException("Recycle limit should be > 0");
		}
		// Note: load-op-retry is deliberately *not* OR'd into recycleFlag here (it used to
		// be, to make the generator keep polling for late-arriving retries after item input
		// exhaustion) - LoadGeneratorImpl now has its own dedicated, always-drained retry
		// queue via LoadGenerator#retry that doesn't depend on recycle-mode at all, so a
		// retry-only (non-recycle) workload's generator can correctly signal
		// itemInputFinished once its (genuinely finite) input is exhausted. retryFlag is
		// still passed through separately, though: see LoadGeneratorImpl's own retryFlag
		// constructor javadoc for the different (countLimit-self-stop-related) reason it
		// still needs to know.
		if (standaloneDelete.enabled()) {
			return (T) new LoadGeneratorImpl(
							(Input) itemInput,
							new DeleteRequestAssembler((OperationsBuilder) opsBuilder, standaloneDelete.batchSize()),
							throttles,
							(Output) opOutput,
							batchSize,
							countLimit,
							recycleLimit,
							false,
							shuffleFlag,
							false);
		}
		return (T) new LoadGeneratorImpl<>(
						itemInput,
						opsBuilder,
						throttles,
						opOutput,
						batchSize,
						countLimit,
						recycleLimit,
						recycleFlag,
						shuffleFlag,
						retryFlag);
	}

	@Override
	public ListShardMetricsRecorder listShardMetricsRecorder() {
		return listShardingContext == null
						? ListShardMetricsRecorder.NO_OP
						: listShardingContext.metricsRecorder();
	}

	private static void configureListOptions(
					final Config opConfig, final PathOperationsBuilderImpl<?, ?> opsBuilder) {
		final var listConfig = opConfig.configVal("list");
		if (listConfig == null) {
			opsBuilder.listOptions(ListOptions.DEFAULT);
			return;
		}
		final var builder = ListOptions.builder();
		final var delimiter = listConfig.stringVal("delimiter");
		if (delimiter != null) {
			builder.delimiter(delimiter);
		}
		builder.fetchMetadata(listConfig.boolVal("fetch_metadata"));
		builder.includeVersions(listConfig.boolVal("include_versions"));
		builder.maxKeys(listConfig.intVal("max_keys"));
		opsBuilder.listOptions(builder.build());
	}

	/**
	 * Select seed shards using delimiter-first discovery when it proves a complete partition.
	 * Integrity discovery falls back to one exact-prefix shard whenever delimiter evidence is
	 * incomplete; ordinary LIST workloads retain the legacy Base62 fallback.
	 */
	List<ListShard> seedListShards(
					final Config opConfig,
					final ListShardingConfig shardingConfig,
					final String seedPrefix,
					final String bucketPath,
					final int concurrency,
					final Output<O> opOutput) throws IllegalConfigurationException {
		final ListShardingConfig effectiveCfg = shardingConfig != null ? shardingConfig : ListShardingConfig.parse(opConfig.configVal("list"));
		final String delimiters = effectiveCfg.delimiters();
		List<ListShard> seeds = null;
		int bestCount = -1;
		String bestDelimiter = null;
		List<String> bestPrefixes = java.util.Collections.emptyList();
		final boolean integrityDiscovery = opOutput instanceof StorageDriver<?, ?> storageDriver
						&& storageDriver.metadataIntegrityEnabled();
		final Config listConfig = opConfig.configVal("list");
		final boolean allVersionIntegrityDiscovery = integrityDiscovery
						&& listConfig != null
						&& listConfig.boolVal("include_versions");
		if (allVersionIntegrityDiscovery) {
			Loggers.MSG.info(
							"LIST all-version discovery uses one exact-prefix shard to preserve completeness");
			return List.of(new ListShard(seedPrefix == null ? "" : seedPrefix, null, null, null));
		}
		if (opOutput instanceof ListDiscoveryProbe probe) {
			for (int i = 0; i < delimiters.length(); i++) {
				final String d = String.valueOf(delimiters.charAt(i));
				try {
					final var result = probe.probeCommonPrefixes(bucketPath, seedPrefix == null ? "" : seedPrefix, d, 1000);
					if (integrityDiscovery) {
						requireCommonPrefixesWithinRoot(result.commonPrefixes(), seedPrefix);
					}
					final int count = result.commonPrefixes().size();
					final boolean completePartition = !result.hasContents() && !result.truncated();
					if ((!integrityDiscovery || completePartition) && count > bestCount) {
						bestCount = count;
						bestDelimiter = d;
						bestPrefixes = result.commonPrefixes();
					}
				} catch (final IOException e) {
					LogUtil.exception(Level.WARN, e, "Delimiter probe failure for \"{}\"", d);
				}
			}
		}
		if (bestCount >= concurrency && !bestPrefixes.isEmpty()) {
			final var shardList = new java.util.ArrayList<ListShard>(bestPrefixes.size());
			for (final var p : bestPrefixes) {
				// Enumerate children directly under the discovered prefix; no delimiter for counting
				shardList.add(new ListShard(p, null, null, null));
			}
			Loggers.MSG.info(
							"LIST seeding via delimiter '{}' produced {} prefixes (target concurrency={})",
							bestDelimiter,
							bestCount,
							concurrency);
			seeds = shardList;
		} else if (integrityDiscovery) {
			seeds = List.of(new ListShard(seedPrefix == null ? "" : seedPrefix, null, null, null));
			Loggers.MSG.info(
							"LIST seeding via exact-prefix integrity fallback produced one completeness-preserving shard");
		} else {
			seeds = effectiveCfg.createStaticShards(seedPrefix);
			Loggers.MSG.info(
							"LIST seeding via Base62 fallback produced {} shards (radix={})",
							seeds.size(),
							effectiveCfg.radix());
		}
		return seeds;
	}

	private static void requireCommonPrefixesWithinRoot(
					final List<String> prefixes, final String seedPrefix)
					throws IllegalConfigurationException {
		final String requestedPrefix = canonicalListPrefix(seedPrefix);
		for (final String prefix : prefixes) {
			if (prefix == null || prefix.isEmpty() || !prefix.startsWith(requestedPrefix)) {
				throw new IllegalConfigurationException(
								"LIST delimiter response prefix is outside requested prefix \""
												+ requestedPrefix + "\": " + prefix);
			}
		}
	}

	private static String canonicalListPrefix(final String prefix) {
		if (prefix == null || prefix.isEmpty() || "/".equals(prefix)) {
			return "";
		}
		return prefix.startsWith("/") ? prefix.substring(1) : prefix;
	}

	private static long estimateTransferSize(
					final DataOperationsBuilder dataOpBuilder,
					final OpType opType,
					final Input<DataItem> itemInput) {
		var sizeThreshold = 0L;
		var randomRangesCount = 0;
		List<Range> fixedRanges = null;
		if (dataOpBuilder != null) {
			sizeThreshold = dataOpBuilder.sizeThreshold();
			randomRangesCount = dataOpBuilder.randomRangesCount();
			fixedRanges = dataOpBuilder.fixedRanges();
		}
		var itemSize = 0L;
		final var maxCount = 0x100;
		final var items = (List<DataItem>) new ArrayList<DataItem>(maxCount);
		var n = 0;
		try {
			while (n < maxCount) {
				n += itemInput.get(items, maxCount - n);
			}
		} catch (final Exception e) {
			if (e instanceof IOException) {
				if (!(e instanceof EOFException)) {
					LogUtil.exception(Level.WARN, e, "Failed to estimate the average data item size");
				}
			} else {
				throw e;
			}
		}
		try {
			itemInput.reset();
		} catch (final Exception e) {
			if (e instanceof IOException) {
				LogUtil.exception(Level.WARN, e, "Failed to reset the items input");
			} else {
				throwUnchecked(e);
			}
		}
		var sumSize = 0L;
		var minSize = Long.MAX_VALUE;
		var maxSize = Long.MIN_VALUE;
		long nextSize;
		if (n > 0) {
			try {
				for (var i = 0; i < n; i++) {
					nextSize = items.get(i).size();
					sumSize += nextSize;
					if (nextSize < minSize) {
						minSize = nextSize;
					}
					if (nextSize > maxSize) {
						maxSize = nextSize;
					}
				}
			} catch (final IOException e) {
				throwUnchecked(e);
			}
			itemSize = minSize == maxSize ? sumSize / n : (minSize + maxSize) / 2;
		}
		switch (opType) {
		case CREATE:
			return Math.min(itemSize, sizeThreshold);
		case READ:
		case UPDATE:
			if (itemSize > 0 && randomRangesCount > 0) {
				return itemSize * randomRangesCount / rangeCount(itemSize);
			} else if (fixedRanges != null && !fixedRanges.isEmpty()) {
				long sizeSum = 0;
				long rangeSize;
				for (final var byteRange : fixedRanges) {
					rangeSize = byteRange.getSize();
					if (rangeSize == -1) {
						rangeSize = byteRange.getEnd() - byteRange.getBeg() + 1;
					}
					if (rangeSize > 0) {
						sizeSum += rangeSize;
					}
				}
				return sizeSum;
			} else {
				return itemSize;
			}
		default:
			return 0;
		}
	}

	private Input<String> getOutputPathSupplier() {
		final Input<String> pathInput;
		final var path = itemConfig.stringVal("output-path");
		if (path.contains(ASYNC_MARKER) || path.contains(SYNC_MARKER) || path.contains(INIT_MARKER)) {
			pathInput = CompositeExpressionInputBuilder.newInstance()
							.expression(path)
							.build();
		} else {
			pathInput = new ConstantValueInputImpl<>(path);
		}
		return pathInput;
	}

	private Input<I> newItemInput() throws IllegalConfigurationException {
		final var namingConfig = itemConfig.configVal("naming");
		final var itemNameInput = ItemNameInputFactory.fromConfig(namingConfig);
		if (itemFactory == null) {
			throw new IllegalConfigurationException("Item factory is not set");
		}
		if (itemFactory instanceof DataItemFactoryImpl) {
			final SizeInBytes itemDataSize;
			final var itemDataSizeRaw = itemConfig.val("data-size");
			if (itemDataSizeRaw instanceof String) {
				itemDataSize = BinarySizeFormat.parseSize((String) itemDataSizeRaw);
			} else {
				itemDataSize = new SizeInBytes(TypeUtil.typeConvert(itemDataSizeRaw, long.class));
			}
			itemInput = (Input<I>) new NewDataItemInput(itemFactory, itemNameInput, itemDataSize);
		} else {
			itemInput = new NewItemInput<>(itemFactory, itemNameInput);
		}
		return itemInput;
	}

	private static Map<String, Credential> loadCredentialsByPath(
					final String file, final long countLimit) {
		final var credByPath = (Map<String, Credential>) new HashMap<String, Credential>();
		try (final var br = Files.newBufferedReader(Paths.get(file))) {
			String line;
			String parts[];
			long count = 0;
			while (null != (line = br.readLine()) && count < countLimit) {
				parts = line.split(",", 3);
				credByPath.put(parts[0], Credential.getInstance(parts[1], parts[2]));
				count++;
			}
			Loggers.MSG.info("Loaded {} credential pairs from the file \"{}\"", credByPath.size(), file);
		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "Failed to load the credentials from the file \"{}\"", file);
		}
		return credByPath;
	}

	private static <I extends Item> int loadSrcItems(
					final Input<I> itemInput, final List<I> itemBuff, final int countLimit) {
		final var loadedCount = new LongAdder();
		final var executor = Executors.newScheduledThreadPool(
						2, new LogContextThreadFactory("loadSrcItemsWorker", true));
		final var finishLatch = new CountDownLatch(1);
		try {
			executor.execute(
							() -> {
								var n = 0;
								int m;
								try {
									while (n < countLimit) {
										m = itemInput.get(itemBuff, countLimit - n);
										if (m < 0) {
											Loggers.MSG.info("Loaded {} items, limit reached", n);
											break;
										} else {
											loadedCount.add(m);
											n += m;
										}
									}
								} catch (final Exception e) {
									if (e instanceof EOFException) {
										Loggers.MSG.info("Loaded {} items, end of items input", n);
									} else if (e instanceof IOException) {
										LogUtil.exception(Level.WARN, e, "Loaded {} items, I/O failure occurred", n);
									} else {
										throwUnchecked(e);
									}
								} finally {
									finishLatch.countDown();
								}
							});
			final ScheduledFuture<?> logTask = executor.scheduleAtFixedRate(
							() -> Loggers.MSG.info("Loaded {} items from the input...", loadedCount.sum()),
							0,
							10,
							TimeUnit.SECONDS);
			finishLatch.await();
			logTask.cancel(true);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} finally {
			executor.shutdownNow();
		}
		return loadedCount.intValue();
	}
}
