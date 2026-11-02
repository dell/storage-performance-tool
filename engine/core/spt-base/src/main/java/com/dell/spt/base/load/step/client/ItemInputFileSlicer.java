package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.load.step.client.LoadStepClient.OUTPUT_PROGRESS_PERIOD_MILLIS;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.deletion.StandaloneDeleteSelection;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.nic.pool.ItemSizeRecording;
import com.dell.spt.base.nic.pool.ItemSizeScan;
import com.dell.spt.base.nic.pool.ItemSizeSummary;
import com.dell.spt.base.nic.pool.PoolDemandConfig;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.file.FileInput;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.commons.csv.CSVPrinter;

public final class ItemInputFileSlicer implements AutoCloseable {

	private static final int APPROX_LINE_LENGTH = 0x40;

	private final String loadStepId;
	private final Map<FileManager, String> itemInputFileSlices;
	private final List<FileManager> fileMgrs;
	private final Map<FileManager, Config> configsByFileManager;
	private final boolean strictMode;
	// Empty unless the step has a NIC set: only a slice that gets a size summary has a recorder.
	private final Map<FileManager, ItemSizeSummary.Recorder> sizeRecorders = new HashMap<>();
	private final ItemSizeRecording sizeRecording;

	private static final class DeleteSelectionStats {
		private final Set<String> retainedBuckets;
		private long selected;
		private long currentKey;
		private long exactVersion;
		private final Map<String, Long> buckets = new TreeMap<>();

		private DeleteSelectionStats(final Set<String> retainedBuckets) {
			this.retainedBuckets = retainedBuckets;
		}

		private void record(final IntegrityManifestDataItem item) {
			selected++;
			if (item.versionId() == null || item.versionId().isEmpty()) {
				currentKey++;
			} else {
				exactVersion++;
			}
			final boolean retained = retainedBuckets.contains(item.bucket());
			final String bucket = retained ? item.bucket() : DeleteMetricsSnapshot.OVERFLOW_BUCKET;
			buckets.merge(bucket, 1L, Math::addExact);
		}
	}

	public <I extends Item> ItemInputFileSlicer(
					final String loadStepId,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final Input<I> itemInput,
					final int batchSize) {
		this(loadStepId, fileMgrs, configSlices, itemInput, batchSize, false);
	}

	public <I extends Item> ItemInputFileSlicer(
					final String loadStepId,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final Input<I> itemInput,
					final int batchSize,
					final boolean strictMode) {
		this(loadStepId, fileMgrs, configSlices, itemInput, batchSize, strictMode, null);
	}

	/**
	 * @param sizeRecording records each slice's item sizes as it writes the slice and passes them in
	 *     the slice's configuration (D54), for a slice whose engine has NIC Pools; {@code null} in a
	 *     step without a NIC set on any engine, which reads and slices as in 5.16
	 */
	public <I extends Item> ItemInputFileSlicer(
					final String loadStepId,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final Input<I> itemInput,
					final int batchSize,
					final boolean strictMode,
					final ItemSizeRecording sizeRecording) {
		this.loadStepId = loadStepId;
		this.strictMode = strictMode;
		this.sizeRecording = sizeRecording;
		final var sliceCount = configSlices.size();
		itemInputFileSlices = new HashMap<>(sliceCount);
		configsByFileManager = new HashMap<>(sliceCount);
		this.fileMgrs = fileMgrs;
		for (var i = 0; i < sliceCount; i++) {
			try {
				final var fileMgr = fileMgrs.get(i);
				if (null == fileMgr) {
					throw new IllegalStateException(
									"File manager for slice #" + i + " is null; remote node may not be reachable");
				}
				final var itemInputFileName = fileMgr.newTmpFileName() + (strictMode ? ".csv" : "");
				itemInputFileSlices.put(fileMgr, itemInputFileName);
				final var configSlice = configSlices.get(i);
				configsByFileManager.put(fileMgr, configSlice);
				configSlice.val("item-input-file", itemInputFileName);
				if (sizeRecording != null && PoolDemandConfig.recordsItemSizes(configSlice)) {
					sizeRecorders.put(fileMgr, new ItemSizeSummary.Recorder(itemInput.toString()));
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (strictMode) {
					throw terminal("failed to allocate item input file for slice " + i, e);
				}
				LogUtil.exception(
								Level.ERROR, e, "Failed to get the item input file name for the step slice #" + i);
			}
		}

		if (!(itemInput instanceof FileInput<?>)) {
			failItemSizes(ItemSizeSummary.Reason.NOT_A_FILE, itemInput.toString());
		}
		try {
			Loggers.MSG.info("{}: scatter the items from the input \"{}\"...", loadStepId, itemInput);
			scatterItems(itemInput, batchSize);
			sizeRecorders.values().forEach(recorder -> ItemSizeScan.finish(itemInput, recorder));
		} catch (final IOException e) {
			if (Thread.currentThread().isInterrupted()) {
				// A local write on an interrupted thread fails with an I/O error.
				removeSlicesAfterInterrupt();
				stopWritingIfInterrupted();
			}
			if (strictMode) {
				throw terminal("failed to parse or scatter canonical integrity input", e);
			}
			failItemSizes(ItemSizeSummary.Reason.READ_FAILED, e.toString());
			LogUtil.exception(Level.WARN, e, "{}: failed to use the item input", loadStepId);
		} catch (final Throwable cause) {
			if (cause instanceof InterruptedException) {
				removeSlicesAfterInterrupt();
			}
			throwUncheckedIfInterrupted(cause);
			if (strictMode) {
				throw terminal("unexpected canonical integrity input failure", cause);
			}
			failItemSizes(
							cause instanceof IllegalArgumentException || cause instanceof IndexOutOfBoundsException
											? ItemSizeSummary.Reason.MALFORMED_ITEM
											: ItemSizeSummary.Reason.READ_FAILED,
							cause.toString());
			LogUtil.exception(Level.ERROR, cause, "{}: unexpected failure", loadStepId);
		}
		publishItemSizes(itemInput);
	}

	private void failItemSizes(final ItemSizeSummary.Reason reason, final String detail) {
		sizeRecorders.values().forEach(recorder -> recorder.fail(reason, detail));
	}

	private void recordItemSize(final FileManager fileMgr, final Item item) throws IOException {
		if (!sizeRecorders.isEmpty()) {
			final ItemSizeSummary.Recorder recorder = sizeRecorders.get(fileMgr);
			if (recorder != null) {
				ItemSizeScan.recordItem(item, recorder);
			}
		}
	}

	/** Between batches: an expired budget leaves the summaries unknown, and the writes go on. */
	private void checkItemSizeBudget() {
		if (sizeRecording != null && !sizeRecorders.isEmpty() && sizeRecording.expired()) {
			failItemSizes(ItemSizeSummary.Reason.BUDGET_EXPIRED, null);
		}
	}

	/**
	 * Passes each summary with its slice's configuration, once the slices are written. This runs only
	 * after the scatter ended on its own, so a distribution fenced by the deadline publishes nothing.
	 */
	private <I extends Item> void publishItemSizes(final Input<I> itemInput) {
		if (sizeRecorders.isEmpty()) {
			return;
		}
		long items = 0;
		long smallest = Long.MAX_VALUE;
		long largest = -1;
		long millis = 0;
		ItemSizeSummary incomplete = null;
		for (final var entry : sizeRecorders.entrySet()) {
			final var summary = entry.getValue().summary();
			configsByFileManager.get(entry.getKey()).val(ItemSizeSummary.CONFIG_PATH, summary.encode());
			items += summary.items();
			millis = Math.max(millis, summary.millis());
			if (summary.largest() >= 0) {
				smallest = Math.min(smallest, summary.smallest());
				largest = Math.max(largest, summary.largest());
			}
			if (!summary.complete() && incomplete == null) {
				incomplete = summary;
			}
		}
		if (incomplete == null) {
			Loggers.MSG.info("{}: recorded the sizes in \"{}\" for {} slice(s) in {} ms: {} items, {} to {}", loadStepId, itemInput, sizeRecorders.size(),
							millis, items, BinarySizeFormat.formatFixedSize(largest < 0 ? 0 : smallest), BinarySizeFormat.formatFixedSize(Math.max(0, largest)));
		} else {
			Loggers.MSG.warn("{}: could not record the sizes in \"{}\" in {} ms: {}", loadStepId, itemInput, millis, incomplete.why());
		}
	}

	@Override
	public final void close() {
		Throwable cleanupFailure = null;
		for (final var entry : itemInputFileSlices.entrySet()) {
			final var fileMgr = entry.getKey();
			final var itemInputFileName = entry.getValue();
			try {
				fileMgr.deleteFile(itemInputFileName);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				LogUtil.exception(
								Level.WARN,
								e,
								"{}: failed to delete the file \"{}\" @ file manager \"{}\"",
								loadStepId,
								itemInputFileName,
								fileMgr);
				if (strictMode) {
					if (cleanupFailure == null) {
						cleanupFailure = e;
					} else {
						cleanupFailure.addSuppressed(e);
					}
				}
			}
		}
		itemInputFileSlices.clear();
		if (cleanupFailure != null) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CLEANUP,
							loadStepId,
							"failed to remove one or more distributed integrity input slices",
							cleanupFailure);
		}
	}

	/** A stop or the admission barrier's deadline interrupts: no further batch is written. */
	private void stopWritingIfInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throwUncheckedIfInterrupted(new InterruptedException(loadStepId + ": item distribution was interrupted"));
		}
	}

	/** The step never holds a slicer whose constructor failed, so it removes its own files. */
	private void removeSlicesAfterInterrupt() {
		try {
			close();
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to remove the item input slices", loadStepId);
		}
	}

	private <I extends Item> void scatterItems(final Input<I> itemInput, final int batchSize)
					throws IOException {
		if (strictMode) {
			scatterIntegrityItems(itemInput, batchSize);
			return;
		}

		Loggers.MSG.info("{}: slice the item input \"{}\"...", loadStepId, itemInput);

		final Map<FileManager, ByteArrayOutputStream> itemsOutByteBuffs = fileMgrs.stream()
						.filter(fm -> fm != null)
						.collect(
										Collectors.toMap(
														Function.identity(),
														fileMgr -> new ByteArrayOutputStream(batchSize * APPROX_LINE_LENGTH)));

		final Map<FileManager, ObjectOutputStream> itemsOutputs = new HashMap<>(itemsOutByteBuffs.size());
		IOException streamFailure = null;
		for (final var entry : itemsOutByteBuffs.entrySet()) {
			try {
				itemsOutputs.put(entry.getKey(), OBJECT_OUTPUT_STREAM_FACTORY.create(entry.getValue()));
			} catch (final IOException e) {
				streamFailure = e;
				LogUtil.exception(
								Level.WARN,
								e,
								"{}: failed to prepare the item input stream for file manager \"{}\"",
								loadStepId,
								entry.getKey());
				break;
			}
		}

		if (streamFailure != null) {
			itemsOutputs.values().forEach(out -> closeQuietly(out));
			throw streamFailure;
		}

		try {
			transferData(itemInput, itemsOutByteBuffs, itemsOutputs, batchSize);
		} finally {
			itemsOutputs.values().forEach(this::closeQuietly);
		}
	}

	private <I extends Item> void scatterIntegrityItems(
					final Input<I> itemInput, final int batchSize) throws IOException {
		Loggers.MSG.info("{}: slice the canonical item input \"{}\"...", loadStepId, itemInput);
		final List<FileManager> sliceFileMgrs = fileMgrs.stream().filter(fileMgr -> fileMgr != null).toList();
		if (sliceFileMgrs.isEmpty()) {
			throw new IOException("no item-input file slices are available");
		}
		final Map<FileManager, ByteArrayOutputStream> buffers = new HashMap<>(sliceFileMgrs.size());
		final Map<FileManager, CSVPrinter> printers = new HashMap<>(sliceFileMgrs.size());
		final Map<FileManager, DeleteSelectionStats> deleteStats = new HashMap<>(sliceFileMgrs.size());
		final Set<String> retainedBuckets = canonicalRetainedBuckets(itemInput);
		try {
			for (final FileManager fileMgr : sliceFileMgrs) {
				final var buffer = new ByteArrayOutputStream(batchSize * APPROX_LINE_LENGTH);
				final var printer = new CSVPrinter(
								new OutputStreamWriter(buffer, StandardCharsets.UTF_8), IntegrityCsvFormat.RFC4180_LF);
				printer.printRecord(IntegrityManifestItemInput.HEADER);
				printer.flush();
				fileMgr.writeToFile(itemInputFileSlices.get(fileMgr), buffer.toByteArray());
				buffer.reset();
				buffers.put(fileMgr, buffer);
				printers.put(fileMgr, printer);
				deleteStats.put(fileMgr, new DeleteSelectionStats(retainedBuckets));
			}

			final List<I> items = new ArrayList<>(batchSize);
			long count = 0;
			long nextSliceIndex = 0;
			while (true) {
				stopWritingIfInterrupted();
				final int itemCount;
				try {
					itemCount = itemInput.get(items, batchSize);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					if (e instanceof EOFException) {
						stopWritingIfInterrupted();
						break;
					}
					throw e;
				}
				if (itemCount <= 0) {
					stopWritingIfInterrupted();
					break;
				}
				for (int i = 0; i < itemCount; i++) {
					if (!(items.get(i) instanceof IntegrityManifestDataItem item)) {
						throw new IOException("canonical integrity input produced a non-manifest item");
					}
					final FileManager fileMgr = sliceFileMgrs.get((int) (nextSliceIndex % sliceFileMgrs.size()));
					printers.get(fileMgr).printRecord(
									item.bucket(), item.name(), item.size(), item.versionId() == null ? "" : item.versionId());
					deleteStats.get(fileMgr).record(item);
					recordItemSize(fileMgr, item);
					nextSliceIndex++;
				}
				items.clear();
				checkItemSizeBudget();
				for (final FileManager fileMgr : sliceFileMgrs) {
					final CSVPrinter printer = printers.get(fileMgr);
					final ByteArrayOutputStream buffer = buffers.get(fileMgr);
					printer.flush();
					final byte[] data = buffer.toByteArray();
					if (data.length > 0) {
						fileMgr.writeToFile(itemInputFileSlices.get(fileMgr), data);
						buffer.reset();
					}
				}
				count += itemCount;
			}
			Loggers.MSG.info(
							"Canonical items input \"{}\": {} items was distributed among the {} load step slices",
							itemInput,
							count,
							sliceFileMgrs.size());
			publishDeleteSelectionStats(deleteStats, retainedBuckets);
		} finally {
			for (final CSVPrinter printer : printers.values()) {
				try {
					printer.close(true);
				} catch (final IOException e) {
					LogUtil.exception(Level.WARN, e, "Failed to close a canonical item-input slice writer");
				}
			}
		}
	}

	private static <I extends Item> Set<String> canonicalRetainedBuckets(final Input<I> itemInput)
					throws IOException {
		if (!(itemInput instanceof IntegrityManifestItemInput manifestInput)) {
			throw new IOException("strict item scatter requires a canonical integrity manifest input");
		}
		final Set<String> retained = new LinkedHashSet<>(DeleteMetricsSnapshot.MAX_BUCKET_METRICS);
		for (final String encoded : StandaloneDeleteSelection
						.fromManifest(manifestInput.filePath().toString())
						.selectedBuckets()) {
			final int separator = encoded.lastIndexOf('=');
			final String bucket = encoded.substring(0, separator);
			if (!DeleteMetricsSnapshot.OVERFLOW_BUCKET.equals(bucket)) {
				retained.add(bucket);
			}
		}
		return retained;
	}

	private void publishDeleteSelectionStats(
					final Map<FileManager, DeleteSelectionStats> deleteStats,
					final Set<String> retainedBuckets) {
		final boolean hasOverflow = deleteStats.values().stream()
						.anyMatch(stats -> stats.buckets.containsKey(DeleteMetricsSnapshot.OVERFLOW_BUCKET));
		for (final var entry : deleteStats.entrySet()) {
			final Config config = configsByFileManager.get(entry.getKey());
			if (!standaloneDelete(config)) {
				continue;
			}
			final DeleteSelectionStats stats = entry.getValue();
			config.val("load-op-delete-selectionOrder", DELETE_SELECTION_ORDER_CANONICAL);
			config.val("load-op-delete-selected", stats.selected);
			config.val("load-op-delete-selectedCurrentKey", stats.currentKey);
			config.val("load-op-delete-selectedExactVersion", stats.exactVersion);
			config.val(
							"load-op-delete-selectedBuckets",
							canonicalBucketCounts(stats, retainedBuckets, hasOverflow).entrySet().stream()
											.map(bucket -> bucket.getKey() + '=' + bucket.getValue())
											.toList());
		}
	}

	private static Map<String, Long> canonicalBucketCounts(
					final DeleteSelectionStats stats,
					final Set<String> retainedBuckets,
					final boolean hasOverflow) {
		final Map<String, Long> result = new TreeMap<>();
		retainedBuckets.forEach(bucket -> result.put(bucket, stats.buckets.getOrDefault(bucket, 0L)));
		if (hasOverflow) {
			result.put(
							DeleteMetricsSnapshot.OVERFLOW_BUCKET,
							stats.buckets.getOrDefault(DeleteMetricsSnapshot.OVERFLOW_BUCKET, 0L));
		}
		return result;
	}

	private static boolean standaloneDelete(final Config config) {
		if (config == null) {
			return false;
		}
		try {
			return config.boolVal("load-op-delete-standalone");
		} catch (final RuntimeException ignored) {
			return false;
		}
	}

	private <I extends Item> void transferData(
					final Input<I> itemInput,
					final Map<FileManager, ByteArrayOutputStream> itemsOutByteBuffs,
					final Map<FileManager, ObjectOutputStream> itemsOutputs,
					final int batchSize)
					throws IOException {

		final int sliceCount = itemsOutByteBuffs.size();
		if (sliceCount == 0) {
			throw new IOException("no item-input file slices are available");
		}
		final List<FileManager> sliceFileMgrs = fileMgrs.stream()
						.filter(itemsOutputs::containsKey)
						.toList();
		final List<I> itemsBuff = new ArrayList<>(batchSize);

		int n;
		long count = 0;
		long nextSliceIndex = 0;
		long lastProgressOutputTimeMillis = System.currentTimeMillis();

		Loggers.MSG.info(
						"Items input \"{}\": starting to distribute the items among the {} load step slices",
						itemInput,
						sliceCount);

		while (true) {

			stopWritingIfInterrupted();

			// get the next batch of items
			try {
				n = itemInput.get(itemsBuff, batchSize);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (e instanceof EOFException) {
					stopWritingIfInterrupted();
					break;
				} else {
					throw e;
				}
			}

			if (n > 0) {

				// distribute the items using the round robin
				for (int i = 0; i < n; i++) {
					final int sliceIndex = (int) (nextSliceIndex % sliceCount);
					itemsOutputs.get(sliceFileMgrs.get(sliceIndex)).writeUnshared(itemsBuff.get(i));
					recordItemSize(sliceFileMgrs.get(sliceIndex), itemsBuff.get(i));
					nextSliceIndex++;
				}

				itemsBuff.clear();
				checkItemSizeBudget();

				// write the text items data to the remote input files
				final var writeFailure = new AtomicReference<IOException>();
				fileMgrs
								.stream()
								.filter(fm -> fm != null)
								.parallel()
								.forEach(
												fileMgr -> {
													final ByteArrayOutputStream buff = itemsOutByteBuffs.get(fileMgr);
													final String itemInputFileName = itemInputFileSlices.get(fileMgr);
													try {
														final byte[] data = buff.toByteArray();
														fileMgr.writeToFile(itemInputFileName, data);
														buff.reset();
													} catch (final IOException e) {
														writeFailure.compareAndSet(null, e);
														final var recorder = sizeRecorders.get(fileMgr);
														if (recorder != null) {
															recorder.fail(ItemSizeSummary.Reason.WRITE_FAILED, e.toString());
														}
														LogUtil.exception(
																		Level.WARN,
																		e,
																		"Failed to write the items input data to the {} file \"{}\"",
																		itemInputFileName,
																		(fileMgr instanceof FileManagerService ? "remote" : "local"));
													}
												});
				if (strictMode && writeFailure.get() != null) {
					throw writeFailure.get();
				}

				count += n;

				if (System.currentTimeMillis() - lastProgressOutputTimeMillis > OUTPUT_PROGRESS_PERIOD_MILLIS) {
					Loggers.MSG.info("Transferred {} items from the input \"{}\"...", count, itemInput);
					lastProgressOutputTimeMillis = System.currentTimeMillis();
				}

			} else {
				stopWritingIfInterrupted();
				break;
			}
		}

		Loggers.MSG.info(
						"Items input \"{}\": {} items was distributed among the {} load step slices",
						itemInput,
						count,
						sliceCount);
	}

	private void closeQuietly(final ObjectOutputStream outStream) {
		if (outStream == null) {
			return;
		}
		try {
			outStream.close();
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to close item input slice stream", loadStepId);
		}
	}

	private IntegrityTerminalException terminal(final String message, final Throwable cause) {
		return new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, loadStepId, message, cause);
	}

	@FunctionalInterface
	interface ObjectOutputStreamFactory {
		ObjectOutputStream create(OutputStream out) throws IOException;
	}

	private static volatile ObjectOutputStreamFactory OBJECT_OUTPUT_STREAM_FACTORY = ObjectOutputStream::new;

	static void setObjectOutputStreamFactoryForTesting(final ObjectOutputStreamFactory factory) {
		OBJECT_OUTPUT_STREAM_FACTORY = factory == null ? ObjectOutputStream::new : factory;
	}

	static void resetObjectOutputStreamFactoryForTesting() {
		OBJECT_OUTPUT_STREAM_FACTORY = ObjectOutputStream::new;
	}
}
