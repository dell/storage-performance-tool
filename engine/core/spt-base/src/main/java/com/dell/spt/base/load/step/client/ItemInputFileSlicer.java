package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.load.step.client.LoadStepClient.OUTPUT_PROGRESS_PERIOD_MILLIS;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;

public final class ItemInputFileSlicer implements AutoCloseable {

	private static final int APPROX_LINE_LENGTH = 0x40;

	private final String loadStepId;
	private final Map<FileManager, String> itemInputFileSlices;
	private final List<FileManager> fileMgrs;
	private final boolean strictMode;

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
		this.loadStepId = loadStepId;
		this.strictMode = strictMode;
		final var sliceCount = configSlices.size();
		itemInputFileSlices = new HashMap<>(sliceCount);
		this.fileMgrs = fileMgrs;
		for (var i = 0; i < sliceCount; i++) {
			try {
				final var fileMgr = fileMgrs.get(i);
				if (null == fileMgr) {
					throw new IllegalStateException(
									"File manager for slice #" + i + " is null; remote node may not be reachable");
				}
				final var itemInputFileName = fileMgr.newTmpFileName();
				itemInputFileSlices.put(fileMgr, itemInputFileName);
				final var configSlice = configSlices.get(i);
				configSlice.val("item-input-file", itemInputFileName);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (strictMode) {
					throw terminal("failed to allocate item input file for slice " + i, e);
				}
				LogUtil.exception(
								Level.ERROR, e, "Failed to get the item input file name for the step slice #" + i);
			}
		}

		try {
			Loggers.MSG.info("{}: scatter the items from the input \"{}\"...", loadStepId, itemInput);
			scatterItems(itemInput, batchSize);
		} catch (final IOException e) {
			if (strictMode) {
				throw terminal("failed to parse or scatter canonical integrity input", e);
			}
			LogUtil.exception(Level.WARN, e, "{}: failed to use the item input", loadStepId);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			if (strictMode) {
				throw terminal("unexpected canonical integrity input failure", cause);
			}
			LogUtil.exception(Level.ERROR, cause, "{}: unexpected failure", loadStepId);
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

	private <I extends Item> void scatterItems(final Input<I> itemInput, final int batchSize)
					throws IOException {

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

	private <I extends Item> void transferData(
					final Input<I> itemInput,
					final Map<FileManager, ByteArrayOutputStream> itemsOutByteBuffs,
					final Map<FileManager, ObjectOutputStream> itemsOutputs,
					final int batchSize)
					throws IOException {

		final int sliceCount = itemsOutByteBuffs.size();
		final List<I> itemsBuff = new ArrayList<>(batchSize);

		int n;
		long count = 0;
		long lastProgressOutputTimeMillis = System.currentTimeMillis();

		Loggers.MSG.info(
						"Items input \"{}\": starting to distribute the items among the {} load step slices",
						itemInput,
						sliceCount);

		while (true) {

			// get the next batch of items
			try {
				n = itemInput.get(itemsBuff, batchSize);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (e instanceof EOFException) {
					break;
				} else {
					throw e;
				}
			}

			if (n > 0) {

				// distribute the items using the round robin
				for (int i = 0; i < n; i++) {
					itemsOutputs.get(fileMgrs.get(i % sliceCount)).writeUnshared(itemsBuff.get(i));
				}

				itemsBuff.clear();

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
