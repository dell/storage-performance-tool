package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.load.step.client.LoadStepClient.OUTPUT_PROGRESS_PERIOD_MILLIS;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.env.FsUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.Level;

public final class ItemOutputFileAggregator implements AutoCloseable {

	/** Reads of one part of a remote file before its transfer is given up and the file kept. */
	static final int READ_ATTEMPTS = 3;
	static final long READ_RETRY_DELAY_MILLIS = 1_000;

	private final String loadStepId;
	private final String itemOutputFile;
	private final Map<FileManager, String> itemOutputFileSlices;
	private final long readRetryDelayMillis;

	public ItemOutputFileAggregator(
					final String loadStepId,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final String itemOutputFile) {
		this(loadStepId, fileMgrs, configSlices, itemOutputFile, READ_RETRY_DELAY_MILLIS);
	}

	ItemOutputFileAggregator(
					final String loadStepId,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final String itemOutputFile,
					final long readRetryDelayMillis) {
		this.loadStepId = loadStepId;
		this.itemOutputFile = itemOutputFile;
		this.readRetryDelayMillis = readRetryDelayMillis;
		final var sliceCount = fileMgrs.size();
		this.itemOutputFileSlices = new HashMap<>(sliceCount);
		for (var i = 0; i < sliceCount; i++) {
			final var fileMgr = fileMgrs.get(i);
			if (i == 0) {
				if (fileMgr instanceof FileManagerService) {
					throw new AssertionError("File manager @ index #" + i + " shouldn't be a service");
				}
			} else {
				if (fileMgr instanceof FileManagerService) {
					try {
						final var remoteItemOutputFileName = fileMgr.newTmpFileName();
						configSlices.get(i).val("item-output-file", remoteItemOutputFileName);
						itemOutputFileSlices.put(fileMgr, remoteItemOutputFileName);
						Loggers.MSG.debug(
										"\"{}\": new tmp item output file \"{}\"", fileMgr, remoteItemOutputFileName);
					} catch (final Exception e) {
						throwUncheckedIfInterrupted(e);
						LogUtil.exception(
										Level.ERROR,
										e,
										"Failed to get the new temporary file name for the file manager service \"{}\"",
										fileMgr);
					}
				} else {
					throw new AssertionError("File manager @ index #" + i + " should be a service");
				}
			}
		}
	}

	@Override
	public final void close() {
		try {
			collectToLocal();
		} finally {
			itemOutputFileSlices.clear();
		}
	}

	private void collectToLocal() {
		final var byteCounter = new LongAdder();
		final var executor = Executors.newScheduledThreadPool(
						2, new LogContextThreadFactory("collectItemOutputFileWorker", true));
		final var finishLatch = new CountDownLatch(1);
		final var itemOutputPath = Paths.get(itemOutputFile);
		FsUtil.createParentDirsIfNotExist(itemOutputPath);
		final Future<?> transferTask = executor.submit(
						() -> {
							try (final var localItemOutput = Files.newOutputStream(itemOutputPath, FileManager.appendOpenOptions())) {
								final Lock localItemOutputLock = new ReentrantLock();
								itemOutputFileSlices
												.entrySet()
												.parallelStream()
												// don't transfer & delete local item output file
												.filter(entry -> entry.getKey() instanceof FileManagerService)
												.forEach(
																entry -> {
																	final var fileMgr = entry.getKey();
																	final var remoteItemOutputFileName = entry.getValue();
																	final boolean transferred = transferToLocal(
																					fileMgr,
																					remoteItemOutputFileName,
																					localItemOutput,
																					localItemOutputLock,
																					byteCounter);
																	if (!transferred) {
																		// Keep the only full copy of what has not arrived.
																		return;
																	}
																	try {
																		fileMgr.deleteFile(remoteItemOutputFileName);
																	} catch (final Exception e) {
																		throwUncheckedIfInterrupted(e);
																		LogUtil.exception(
																						Level.WARN,
																						e,
																						"{}: failed to delete the file \"{}\" @ file manager \"{}\"",
																						loadStepId,
																						remoteItemOutputFileName,
																						fileMgr);
																	}
																});
							} catch (final IOException e) {
								LogUtil.exception(
												Level.WARN,
												e,
												"{}: failed to open the local item output file \"{}\" for appending",
												loadStepId,
												itemOutputFile);
							} finally {
								finishLatch.countDown();
							}
						});
		final ScheduledFuture<?> progressTask = executor.scheduleAtFixedRate(
						() -> Loggers.MSG.info(
										"\"{}\" <- transferred {} of the output items data...",
										itemOutputFile,
										BinarySizeFormat.formatFixedSize(byteCounter.longValue())),
						0,
						OUTPUT_PROGRESS_PERIOD_MILLIS,
						TimeUnit.MILLISECONDS);

		try {
			finishLatch.await();
			transferTask.get();
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final ExecutionException e) {
			final var cause = e.getCause() != null ? e.getCause() : e;
			LogUtil.exception(Level.ERROR, cause, "{}: data aggregation failed", loadStepId);
		} finally {
			progressTask.cancel(true);
			executor.shutdownNow();
			Loggers.MSG.info(
							"\"{}\" <- transferred {} of the output items data",
							itemOutputFile,
							BinarySizeFormat.formatFixedSize(byteCounter.longValue()));
		}
	}

	/**
	 * Appends a remote file to the local output and returns whether all of it arrived. A failed
	 * read is retried from the same offset, which a read returning whole lines makes safe; a
	 * failed local write is not, as it may have written part of its buffer.
	 */
	private boolean transferToLocal(
					final FileManager fileMgr,
					final String remoteItemOutputFileName,
					final OutputStream localItemOutput,
					final Lock localItemOutputLock,
					final LongAdder byteCounter) {
		long transferredByteCount = 0;
		int failedReads = 0;
		try (final var logCtx = put(KEY_CLASS_NAME, ItemOutputFileAggregator.class.getSimpleName())) {
			while (true) {
				final byte[] buff;
				try {
					buff = fileMgr.readFromFile(remoteItemOutputFileName, transferredByteCount);
				} catch (final EOFException eof) {
					Loggers.MSG.debug(
									"Reached end of remote item output file '{}' @ '{}' after {}",
									remoteItemOutputFileName,
									fileMgr,
									BinarySizeFormat.formatFixedSize(transferredByteCount));
					return true;
				} catch (final IOException e) {
					if (++failedReads >= READ_ATTEMPTS) {
						LogUtil.exception(
										Level.ERROR,
										e,
										"{}: failed to read the item output file \"{}\" @ \"{}\" {} times; {} of it was collected"
														+ " and the file is kept there",
										loadStepId,
										remoteItemOutputFileName,
										fileMgr,
										failedReads,
										BinarySizeFormat.formatFixedSize(transferredByteCount));
						return false;
					}
					LogUtil.exception(
									Level.WARN,
									e,
									"{}: failed to read the item output file \"{}\" @ \"{}\", retrying",
									loadStepId,
									remoteItemOutputFileName,
									fileMgr);
					try {
						Thread.sleep(readRetryDelayMillis);
					} catch (final InterruptedException interrupted) {
						Thread.currentThread().interrupt();
						return false;
					}
					continue;
				}
				if (buff.length == 0) {
					return true;
				}
				failedReads = 0;
				localItemOutputLock.lock();
				try {
					localItemOutput.write(buff);
				} catch (final IOException e) {
					LogUtil.exception(
									Level.ERROR,
									e,
									"{}: failed to write the item output file \"{}\" @ \"{}\" to the local output; the file is"
													+ " kept there",
									loadStepId,
									remoteItemOutputFileName,
									fileMgr);
					return false;
				} finally {
					localItemOutputLock.unlock();
				}
				transferredByteCount += buff.length;
				byteCounter.add(buff.length);
			}
		} finally {
			Loggers.MSG.debug(
							"{} of items output data transferred from \"{}\" @ \"{}\" to \"{}\"",
							BinarySizeFormat.formatFixedSize(transferredByteCount),
							remoteItemOutputFileName,
							fileMgr,
							localItemOutput);
		}
	}
}
