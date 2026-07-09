package com.dell.spt.base.load.step.client;

import com.dell.spt.base.env.FsUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.util.BinarySizeFormat;
import org.apache.logging.log4j.Level;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.load.step.client.LoadStepClient.OUTPUT_PROGRESS_PERIOD_MILLIS;
import static org.apache.logging.log4j.CloseableThreadContext.put;

// unlike other aggregators this one pulls each remote tmp timing metrics (latency and duration) file
// into a separate local file.
// This is done to increase performance as mock tests showed that sorting 4 files 25mil lines each
// and then mergeSorting them is 2+ times faster than sorting 100mil file using Collections.sort().
public class ItemTimingMetricOutputFileAggregator implements AutoCloseable {

	private final String loadStepId;
	private final Path itemTimingMetricsOutputFilePath;
	private final Map<FileManager, String> itemTimingMetricOutputFileSlices;

	public ItemTimingMetricOutputFileAggregator(
					final String loadStepId,
					final List<FileManager> fileMgrs) {
		this.loadStepId = loadStepId;
		final var sliceCount = fileMgrs.size();
		this.itemTimingMetricOutputFileSlices = new HashMap<>(sliceCount);
		this.itemTimingMetricsOutputFilePath = Paths.get(System.getProperty("java.io.tmpdir"),
						"spt", "timingMetrics_" + loadStepId);

		// file manager for slice == 0 is the file manager of the entry node. Shouldn't be a service.
		if (fileMgrs.get(0) instanceof FileManagerService) {
			throw new AssertionError("File manager @ index #0 shouldn't be a service");
		}
		itemTimingMetricOutputFileSlices.put(fileMgrs.get(0), itemTimingMetricsOutputFilePath.toString());

		for (var i = 1; i < sliceCount; i++) {
			final var fileMgr = fileMgrs.get(i);
			if (fileMgr instanceof FileManagerService) {
				try {
					itemTimingMetricOutputFileSlices.put(fileMgr, itemTimingMetricsOutputFilePath.toString());
					Loggers.MSG.debug(
									"\"{}\": new tmp item output file \"{}\"", fileMgr, itemTimingMetricsOutputFilePath.toString());
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

	// each remote file is collected to a local file that has the same old name + the random number
	private void collectToLocal() {
		final var byteCounter = new LongAdder();
		final var executor = Executors.newScheduledThreadPool(
						2, new LogContextThreadFactory("collectItemTimingMetricsOutputFileWorker", true));
		// we need as many locks as there are slices except for entry node. Because we have a separate tmp file for each slice
		final var finishLatch = new CountDownLatch(itemTimingMetricOutputFileSlices.size() - 1);
		FsUtil.createParentDirsIfNotExist(itemTimingMetricsOutputFilePath);
		final Future<?> transferTask = executor.submit(
						() -> {
							itemTimingMetricOutputFileSlices
											.entrySet()
											.parallelStream()
											// don't transfer & delete local item output file
											.filter(entry -> entry.getKey() instanceof FileManagerService)
											.forEach(
															entry -> {
																final var fileMgr = entry.getKey();
																final var remoteItemOutputFileName = entry.getValue();
																final Path localItemOutputPath = Paths.get(
																				itemTimingMetricsOutputFilePath.toString()
																								+ "_"
																								+ ThreadLocalRandom.current().nextLong());
																try (final var localItemOutput = Files.newOutputStream(localItemOutputPath,
																				FileManager.appendOpenOptions())) {
																	transferToLocal(
																					fileMgr,
																					remoteItemOutputFileName,
																					localItemOutput,
																					byteCounter);
																} catch (final IOException e) {
																	LogUtil.exception(
																					Level.WARN,
																					e,
																					"{}: failed to open the local item output file \"{}\" for appending",
																					loadStepId,
																					itemTimingMetricsOutputFilePath.toString());
																} finally {
																	finishLatch.countDown();
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
						});
		final ScheduledFuture<?> progressTask = executor.scheduleAtFixedRate(
						() -> Loggers.MSG.info(
										"\"{}\" <- transferred {} of the output items data...",
										itemTimingMetricsOutputFilePath.toString(),
										BinarySizeFormat.formatFixedSize(byteCounter.longValue())),
						0,
						OUTPUT_PROGRESS_PERIOD_MILLIS,
						TimeUnit.MILLISECONDS);

		try {
			finishLatch.await();
			transferTask.get();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			LogUtil.exception(Level.ERROR, e, "data aggregation was interrupted. Metrics may be inaccurate");
			//throwUnchecked(e); //TODO: what to do? ctrl c while aggregating -> ? BASE-1447
		} catch (final ExecutionException e) {
			final var cause = e.getCause() != null ? e.getCause() : e;
			LogUtil.exception(Level.ERROR, cause, "data aggregation failed");
		} finally {
			progressTask.cancel(true);
			executor.shutdownNow();
			Loggers.MSG.info(
							"\"{}\" <- transferred {} of the output items data",
							itemTimingMetricsOutputFilePath.toString(),
							BinarySizeFormat.formatFixedSize(byteCounter.longValue()));
		}
	}

	private static void transferToLocal(
					final FileManager fileMgr,
					final String remoteItemOutputFileName,
					final OutputStream localItemOutput,
					final LongAdder byteCounter) {
		long transferredByteCount = 0;
		try (final var logCtx = put(KEY_CLASS_NAME,
						ItemTimingMetricOutputFileAggregator.class.getSimpleName())) {
			byte buff[];
			while (true) {
				buff = fileMgr.readFromFile(remoteItemOutputFileName, transferredByteCount);
				localItemOutput.write(buff);
				transferredByteCount += buff.length;
				byteCounter.add(buff.length);
			}
		} catch (final EOFException eof) {
			Loggers.MSG.debug(
							"Reached end of remote timing metrics file '{}' @ '{}' after {}",
							remoteItemOutputFileName,
							fileMgr,
							BinarySizeFormat.formatFixedSize(transferredByteCount));
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, "Remote items output timing metrics file transfer failure");
		} finally {
			Loggers.MSG.debug(
							"{} of items timing metrics output data transferred from \"{}\" @ \"{}\" to \"{}\"",
							BinarySizeFormat.formatFixedSize(transferredByteCount),
							remoteItemOutputFileName,
							fileMgr,
							localItemOutput);
		}
	}

	@Override
	public final void close() {
		Loggers.MSG.info("closing metrics aggregator");
		try {
			collectToLocal();
		} finally {
			itemTimingMetricOutputFileSlices.clear();
		}
	}
}
