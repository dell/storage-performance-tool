package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.load.step.client.LoadStepClient.OUTPUT_PROGRESS_PERIOD_MILLIS;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.util.BinarySizeFormat;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.Level;

public class OpTraceLogFileAggregator implements Closeable {

	private final String loadStepId;
	private final int sliceCount;
	private final List<Map.Entry<FileManager, String>> remoteSlicesByIndex;

	public OpTraceLogFileAggregator(final String loadStepId, final List<FileManager> fileMgrs) {
		this.loadStepId = loadStepId;
		this.sliceCount = fileMgrs.size();
		// One entry per remote slice, in slice order; the local trace log of slice 0 is left out.
		this.remoteSlicesByIndex = new ArrayList<>(Math.max(0, sliceCount - 1));
		for (var i = 1; i < sliceCount; i++) {
			final var fileMgr = fileMgrs.get(i);
			if (!(fileMgr instanceof FileManagerService)) {
				remoteSlicesByIndex.add(null);
				continue;
			}
			String ioTraceLogFileSliceName = null;
			try {
				ioTraceLogFileSliceName = fileMgr.logFileName(Loggers.OP_TRACES.getName(), loadStepId);
				Loggers.MSG.debug(
								"{}: the remote file manager \"{}\" returned the file name \"{}\" for the I/O traces",
								loadStepId,
								fileMgr,
								ioTraceLogFileSliceName);
			} catch (final IOException e) {
				LogUtil.exception(
								Level.WARN,
								e,
								"{}: failed to get the remote log file name",
								loadStepId);
			}
			remoteSlicesByIndex.add(ioTraceLogFileSliceName == null ? null : Map.entry(fileMgr, ioTraceLogFileSliceName));
		}
	}

	public final void collectToLocal() {
		collectToLocal(remoteSlicesByIndex);
	}

	private void collectToLocal(final List<Map.Entry<FileManager, String>> remoteSlices) {

		final LongAdder byteCounter = new LongAdder();
		final ScheduledExecutorService executor = Executors.newScheduledThreadPool(
						2, new LogContextThreadFactory("collectOpTraceLogFileWorker", true));
		final CountDownLatch finishLatch = new CountDownLatch(1);

		final Future<?> transferTask = executor.submit(
						() -> {
							try {
								remoteSlices
												.parallelStream()
												// don't transfer the local file data
												.filter(entry -> entry != null && entry.getKey() instanceof FileManagerService)
												.forEach(
																entry -> {
																	final var fileMgr = entry.getKey();
																	final var remoteIoTraceLogFileName = entry.getValue();
																	if (!transferToLocal(fileMgr, remoteIoTraceLogFileName, byteCounter)) {
																		return;
																	}
																	try {
																		fileMgr.deleteFile(remoteIoTraceLogFileName);
																	} catch (final Exception e) {
																		throwUncheckedIfInterrupted(e);
																		LogUtil.exception(
																						Level.WARN,
																						e,
																						"{}: failed to delete the file \"{}\" @ file manager \"{}\"",
																						loadStepId,
																						remoteIoTraceLogFileName,
																						fileMgr);
																	}
																});
							} finally {
								finishLatch.countDown();
							}
						});
		final ScheduledFuture<?> progressTask = executor.scheduleAtFixedRate(
						() -> Loggers.MSG.info(
										"\"{}\": transferred {} I/O trace data...",
										loadStepId,
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
			LogUtil.exception(Level.ERROR, cause, "{}: failed during operation trace aggregation", loadStepId);
		} finally {
			progressTask.cancel(true);
			executor.shutdownNow();
			Loggers.MSG.info(
							"\"{}\": transferred {} of the operation traces data",
							loadStepId,
							BinarySizeFormat.formatFixedSize(byteCounter.longValue()));
		}
	}

	/**
	 * Copies the remote file into the local operation traces log. Returns {@code false} when the
	 * remote file does not exist before anything was copied: the engine traced nothing under this
	 * step id, so there is nothing to delete either. A file that goes away after a part was copied
	 * leaves an incomplete trace, and is an error.
	 */
	private static boolean transferToLocal(
					final FileManager fileMgr,
					final String remoteIoTraceLogFileName,
					final LongAdder byteCounter) {
		long transferredByteCount = 0;
		try (final Instance logCtx = put(KEY_CLASS_NAME, OpTraceLogFileAggregator.class.getSimpleName())) {
			byte[] data;
			while (true) {
				data = fileMgr.readFromFile(remoteIoTraceLogFileName, transferredByteCount);
				Loggers.OP_TRACES.info(new String(data, StandardCharsets.UTF_8));
				transferredByteCount += data.length;
				byteCounter.add(data.length);
			}
		} catch (final EOFException eof) {
			Loggers.MSG.debug(
							"Reached end of remote operation trace file '{}' @ '{}' after {}",
							remoteIoTraceLogFileName,
							fileMgr,
							BinarySizeFormat.formatFixedSize(transferredByteCount));
		} catch (final RemoteException e) {
			LogUtil.exception(Level.WARN, e, "Failed to read the data from the remote file");
		} catch (final NoSuchFileException absent) {
			if (transferredByteCount > 0) {
				LogUtil.exception(Level.ERROR, absent, "Unexpected I/O exception");
				return true;
			}
			Loggers.MSG.debug(
							"No operation trace file '{}' @ '{}': that engine traced no operations",
							remoteIoTraceLogFileName,
							fileMgr);
			return false;
		} catch (final IOException e) {
			LogUtil.exception(Level.ERROR, e, "Unexpected I/O exception");
		} finally {
			Loggers.MSG.debug(
							"Transferred {} of the remote operation traces data from the remote file \"{}\" @ \"{}\"",
							BinarySizeFormat.formatFixedSize(transferredByteCount),
							remoteIoTraceLogFileName,
							fileMgr);
		}
		return true;
	}

	@Override
	public final void close() {
		try (final Instance logCtx = put(KEY_STEP_ID, loadStepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			collectToLocal(remoteSlicesByIndex);
		}
		remoteSlicesByIndex.clear();
	}

	/** Collects remote operation traces only from the first {@code startedSlices} slices whose start returned. */
	void closeAfterStoppedStart(final int startedSlices) {
		if (startedSlices < 0 || startedSlices > sliceCount) {
			throw new IllegalArgumentException(
							"started slices " + startedSlices + " not in 0.." + sliceCount);
		}
		try (final Instance logCtx = put(KEY_STEP_ID, loadStepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			if (startedSlices > 1) {
				collectToLocal(remoteSlicesByIndex.subList(0, startedSlices - 1));
			}
		} finally {
			remoteSlicesByIndex.clear();
		}
	}
}
