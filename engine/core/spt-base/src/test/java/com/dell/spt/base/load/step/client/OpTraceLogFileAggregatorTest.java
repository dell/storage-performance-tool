package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.Loggers;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link OpTraceLogFileAggregator#transferToLocal(FileManager, String, LongAdder)} via
 * reflection.
 */
public class OpTraceLogFileAggregatorTest {

	@Test
	public void transferToLocalStopsOnEof() throws Exception {
		final Method method = OpTraceLogFileAggregator.class.getDeclaredMethod(
						"transferToLocal", FileManager.class, String.class, LongAdder.class);
		method.setAccessible(true);
		final RecordingFileManager fileManager = new RecordingFileManager("trace.log", "entry".getBytes(StandardCharsets.UTF_8));
		final LongAdder counter = new LongAdder();
		method.invoke(null, fileManager, "trace.log", counter);
		assertEquals(1, fileManager.transferCount());
		assertEquals(5L, counter.longValue());
	}

	/** An engine that traced nothing has no trace file: that is no error, and there is nothing to delete. */
	@Test
	public void aMissingRemoteFileLogsNoWarningAndIsNotDeleted() throws Exception {
		final FileManagerService worker = mock(FileManagerService.class);
		when(worker.logFileName(Loggers.OP_TRACES.getName(), "untraced-step")).thenReturn("/w/op.trace.csv");
		when(worker.readFromFile(anyString(), anyLong())).thenThrow(new NoSuchFileException("/w/op.trace.csv"));
		final var aggregator = new OpTraceLogFileAggregator("untraced-step", List.of(FileManager.INSTANCE, worker));

		final List<String> warnings = warningsWhile(aggregator::close);

		verify(worker, never()).deleteFile(anyString());
		assertEquals(List.of(), warnings);
	}

	/** The WARN and ERROR messages the message and error loggers received while {@code action} ran. */
	private static List<String> warningsWhile(final Runnable action) throws InterruptedException {
		final var context = LoggerContext.getContext(false);
		final Logger messages = context.getLogger(Loggers.MSG.getName());
		final Logger errors = context.getLogger(Loggers.ERR.getName());
		final var capture = new LogCapture();
		capture.start();
		messages.addAppender(capture);
		errors.addAppender(capture);
		try {
			action.run();
			// The loggers may be asynchronous: this line comes after everything the action logged.
			Loggers.ERR.error(END_OF_LOG);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (!capture.lines.contains(END_OF_LOG)) {
				assertTrue(System.nanoTime() < deadline, "the log did not reach its end: " + capture.lines);
				Thread.sleep(10);
			}
			return capture.lines.stream().filter(line -> !END_OF_LOG.equals(line)).toList();
		} finally {
			messages.removeAppender(capture);
			errors.removeAppender(capture);
			capture.stop();
		}
	}

	private static final String END_OF_LOG = "op-trace-aggregator-test: end of log";

	private static final class LogCapture extends AbstractAppender {

		private final List<String> lines = new CopyOnWriteArrayList<>();

		private LogCapture() {
			super("opTraceAggregatorTestCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
				lines.add(event.getMessage().getFormattedMessage());
			}
		}
	}

	private static final class RecordingFileManager implements FileManager {
		private final String expectedFile;
		private final byte[] payload;
		private boolean served;
		private int transferCount;

		RecordingFileManager(final String expectedFile, final byte[] payload) {
			this.expectedFile = expectedFile;
			this.payload = payload;
		}

		@Override
		public byte[] readFromFile(final String fileName, final long offset) throws IOException {
			if (!expectedFile.equals(fileName)) {
				throw new IOException("unexpected file");
			}
			if (served) {
				throw new EOFException();
			}
			served = true;
			transferCount++;
			return payload;
		}

		@Override
		public String logFileName(final String loggerName, final String testStepId) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String newTmpFileName() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeToFile(final String fileName, final byte[] buff) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public long fileSize(final String fileName) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void truncateFile(final String fileName, final long size) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void deleteFile(final String fileName) throws IOException {
			throw new UnsupportedOperationException();
		}

		int transferCount() {
			return transferCount;
		}
	}

}
