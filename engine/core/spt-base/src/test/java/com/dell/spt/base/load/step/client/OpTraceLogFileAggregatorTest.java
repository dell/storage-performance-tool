package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
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
import java.util.UUID;
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
		final String stepId = "untraced-step-" + UUID.randomUUID();
		final String file = "/w/" + stepId + "/op.trace.csv";
		final FileManagerService worker = mock(FileManagerService.class);
		when(worker.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenReturn(file);
		when(worker.readFromFile(anyString(), anyLong())).thenThrow(new NoSuchFileException(file));
		final var aggregator = new OpTraceLogFileAggregator(stepId, List.of(FileManager.INSTANCE, worker));

		final List<String> warnings = warningsWhile(stepId, aggregator::close);

		verify(worker, never()).deleteFile(anyString());
		assertEquals(List.of(), warnings);
	}

	/**
	 * A trace file that goes away after a part of it was copied leaves the entry's trace incomplete:
	 * that is still an error, and the delete is still tried.
	 */
	@Test
	public void aRemoteFileThatGoesAwayPartWayIsAnError() throws Exception {
		final String stepId = "partly-traced-step-" + UUID.randomUUID();
		final String file = "/w/" + stepId + "/op.trace.csv";
		final byte[] row = "1,trace-row\n".getBytes(StandardCharsets.UTF_8);
		final FileManagerService worker = mock(FileManagerService.class);
		when(worker.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenReturn(file);
		when(worker.readFromFile(file, 0L)).thenReturn(row);
		when(worker.readFromFile(file, row.length)).thenThrow(new NoSuchFileException(file));
		doThrow(new NoSuchFileException(file)).when(worker).deleteFile(file);
		final var aggregator = new OpTraceLogFileAggregator(stepId, List.of(FileManager.INSTANCE, worker));

		final List<String> warnings = warningsWhile(stepId, aggregator::close);

		verify(worker).deleteFile(file);
		assertEquals(2, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).startsWith("ERROR Unexpected I/O exception"), warnings.toString());
		assertTrue(warnings.get(1).startsWith("WARN " + stepId + ": failed to delete the file"), warnings.toString());
	}

	/** The capture keeps the messages about its token only, not another test's warning in the same JVM. */
	@Test
	public void theCaptureLeavesOutMessagesOfOtherTests() throws Exception {
		final String token = "capture-" + UUID.randomUUID();

		final List<String> warnings = warningsWhile(token, () -> {
			Loggers.MSG.warn("a warning of another test");
			Loggers.ERR.error("an error of another test", new IOException("another file"));
			Loggers.MSG.warn("{}: a warning of this test", token);
		});

		assertEquals(List.of("WARN " + token + ": a warning of this test"), warnings);
	}

	/**
	 * The WARN and ERROR messages about {@code token} (in the message or its cause) that the message
	 * and error loggers received while {@code action} ran, each prefixed with its level. Other
	 * messages, as from threads of other tests in this JVM, are left out.
	 */
	private static List<String> warningsWhile(final String token, final Runnable action) throws InterruptedException {
		final var context = LoggerContext.getContext(false);
		final Logger messages = context.getLogger(Loggers.MSG.getName());
		final Logger errors = context.getLogger(Loggers.ERR.getName());
		final String endOfLog = token + ": end of log";
		final var capture = new LogCapture(token);
		capture.start();
		messages.addAppender(capture);
		errors.addAppender(capture);
		try {
			action.run();
			// The loggers may be asynchronous: this line comes after everything the action logged.
			Loggers.ERR.error(endOfLog);
			final String endLine = Level.ERROR + " " + endOfLog;
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (!capture.lines.contains(endLine)) {
				assertTrue(System.nanoTime() < deadline, "the log did not reach its end: " + capture.lines);
				Thread.sleep(10);
			}
			return capture.lines.stream().filter(line -> !endLine.equals(line)).toList();
		} finally {
			messages.removeAppender(capture);
			errors.removeAppender(capture);
			capture.stop();
		}
	}

	private static final class LogCapture extends AbstractAppender {

		private final String token;
		private final List<String> lines = new CopyOnWriteArrayList<>();

		private LogCapture(final String token) {
			super("opTraceAggregatorTestCapture", null, null, true, Property.EMPTY_ARRAY);
			this.token = token;
		}

		@Override
		public void append(final LogEvent event) {
			if (!event.getLevel().isMoreSpecificThan(Level.WARN)) {
				return;
			}
			final String message = event.getMessage().getFormattedMessage();
			final Throwable thrown = event.getThrown();
			if (message.contains(token) || thrown != null && String.valueOf(thrown).contains(token)) {
				lines.add(event.getLevel() + " " + message);
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
