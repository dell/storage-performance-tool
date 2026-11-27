package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
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
import java.rmi.RemoteException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * Tests the operation trace collection of {@link OpTraceLogFileAggregator}, and its
 * {@link OpTraceLogFileAggregator#transferToLocal(FileManager, String, LongAdder)} via reflection.
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
	 * A worker whose trace file name cannot be read is left out with one warning; the other workers'
	 * traces are still collected, and the step is not refused for it.
	 */
	@Test
	public void aWorkerWhoseTraceFileNameCannotBeReadIsLeftOutWithOneWarning() throws Exception {
		final String stepId = "unnamed-trace-step-" + UUID.randomUUID();
		final String file = "/w2/" + stepId + "/op.trace.csv";
		final FileManagerService unnamed = mock(FileManagerService.class);
		when(unnamed.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenThrow(new RemoteException("the worker went away"));
		final FileManagerService named = mock(FileManagerService.class);
		when(named.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenReturn(file);
		when(named.readFromFile(file, 0L)).thenThrow(new EOFException());
		final List<OpTraceLogFileAggregator> built = new ArrayList<>();

		final List<String> warnings = warningsWhile(
						stepId, () -> built.add(new OpTraceLogFileAggregator(stepId, List.of(FileManager.INSTANCE, unnamed, named))));
		built.getFirst().close();

		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.getFirst().startsWith("WARN " + stepId + ": failed to get the remote log file name"), warnings.toString());
		verify(unnamed, never()).readFromFile(anyString(), anyLong());
		verify(named).readFromFile(file, 0L);
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

	/**
	 * Collecting a step's worker traces called directly, from a thread whose log context is left over from an
	 * earlier step, writes every trace row and every warning under this step's id and the aggregator's name.
	 */
	@Test
	public void collectToLocalRoutesEveryRowAndWarningToItsOwnStep() throws Exception {
		assertTracesAndWarningsGoToTheirStep(OpTraceLogFileAggregator::collectToLocal);
	}

	/** The same for the close at the end of the step. */
	@Test
	public void closeRoutesEveryRowAndWarningToItsOwnStep() throws Exception {
		assertTracesAndWarningsGoToTheirStep(OpTraceLogFileAggregator::close);
	}

	/**
	 * Two workers: the first returns three rows, then fails a read (an ERROR); the second returns three rows in
	 * two parts, then fails the delete (a WARN). Each worker's first read waits up to 2 s for the other's, so that
	 * transfers made in parallel overlap and one of them runs off the calling thread, whatever the scheduling;
	 * transfers made one after another pass after the first wait times out. On a common pool thread the log
	 * context is the one the thread inherited from whichever thread created it, so no transfer may run there.
	 */
	private static void assertTracesAndWarningsGoToTheirStep(final Consumer<OpTraceLogFileAggregator> collect)
					throws Exception {
		final String stepId = "routed-step-" + UUID.randomUUID();
		final String className = OpTraceLogFileAggregator.class.getSimpleName();
		final String fileA = "/w1/" + stepId + "/op.trace.csv";
		final String fileB = "/w2/" + stepId + "/op.trace.csv";
		final byte[] rowsA = rows(stepId, "a", 3);
		final byte[] rowsB1 = rows(stepId, "b1", 2);
		final byte[] rowsB2 = rows(stepId, "b2", 1);
		final CountDownLatch bothReading = new CountDownLatch(2);
		final List<String> calls = new CopyOnWriteArrayList<>();
		final FileManagerService workerA = mock(FileManagerService.class);
		when(workerA.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenReturn(fileA);
		doAnswer(invocation -> {
			recordCall(calls, "read " + fileA);
			bothReading.countDown();
			bothReading.await(2, TimeUnit.SECONDS);
			return rowsA;
		}).when(workerA).readFromFile(fileA, 0L);
		when(workerA.readFromFile(fileA, rowsA.length)).thenThrow(new IOException(stepId + " read failed"));
		doAnswer(invocation -> {
			recordCall(calls, "delete " + fileA);
			return null;
		}).when(workerA).deleteFile(fileA);
		final FileManagerService workerB = mock(FileManagerService.class);
		when(workerB.logFileName(Loggers.OP_TRACES.getName(), stepId)).thenReturn(fileB);
		doAnswer(invocation -> {
			recordCall(calls, "read " + fileB);
			bothReading.countDown();
			bothReading.await(2, TimeUnit.SECONDS);
			return rowsB1;
		}).when(workerB).readFromFile(fileB, 0L);
		when(workerB.readFromFile(fileB, rowsB1.length)).thenReturn(rowsB2);
		when(workerB.readFromFile(fileB, rowsB1.length + rowsB2.length)).thenThrow(new EOFException());
		doAnswer(invocation -> {
			recordCall(calls, "delete " + fileB);
			throw new IOException(stepId + " delete failed");
		}).when(workerB).deleteFile(fileB);
		final var aggregator = new OpTraceLogFileAggregator(stepId, List.of(FileManager.INSTANCE, workerA, workerB));

		final List<CapturedEvent> events;
		try (var levels = LoggerLevels.atMost(Level.WARN, Loggers.MSG.getName(), Loggers.ERR.getName());
						var stale = CloseableThreadContext.put(KEY_STEP_ID, "stale-step-" + UUID.randomUUID())
										.put(KEY_CLASS_NAME, "LinearLoadStepClient")) {
			events = eventsWhile(stepId, () -> collect.accept(aggregator));
		}

		verify(workerA).deleteFile(fileA);
		verify(workerB).deleteFile(fileB);
		final String expectedContext = " step " + stepId + " class " + className;
		assertEquals(4, calls.size(), calls.toString());
		for (final String call : calls) {
			assertFalse(call.contains(" on common pool "), call);
			assertTrue(call.endsWith(expectedContext), call);
		}
		final List<CapturedEvent> traces = events.stream().filter(e -> e.logger.equals(Loggers.OP_TRACES.getName())).toList();
		final List<CapturedEvent> warnings = events.stream().filter(e -> !e.logger.equals(Loggers.OP_TRACES.getName())).toList();
		assertEquals(6, traces.stream().mapToLong(e -> e.message.chars().filter(c -> c == '\n').count()).sum(), traces.toString());
		assertEquals(2, warnings.size(), warnings.toString());
		assertTrue(warnings.stream().anyMatch(e -> Level.ERROR.equals(e.level) && e.message.contains(stepId + " read failed")), warnings.toString());
		assertTrue(warnings.stream().anyMatch(e -> Level.WARN.equals(e.level) && e.message.contains(stepId + ": failed to delete")), warnings.toString());
		for (final CapturedEvent event : events) {
			assertEquals(stepId, event.stepId, event.toString());
			assertEquals(className, event.className, event.toString());
		}
	}

	private static byte[] rows(final String stepId, final String part, final int count) {
		final var rows = new StringBuilder();
		for (var i = 0; i < count; i++) {
			rows.append(stepId).append(',').append(part).append('-').append(i).append('\n');
		}
		return rows.toString().getBytes(StandardCharsets.UTF_8);
	}

	private static void recordCall(final List<String> calls, final String call) {
		calls.add(call
						+ (Thread.currentThread() instanceof ForkJoinWorkerThread ? " on common pool " : " on ")
						+ Thread.currentThread().getName()
						+ " step " + ThreadContext.get(KEY_STEP_ID)
						+ " class " + ThreadContext.get(KEY_CLASS_NAME));
	}

	private record CapturedEvent(String logger, Level level, String message, String stepId, String className) {}

	/**
	 * The trace rows, and the WARN and ERROR messages, about {@code token} that the trace, message and error
	 * loggers received while {@code action} ran, with the step id and class name of their log context.
	 */
	private static List<CapturedEvent> eventsWhile(final String token, final Runnable action) throws InterruptedException {
		final var context = LoggerContext.getContext(false);
		final List<Logger> loggers = List.of(
						context.getLogger(Loggers.OP_TRACES.getName()),
						context.getLogger(Loggers.MSG.getName()),
						context.getLogger(Loggers.ERR.getName()));
		final String endOfLog = token + ": end of log";
		final List<CapturedEvent> events = new CopyOnWriteArrayList<>();
		final var capture = new AbstractAppender("opTraceAggregatorRoutingCapture", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final boolean trace = Loggers.OP_TRACES.getName().equals(event.getLoggerName());
				final String message = event.getMessage().getFormattedMessage();
				if ((trace || event.getLevel().isMoreSpecificThan(Level.WARN)) && message.contains(token)) {
					events.add(new CapturedEvent(
									event.getLoggerName(),
									event.getLevel(),
									message,
									event.getContextData().getValue(KEY_STEP_ID),
									event.getContextData().getValue(KEY_CLASS_NAME)));
				}
			}
		};
		capture.start();
		loggers.forEach(logger -> logger.addAppender(capture));
		try {
			action.run();
			// The loggers may be asynchronous: this line comes after everything the action logged.
			Loggers.ERR.error(endOfLog);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (events.stream().noneMatch(e -> e.message.equals(endOfLog))) {
				assertTrue(System.nanoTime() < deadline, "the log did not reach its end: " + events);
				Thread.sleep(10);
			}
			return events.stream().filter(e -> !e.message.equals(endOfLog)).toList();
		} finally {
			loggers.forEach(logger -> logger.removeAppender(capture));
			capture.stop();
		}
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
	 * messages, as from threads of other tests in this JVM, are left out. Both loggers let WARN
	 * through meanwhile, whatever level an earlier test in this JVM left them at.
	 */
	private static List<String> warningsWhile(final String token, final Runnable action) throws InterruptedException {
		try (var levels = LoggerLevels.atMost(Level.WARN, Loggers.MSG.getName(), Loggers.ERR.getName())) {
			return capturedWhile(token, action);
		}
	}

	private static List<String> capturedWhile(final String token, final Runnable action) throws InterruptedException {
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
			if (message.contains(token) || (thrown != null && String.valueOf(thrown).contains(token))) {
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
