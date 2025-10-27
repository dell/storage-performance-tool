package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.load.step.file.FileManager;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.LongAdder;
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
