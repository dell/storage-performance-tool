package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.load.step.file.FileManager;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * Tests for the helper logic in {@link ItemOutputFileAggregator}.
 */
public class ItemOutputFileAggregatorTest {

	@Test
	public void transferToLocalStopsOnEofAndCountsBytes() throws Exception {
		final Method method = ItemOutputFileAggregator.class.getDeclaredMethod(
						"transferToLocal",
						FileManager.class,
						String.class,
						OutputStream.class,
						Lock.class,
						LongAdder.class);
		method.setAccessible(true);

		final RecordingFileManager fileManager = new RecordingFileManager("remote.out", new byte[]{5, 6, 7
		});
		final ByteArrayOutputStream local = new ByteArrayOutputStream();
		final LongAdder counter = new LongAdder();

		method.invoke(null, fileManager, "remote.out", local, new ReentrantLock(), counter);

		assertArrayEquals(new byte[]{5, 6, 7
		}, local.toByteArray());
		assertEquals(3L, counter.longValue());
		assertEquals(1, fileManager.transferCount);
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

		// Remaining interface methods are unused in this test.
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
	}
}
