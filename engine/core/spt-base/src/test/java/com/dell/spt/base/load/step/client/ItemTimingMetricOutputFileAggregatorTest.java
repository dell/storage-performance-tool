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
import org.junit.jupiter.api.Test;

/**
 * Verifies error-prone compliant handling within {@link ItemTimingMetricOutputFileAggregator}.
 */
public class ItemTimingMetricOutputFileAggregatorTest {

	@Test
	public void transferToLocalStopsOnEofAndCountsBytes() throws Exception {
		final Method method = ItemTimingMetricOutputFileAggregator.class.getDeclaredMethod(
						"transferToLocal", FileManager.class, String.class, OutputStream.class, LongAdder.class);
		method.setAccessible(true);
		final RecordingFileManager fileManager = new RecordingFileManager("remote-file", new byte[]{1, 2, 3, 4
		});
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final LongAdder bytes = new LongAdder();

		method.invoke(null, fileManager, "remote-file", out, bytes);

		assertArrayEquals(new byte[]{1, 2, 3, 4
		}, out.toByteArray());
		assertEquals(4L, bytes.longValue());
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

		// Methods below are not exercised in this test and simply satisfy the interface.
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
