package com.dell.spt.base.load.step.file;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@link FileManagerImpl#writeToFile} writes the whole buffer when a write takes only part of it. */
class FileManagerImplWriteTest {

	@TempDir
	Path dir;

	/** Takes at most {@code chunk} bytes per write, as a channel may. */
	private static class ShortWriteChannel implements WritableByteChannel {

		final ByteArrayOutputStream written = new ByteArrayOutputStream();
		int writes;
		private final int chunk;

		ShortWriteChannel(final int chunk) {
			this.chunk = chunk;
		}

		@Override
		public int write(final ByteBuffer src) {
			writes++;
			final int n = Math.min(chunk, src.remaining());
			for (int i = 0; i < n; i++) {
				written.write(src.get());
			}
			return n;
		}

		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close() {}
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
	void aShortWriteIsFollowedByTheRestAndThenEnds() throws Exception {
		final byte[] record = "x".repeat(100).getBytes(StandardCharsets.UTF_8);
		// 40 bytes, then the remaining 60: the old loop compared the 60 with 100 and spun on the empty buffer.
		final var channel = new ShortWriteChannel(60) {
			@Override
			public int write(final ByteBuffer src) {
				if (writes == 0) {
					writes++;
					src.position(src.position() + 40);
					written.write(record, 0, 40);
					return 40;
				}
				return super.write(src);
			}
		};

		FileManagerImpl.writeFully(channel, ByteBuffer.wrap(record));

		assertArrayEquals(record, channel.written.toByteArray());
		assertEquals(2, channel.writes);
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
	void manyShortWritesWriteEveryByte() throws Exception {
		final byte[] record = "0123456789".repeat(10).getBytes(StandardCharsets.UTF_8);
		final var channel = new ShortWriteChannel(7);

		FileManagerImpl.writeFully(channel, ByteBuffer.wrap(record));

		assertArrayEquals(record, channel.written.toByteArray());
		assertEquals(15, channel.writes);
	}

	@Test
	void aFileGetsItsAppendsWhole() throws Exception {
		final var file = dir.resolve("slice.csv");
		final var manager = new FileManagerImpl();

		manager.writeToFile(file.toString(), "a,1\n".getBytes(StandardCharsets.UTF_8));
		manager.writeToFile(file.toString(), "b,2\n".getBytes(StandardCharsets.UTF_8));
		manager.writeToFile(file.toString(), new byte[0]);

		assertEquals("a,1\nb,2\n", Files.readString(file));
	}
}
