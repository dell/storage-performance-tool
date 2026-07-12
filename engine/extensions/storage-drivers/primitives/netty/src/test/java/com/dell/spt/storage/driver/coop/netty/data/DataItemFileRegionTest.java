package com.dell.spt.storage.driver.coop.netty.data;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.github.akurilov.commons.system.SizeInBytes;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

class DataItemFileRegionTest {

	@Test
	void partialWritesFollowFileRegionProgressContract() throws Exception {
		try (final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 1, false)) {
			final var dataItem = new DataItemImpl("item", 0, 26);
			dataItem.dataInput(dataInput);
			dataItem.position(3);
			final var region = new DataItemFileRegion(dataItem);
			final var target = new LimitedWritableByteChannel(5);
			final var transferResults = new ArrayList<Long>();
			final var transferredProgress = new ArrayList<Long>();
			final var regionPositions = new ArrayList<Long>();

			try {
				final long initialPosition = region.position();
				while (target.totalBytesWritten() < region.count()) {
					final long actualProgress = target.totalBytesWritten();
					transferResults.add(region.transferTo(target, actualProgress));
					transferredProgress.add(region.transferred());
					regionPositions.add(region.position());
				}
				final long resultAfterCompletion = region.transferTo(target, target.totalBytesWritten());

				assertAll(
								() -> assertEquals(3, initialPosition),
								() -> assertEquals(23, region.count()),
								() -> assertEquals(region.count(), target.totalBytesWritten()),
								() -> assertEquals(region.count(), region.transferred()),
								() -> assertIterableEquals(target.bytesWrittenPerCall(), transferResults),
								() -> assertIterableEquals(target.cumulativeBytesWritten(), transferredProgress),
								() -> assertTrue(regionPositions.stream().allMatch(position -> position == initialPosition)),
								() -> assertTrue(transferredProgress.stream().allMatch(progress -> progress <= region.count())),
								() -> assertEquals(0, resultAfterCompletion));
			} finally {
				region.release();
				target.close();
			}
		}
	}

	private static final class LimitedWritableByteChannel implements WritableByteChannel {
		private final int maxBytesPerWrite;
		private final List<Long> bytesWrittenPerCall = new ArrayList<>();
		private final List<Long> cumulativeBytesWritten = new ArrayList<>();
		private long totalBytesWritten;
		private boolean open = true;

		private LimitedWritableByteChannel(final int maxBytesPerWrite) {
			this.maxBytesPerWrite = maxBytesPerWrite;
		}

		@Override
		public int write(final ByteBuffer source) {
			final int bytesWritten = Math.min(maxBytesPerWrite, source.remaining());
			source.position(source.position() + bytesWritten);
			totalBytesWritten += bytesWritten;
			bytesWrittenPerCall.add((long) bytesWritten);
			cumulativeBytesWritten.add(totalBytesWritten);
			return bytesWritten;
		}

		private List<Long> bytesWrittenPerCall() {
			return bytesWrittenPerCall;
		}

		private List<Long> cumulativeBytesWritten() {
			return cumulativeBytesWritten;
		}

		private long totalBytesWritten() {
			return totalBytesWritten;
		}

		@Override
		public boolean isOpen() {
			return open;
		}

		@Override
		public void close() {
			open = false;
		}
	}
}
