package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * A payload copied in parts holds the same bytes as one copied by a single thread, and the copy
 * ends only when no thread can still write to the buffer.
 */
class RdmaPayloadCopierTest {

	private static final int PART = RdmaPayloadCopier.PART_BYTES;
	private static final long WAIT_SECONDS = 5;
	private static final long STILL_WAITING_MILLIS = 200;
	/** Not a divisor of the part size, so a part copied from the wrong offset holds other bytes. */
	private static final SizeInBytes RING_SIZE = new SizeInBytes("100KB");
	private static final long ITEM_OFFSET = 12_345;

	@Test
	void splitCopyHoldsTheSameBytesAsASingleThreadCopy() throws Exception {
		final int[] sizes = {2 * PART, 2 * PART + 1, 3 * PART - 1, 5 * PART + 12_345, 8 * PART
		};
		for (final boolean dedupable : new boolean[]{true, false
		}) {
			try (final DataInput input = input(dedupable); final var single = new RdmaPayloadCopier(1, "unused-")) {
				for (final int size : sizes) {
					final byte[] expected = copy(single, item(input, size), size);
					for (final int threads : new int[]{2, 3, 8
					}) {
						try (final var copier = new RdmaPayloadCopier(threads, "rdma-copy-same-bytes-")) {
							final var item = item(input, size);
							assertArrayEquals(expected, copy(copier, item, size),
											"dedupable=" + dedupable + ", size=" + size + ", threads=" + threads);
							assertTrue(item.slices > 0, "the payload must be split");
							assertEquals(size, item.position());
						}
					}
				}
			}
		}
	}

	@Test
	void partsAreCopiedAtTheSameTimeByTheCallerAndThePool() throws Exception {
		final int parts = 4;
		final int size = parts * PART + 7;
		final String prefix = "rdma-copy-concurrent-";
		final Set<String> copyingThreads = ConcurrentHashMap.newKeySet();
		// Every part waits for all the others, so the copy completes only if they run concurrently.
		final CyclicBarrier allPartsCopying = new CyclicBarrier(parts);
		final DataItem item = Mockito.mock(DataItem.class);
		Mockito.when(item.slice(anyLong(), anyLong())).thenAnswer(slice -> {
			final long from = slice.getArgument(0);
			return part(dst -> {
				copyingThreads.add(Thread.currentThread().getName());
				allPartsCopying.await(WAIT_SECONDS, TimeUnit.SECONDS);
				return fill(dst, (byte) (from / PART));
			});
		});
		final ByteBuffer dst = ByteBuffer.allocateDirect(size);

		try (final var copier = new RdmaPayloadCopier(parts, prefix)) {
			copier.copy(item, dst, size);
		}

		assertEquals(parts, copyingThreads.size(), copyingThreads.toString());
		assertTrue(copyingThreads.remove(Thread.currentThread().getName()), "the caller copies one part");
		copyingThreads.forEach(name -> assertTrue(name.startsWith(prefix), name));
		assertEquals(0, dst.position());
		assertEquals(size, dst.limit());
		for (int i = 0; i < size; i++) {
			// The last part also takes the bytes beyond the last boundary.
			assertEquals(Math.min(i / PART, parts - 1), dst.get(i), "byte " + i);
		}
	}

	@Test
	void payloadBelowTwoPartsIsCopiedByTheCallerAlone() throws Exception {
		final int size = 2 * PART - 1;
		try (final DataInput input = input(false);
						final var single = new RdmaPayloadCopier(1, "unused-");
						final var copier = new RdmaPayloadCopier(4, "rdma-copy-small-")) {
			final var item = item(input, size);

			assertArrayEquals(copy(single, item(input, size), size), copy(copier, item, size));
			assertEquals(0, item.slices);
		}
	}

	@Test
	void itemNotAtItsStartIsCopiedByTheCallerAlone() throws Exception {
		final int size = 4 * PART;
		final long start = 4096 + 7;
		try (final DataInput input = input(false);
						final var single = new RdmaPayloadCopier(1, "unused-");
						final var copier = new RdmaPayloadCopier(4, "rdma-copy-position-")) {
			final var expectedSource = item(input, size);
			expectedSource.position(start);
			final var item = item(input, size);
			item.position(start);

			assertArrayEquals(copy(single, expectedSource, size), copy(copier, item, size));
			assertEquals(0, item.slices);
			assertEquals(start + size, item.position());
		}
	}

	@Test
	void failureOfTheCallersPartIsReportedOnlyAfterThePoolFinished() throws Exception {
		final int size = 2 * PART;
		final var poolPartCopying = new CountDownLatch(1);
		final var releasePoolPart = new CountDownLatch(1);
		final var failure = new IOException("caller's part failed");
		final DataItem item = Mockito.mock(DataItem.class);
		Mockito.when(item.slice(eq(0L), anyLong())).thenAnswer(slice -> part(dst -> {
			throw failure;
		}));
		Mockito.when(item.slice(eq((long) PART), anyLong())).thenAnswer(slice -> part(dst -> {
			poolPartCopying.countDown();
			releasePoolPart.await();
			return fill(dst, (byte) 1);
		}));
		final var thrown = new AtomicReference<Throwable>();
		try (final var copier = new RdmaPayloadCopier(2, "rdma-copy-failure-")) {
			final Thread caller = new Thread(() -> {
				try {
					copier.copy(item, ByteBuffer.allocateDirect(size), size);
				} catch (final Throwable t) {
					thrown.set(t);
				}
			});
			caller.start();
			assertTrue(poolPartCopying.await(WAIT_SECONDS, TimeUnit.SECONDS));

			caller.join(STILL_WAITING_MILLIS);
			assertTrue(caller.isAlive(), "the copy must not end while a part is still being written");

			releasePoolPart.countDown();
			caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
			assertFalse(caller.isAlive());
		}
		assertSame(failure, thrown.get());
	}

	@Test
	void failureOfAPoolPartIsThrownToTheCaller() throws Exception {
		final int size = 2 * PART;
		final var failure = new IOException("pool part failed");
		final DataItem item = Mockito.mock(DataItem.class);
		Mockito.when(item.slice(eq(0L), anyLong())).thenAnswer(slice -> part(dst -> fill(dst, (byte) 0)));
		Mockito.when(item.slice(eq((long) PART), anyLong())).thenAnswer(slice -> part(dst -> {
			throw failure;
		}));
		try (final var copier = new RdmaPayloadCopier(2, "rdma-copy-pool-failure-")) {
			assertSame(failure, assertThrows(
							IOException.class, () -> copier.copy(item, ByteBuffer.allocateDirect(size), size)));
		}
	}

	@Test
	void interruptedCallerStillWaitsForEveryPart() throws Exception {
		final int size = 2 * PART;
		final var poolPartCopying = new CountDownLatch(1);
		final var releasePoolPart = new CountDownLatch(1);
		final DataItem item = Mockito.mock(DataItem.class);
		Mockito.when(item.slice(eq(0L), anyLong())).thenAnswer(slice -> part(dst -> fill(dst, (byte) 0)));
		Mockito.when(item.slice(eq((long) PART), anyLong())).thenAnswer(slice -> part(dst -> {
			poolPartCopying.countDown();
			releasePoolPart.await();
			return fill(dst, (byte) 1);
		}));
		final var thrown = new AtomicReference<Throwable>();
		final var interruptKept = new AtomicBoolean();
		final ByteBuffer dst = ByteBuffer.allocateDirect(size);
		try (final var copier = new RdmaPayloadCopier(2, "rdma-copy-interrupt-")) {
			final Thread caller = new Thread(() -> {
				try {
					copier.copy(item, dst, size);
				} catch (final Throwable t) {
					thrown.set(t);
				}
				interruptKept.set(Thread.currentThread().isInterrupted());
			});
			caller.start();
			assertTrue(poolPartCopying.await(WAIT_SECONDS, TimeUnit.SECONDS));

			caller.interrupt();
			caller.join(STILL_WAITING_MILLIS);
			assertTrue(caller.isAlive(), "an interrupt must not end the copy while a part is still being written");

			releasePoolPart.countDown();
			caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
			assertFalse(caller.isAlive());
		}
		assertNull(thrown.get());
		assertTrue(interruptKept.get(), "the caller's interrupt must be kept");
		assertEquals(1, dst.get(size - 1), "the pool's part was copied");
	}

	@Test
	void singleThreadCopierStartsNoThreads() throws Exception {
		final String prefix = "rdma-copy-single-";
		final int size = 4 * PART;
		try (final DataInput input = input(true); final var copier = new RdmaPayloadCopier(1, prefix)) {
			final var item = item(input, size);
			final byte[] unused = copy(copier, item, size);

			assertEquals(0, item.slices);
			assertEquals(0, liveThreads(prefix));
			assertTrue(copier.isTerminated());
		}
	}

	@Test
	void closeEndsThePoolThreads() throws Exception {
		final String prefix = "rdma-copy-close-";
		final int threads = 4;
		final int size = threads * PART;
		try (final DataInput input = input(true)) {
			final var copier = new RdmaPayloadCopier(threads, prefix);
			for (int i = 0; i < 3; i++) {
				final byte[] unused = copy(copier, item(input, size), size);
			}
			assertEquals(threads - 1, liveThreads(prefix), "the pool is bounded by the configured threads");

			copier.close();

			assertTrue(copier.isTerminated());
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
			while (liveThreads(prefix) > 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, liveThreads(prefix));
		}
	}

	@Test
	void splitCopyAfterCloseFailsInsteadOfWaiting() throws Exception {
		final int size = 2 * PART;
		try (final DataInput input = input(true)) {
			final var copier = new RdmaPayloadCopier(2, "rdma-copy-closed-");
			copier.close();

			assertThrows(RejectedExecutionException.class,
							() -> copier.copy(item(input, size), ByteBuffer.allocateDirect(size), size));
		}
	}

	private interface PartRead {
		int read(ByteBuffer dst) throws Exception;
	}

	/** A slice whose {@code read} is {@code read}. */
	private static DataItem part(final PartRead read) throws IOException {
		final DataItem part = Mockito.mock(DataItem.class);
		Mockito.when(part.read(any(ByteBuffer.class))).thenAnswer((Answer<Integer>) invocation -> read.read(invocation.getArgument(0)));
		return part;
	}

	private static int fill(final ByteBuffer dst, final byte value) {
		final int count = dst.remaining();
		while (dst.hasRemaining()) {
			dst.put(value);
		}
		return count;
	}

	private static DataInput input(final boolean dedupable) throws IOException {
		return DataInput.instance(null, "7a42d9c483244167", RING_SIZE, 16, false, 0.0, dedupable);
	}

	private static SliceCountingItem item(final DataInput input, final long size) {
		final var item = new SliceCountingItem(size);
		item.dataInput(input);
		return item;
	}

	/** Copies into a buffer larger than the payload and returns the payload. */
	private static byte[] copy(final RdmaPayloadCopier copier, final DataItem item, final int size) throws IOException {
		final ByteBuffer dst = ByteBuffer.allocateDirect(size + PART);
		copier.copy(item, dst, size);
		assertEquals(0, dst.position());
		assertEquals(size, dst.limit());
		final byte[] payload = new byte[size];
		dst.get(payload);
		return payload;
	}

	private static long liveThreads(final String prefix) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(thread -> thread.isAlive() && thread.getName().startsWith(prefix))
						.count();
	}

	private static final class SliceCountingItem extends DataItemImpl {

		int slices;

		SliceCountingItem(final long size) {
			super("obj", ITEM_OFFSET, size);
		}

		@Override
		public DataItemImpl slice(final long from, final long partSize) {
			slices++;
			return super.slice(from, partSize);
		}
	}
}
