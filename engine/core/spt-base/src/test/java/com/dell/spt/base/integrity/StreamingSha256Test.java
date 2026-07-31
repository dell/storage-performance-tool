package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamingSha256Test {

	@Test
	void hashesExactFinalBytesAndAlwaysResetsTheItem() throws Exception {
		final byte[] bytes = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		final var item = new DataItemImpl("object", 0, bytes.length);
		item.dataInput(new FixedDataInput(bytes));
		item.position(2);

		try (final var hasher = new StreamingSha256(1)) {
			final var result = hasher.hash(item);
			assertEquals(
							"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
							result.metadata().digest());
			assertEquals(3, result.metadata().size());
			assertTrue(result.workerNanos() >= 0);
			assertEquals(0, item.position());
		}
	}

	@Test
	void usesAReusableBoundedWorkerSetAndRejectsUseAfterClose() {
		final var hasher = new StreamingSha256(2);
		assertEquals(2, hasher.workerCount());
		hasher.close();
		final var item = new DataItemImpl("empty", 0, 0);
		assertThrows(java.io.IOException.class, () -> hasher.hash(item));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 1, StreamingSha256.BUFFER_SIZE - 1, StreamingSha256.BUFFER_SIZE,
			StreamingSha256.BUFFER_SIZE + 1, StreamingSha256.BUFFER_SIZE * 2 + 17
	})
	void matchesReferenceDigestAcrossStreamingBufferBoundaries(final int size) throws Exception {
		final byte[] bytes = new byte[size];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) (i * 31 + 7);
		}
		final var item = new DataItemImpl("boundary-" + size, 0, size);
		item.dataInput(new FixedDataInput(bytes));
		final String expected = HexFormat.of().formatHex(
						MessageDigest.getInstance("SHA-256").digest(bytes));

		try (final var hasher = new StreamingSha256(1)) {
			final var result = hasher.hash(item);
			assertEquals(expected, result.metadata().digest());
			assertEquals(size, result.metadata().size());
			assertEquals(0, item.position());
		}
	}

	@Test
	void closeWakesAllWaitingCallersAndAllowsInFlightDigestToFinish() throws Exception {
		final var enteredRead = new CountDownLatch(1);
		final var releaseRead = new CountDownLatch(1);
		final DataItem blockingItem = mock(DataItem.class);
		when(blockingItem.size()).thenReturn(1L);
		doNothing().when(blockingItem).reset();
		when(blockingItem.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
			enteredRead.countDown();
			releaseRead.await(5, TimeUnit.SECONDS);
			invocation.<ByteBuffer> getArgument(0).put((byte) 1);
			return 1;
		});
		final DataItem waitingItem = mock(DataItem.class);
		when(waitingItem.size()).thenReturn(0L);
		doNothing().when(waitingItem).reset();
		final var hasher = new StreamingSha256(1);
		final var executor = Executors.newFixedThreadPool(3);
		try {
			final var inFlight = executor.submit(() -> hasher.hash(blockingItem));
			assertTrue(enteredRead.await(5, TimeUnit.SECONDS));
			final var waiterOne = executor.submit(() -> hasher.hash(waitingItem));
			final var waiterTwo = executor.submit(() -> hasher.hash(waitingItem));
			hasher.close();
			hasher.close();
			final var failureOne = assertThrows(
							java.util.concurrent.ExecutionException.class,
							() -> waiterOne.get(5, TimeUnit.SECONDS));
			final var failureTwo = assertThrows(
							java.util.concurrent.ExecutionException.class,
							() -> waiterTwo.get(5, TimeUnit.SECONDS));
			assertTrue(failureOne.getCause() instanceof java.io.IOException);
			assertTrue(failureTwo.getCause() instanceof java.io.IOException);
			releaseRead.countDown();
			assertEquals(1, inFlight.get(5, TimeUnit.SECONDS).metadata().size());
		} finally {
			releaseRead.countDown();
			hasher.close();
			executor.shutdownNow();
		}
	}

	@Test
	void failureCarriesPartialBytesAndWorkerTime() throws Exception {
		final DataItem item = mock(DataItem.class);
		when(item.size()).thenReturn(2L);
		doNothing().when(item).reset();
		final var reads = new AtomicInteger();
		when(item.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
			if (reads.getAndIncrement() == 0) {
				invocation.<ByteBuffer> getArgument(0).put((byte) 1);
				return 1;
			}
			return 0;
		});
		try (final var hasher = new StreamingSha256(1)) {
			final var failure = assertThrows(
							StreamingSha256.DigestFailureException.class, () -> hasher.hash(item));
			assertEquals(1, failure.processedBytes());
			assertTrue(failure.workerNanos() >= 0);
		}
	}

	private static final class FixedDataInput implements DataInput {
		private final ByteBuffer data;

		private FixedDataInput(final byte[] data) {
			this.data = ByteBuffer.wrap(data);
		}

		@Override
		public ByteBuffer getLayer(final int layerIdx) {
			return data.asReadOnlyBuffer();
		}

		@Override
		public int getSize() {
			return data.capacity();
		}

		@Override
		public void close() {}
	}
}
