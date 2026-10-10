package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.DataItem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copies a PUT payload from its data item into the registered buffer.
 *
 * <p>With more than one thread, a large payload is split into parts that are copied at the same
 * time: the calling thread copies the first part and a fixed pool copies the others. A part starts
 * on a {@link #PART_BYTES} boundary, where a slice of the item yields the same bytes as the item
 * itself. {@link #copy} returns only after every part has finished, so the caller may release the
 * buffer as soon as it returns or throws.
 */
final class RdmaPayloadCopier implements AutoCloseable {

	/** Smallest part, and the boundary parts start on: a multiple of the data item's stamp chunk. */
	static final int PART_BYTES = 1 << 20;

	private static final long CLOSE_WAIT_SECONDS = 5;

	private final int threads;
	/** Copies every part but the first; {@code null} when the calling thread copies alone. */
	private final ExecutorService helpers;

	/**
	 * @param threads threads copying one payload, the calling thread included
	 * @param threadNamePrefix name prefix of the pool's threads
	 */
	RdmaPayloadCopier(final int threads, final String threadNamePrefix) {
		this.threads = threads;
		if (threads < 2) {
			helpers = null;
		} else {
			final AtomicInteger sequence = new AtomicInteger();
			helpers = Executors.newFixedThreadPool(threads - 1, task -> {
				final Thread thread = new Thread(task, threadNamePrefix + sequence.getAndIncrement());
				thread.setDaemon(true);
				return thread;
			});
		}
	}

	/**
	 * Fills {@code dst} with the next {@code size} bytes of {@code item} and flips it. The item's
	 * position advances by {@code size}.
	 */
	void copy(final DataItem item, final ByteBuffer dst, final int size) throws IOException {
		dst.clear();
		dst.limit(size);
		// A slice restarts the item's stamp chunks, so only a copy from the item's start is split.
		final int parts = helpers == null || item.position() != 0 ? 1 : Math.min(threads, size / PART_BYTES);
		if (parts < 2) {
			fill(item, dst, size);
		} else {
			copyInParts(item, dst, size, parts);
			item.position(size);
			dst.position(size);
		}
		dst.flip();
	}

	private void copyInParts(final DataItem item, final ByteBuffer dst, final int size, final int parts)
					throws IOException {
		final int partSize = size / parts / PART_BYTES * PART_BYTES;
		final Future<?>[] pending = new Future<?>[parts - 1];
		Throwable failure = null;
		try {
			for (int i = 1; i < parts; i++) {
				final int from = i * partSize;
				final int length = i == parts - 1 ? size - from : partSize;
				// Sliced and windowed here, so no thread shares the item's or the buffer's position.
				final DataItem part = item.slice(from, length);
				final ByteBuffer window = dst.duplicate().limit(from + length).position(from);
				pending[i - 1] = helpers.submit(() -> {
					fill(part, window, length);
					return null;
				});
			}
			fill(item.slice(0, partSize), dst.duplicate().limit(partSize), partSize);
		} catch (final Throwable thrown) {
			failure = thrown;
		}
		// Every submitted part is awaited, also after a failure or an interrupt: once this method
		// returns, the caller may hand the buffer to another operation.
		boolean interrupted = false;
		for (final Future<?> part : pending) {
			while (part != null) {
				try {
					part.get();
					break;
				} catch (final InterruptedException e) {
					interrupted = true;
				} catch (final ExecutionException e) {
					if (failure == null) {
						failure = e.getCause();
					}
					break;
				}
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
		if (failure instanceof IOException ioFailure) {
			throw ioFailure;
		}
		if (failure instanceof RuntimeException runtimeFailure) {
			throw runtimeFailure;
		}
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure != null) {
			throw new IOException("RDMA payload copy failed", failure);
		}
	}

	private static void fill(final DataItem item, final ByteBuffer dst, final int size) throws IOException {
		int totalRead = 0;
		while (totalRead < size) {
			final int bytesRead = item.read(dst);
			if (bytesRead < 0) {
				break;
			}
			totalRead += bytesRead;
		}
		if (totalRead < size) {
			throw new IOException("RDMA short read: expected " + size + " bytes but got " + totalRead);
		}
	}

	/** Ends the pool's threads. Parts already submitted are still copied. */
	@Override
	public void close() {
		if (helpers == null) {
			return;
		}
		helpers.shutdown();
		try {
			final boolean unused = helpers.awaitTermination(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** Whether the pool's threads have ended; true when there is no pool. */
	boolean isTerminated() {
		return helpers == null || helpers.isTerminated();
	}
}
