package com.dell.spt.base.integrity;

import com.dell.spt.base.item.DataItem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded reusable SHA-256 workers for full {@link DataItem} streams. No object-sized buffer is
 * created and every item is reset even when digesting fails.
 */
public final class StreamingSha256 implements AutoCloseable {

	public static final int BUFFER_SIZE = 64 * 1024;
	private static final String JCA_ALGORITHM = "SHA-256";
	private static final Worker CLOSED_WORKER = Worker.closedSentinel();

	private final int workerCount;
	private final ArrayBlockingQueue<Worker> workers;
	private final AtomicBoolean closed = new AtomicBoolean();

	public StreamingSha256(final int workerCount) {
		if (workerCount < 1) {
			throw new IllegalArgumentException("workerCount must be positive");
		}
		this.workerCount = workerCount;
		workers = new ArrayBlockingQueue<>(workerCount);
		try {
			for (int i = 0; i < workerCount; i++) {
				workers.add(Worker.digestWorker());
			}
		} catch (final NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable", e);
		}
	}

	public int workerCount() {
		return workerCount;
	}

	public DigestResult hash(final DataItem item) throws IOException {
		if (item == null) {
			throw new IllegalArgumentException("item must not be null");
		}
		if (closed.get()) {
			throw new IOException("integrity digest workers are closed");
		}
		final long expectedSize = item.size();
		if (expectedSize < 0) {
			throw new IOException("cannot digest an item with negative size");
		}

		final Worker worker;
		try {
			worker = workers.take();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("interrupted while waiting for an integrity digest worker", e);
		}

		if (worker == CLOSED_WORKER) {
			workers.offer(CLOSED_WORKER);
			throw new IOException("integrity digest workers are closed");
		}

		final long started = System.nanoTime();
		long processedBytes = 0;
		byte[] digest = null;
		IOException failure = null;
		try {
			worker.digest.reset();
			item.reset();
			long remaining = expectedSize;
			while (remaining > 0) {
				worker.buffer.clear();
				worker.buffer.limit((int) Math.min(worker.buffer.capacity(), remaining));
				final int read = item.read(worker.buffer);
				if (read <= 0) {
					throw new IOException(
									"data item made no progress with " + remaining + " digest bytes remaining");
				}
				remaining -= read;
				processedBytes += read;
				worker.buffer.flip();
				worker.digest.update(worker.buffer);
			}
			digest = worker.digest.digest();
		} catch (final IOException e) {
			failure = e;
		} catch (final RuntimeException e) {
			failure = new IOException("failed to compute the object integrity digest", e);
		} finally {
			try {
				item.reset();
			} catch (final RuntimeException e) {
				if (failure == null) {
					failure = new IOException("failed to reset the item after integrity digesting", e);
				} else {
					failure.addSuppressed(e);
				}
			}
			if (!closed.get()) {
				workers.offer(worker);
			}
		}
		final long elapsed = System.nanoTime() - started;
		if (failure != null) {
			throw new DigestFailureException(failure, processedBytes, elapsed);
		}
		final IntegrityMetadata metadata = new IntegrityMetadata(
						IntegrityMetadataCodec.VERSION_1,
						IntegrityMetadataCodec.ALGORITHM_SHA256,
						HexFormat.of().formatHex(digest),
						expectedSize);
		return new DigestResult(metadata, elapsed);
	}

	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			workers.clear();
			workers.offer(CLOSED_WORKER);
		}
	}

	public static final class DigestFailureException extends IOException {
		private final long processedBytes;
		private final long workerNanos;

		private DigestFailureException(
						final IOException cause, final long processedBytes, final long workerNanos) {
			super(cause.getMessage(), cause);
			this.processedBytes = Math.max(0, processedBytes);
			this.workerNanos = Math.max(0, workerNanos);
		}

		public long processedBytes() {
			return processedBytes;
		}

		public long workerNanos() {
			return workerNanos;
		}
	}

	private static final class Worker {
		private final ByteBuffer buffer;
		private final MessageDigest digest;

		private Worker(final ByteBuffer buffer, final MessageDigest digest) {
			this.buffer = buffer;
			this.digest = digest;
		}

		private static Worker digestWorker() throws NoSuchAlgorithmException {
			return new Worker(
							ByteBuffer.allocate(BUFFER_SIZE), MessageDigest.getInstance(JCA_ALGORITHM));
		}

		private static Worker closedSentinel() {
			return new Worker(null, null);
		}
	}
}
