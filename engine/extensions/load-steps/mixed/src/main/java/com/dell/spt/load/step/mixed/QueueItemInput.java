package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.Item;
import com.github.akurilov.commons.io.Input;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

/**
 * An {@link Input} backed by a {@link LinkedBlockingQueue}. Used to feed the DELETE generator with
 * items produced by the PUT generator during a mixed workload.
 *
 * <p>Blocking semantics: {@link #get(List, int)} blocks (in 100ms polling intervals) until at
 * least one item is available, then non-blockingly drains the rest of the batch. The generator
 * virtual thread is safely parked by the blocking-queue poll; it will be interrupted when the load
 * step's time limit expires, at which point the interrupt is re-thrown as unchecked so the engine
 * can clean up.
 */
public final class QueueItemInput<I extends Item> implements Input<I> {

	private static final long POLL_TIMEOUT_MS = 100;

	private final LinkedBlockingQueue<I> queue;

	public QueueItemInput(final LinkedBlockingQueue<I> queue) {
		this.queue = queue;
	}

	/** Single-item get; blocks until an item is available or the thread is interrupted. */
	@Override
	public I get() {
		try {
			I item = null;
			while (item == null) {
				item = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			}
			return item;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throwUnchecked(e);
			return null; // unreachable
		}
	}

	/**
	 * Batch get; blocks for the first item, then drains non-blocking for the rest. Returning an
	 * empty list would cause the generator to set {@code itemInputFinishFlag}, so we block until at
	 * least one item is ready.
	 */
	@Override
	public int get(final List<I> buffer, final int limit) {
		try {
			I first = null;
			while (first == null) {
				first = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			}
			buffer.add(first);
			int count = 1;
			while (count < limit) {
				final I next = queue.poll();
				if (next == null) {
					break;
				}
				buffer.add(next);
				count++;
			}
			return count;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throwUnchecked(e);
			return 0; // unreachable
		}
	}

	@Override
	public long skip(final long itemsCount) {
		long skipped = 0;
		while (skipped < itemsCount && queue.poll() != null) {
			skipped++;
		}
		return skipped;
	}

	@Override
	public void reset() {
		// No-op: queue is live; reset not supported.
	}

	@Override
	public void close() {
		// No-op: queue lifecycle is managed by MixedLoadStepLocal.
	}

	@Override
	public String toString() {
		return "QueueItemInput(size=" + queue.size() + ")";
	}
}
