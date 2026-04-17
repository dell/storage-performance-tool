package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.Item;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Non-blocking item source for the {@link MixedLoadGenerator}.
 *
 * <p>Manages two item pools:
 * <ol>
 *   <li><b>Read pool</b> — immutable array of seed items populated at construction.
 *       {@link #randomItem()} returns a random item (thread-safe, no mutation).
 *       Used by GET and STAT operations.
 *   <li><b>Delete queue</b> — lock-free {@link ConcurrentLinkedQueue} fed by PUT completions.
 *       {@link #pollDelete()} returns null immediately when empty (non-blocking).
 *       Used by DELETE operations.
 * </ol>
 *
 * @param <I> the item type
 */
public final class PoolItemInput<I extends Item> {

	private final I[] readPool;
	private final ConcurrentLinkedQueue<I> deleteQueue;

	/**
	 * Creates a pool with the given seed items as the read pool and an empty delete queue.
	 *
	 * @param seedItems non-empty list of items for the read pool
	 */
	public PoolItemInput(final List<I> seedItems) {
		this(seedItems, null);
	}

	/**
	 * Creates a pool with seed items for the read pool and optional pre-populated delete queue items.
	 *
	 * @param seedItems    non-empty list of items for the read pool
	 * @param deleteItems  items to pre-populate the delete queue (may be null or empty)
	 */
	@SuppressWarnings("unchecked")
	public PoolItemInput(final List<I> seedItems, final Collection<I> deleteItems) {
		if (seedItems == null || seedItems.isEmpty()) {
			throw new IllegalArgumentException("Seed items list must be non-null and non-empty");
		}
		this.readPool = seedItems.toArray((I[]) new Item[0]);
		this.deleteQueue = new ConcurrentLinkedQueue<>();
		if (deleteItems != null) {
			this.deleteQueue.addAll(deleteItems);
		}
	}

	/** Returns a random item from the read pool. Thread-safe, never returns null. */
	public I randomItem() {
		return readPool[ThreadLocalRandom.current().nextInt(readPool.length)];
	}

	/** Number of items in the read pool. */
	public int readPoolSize() {
		return readPool.length;
	}

	/**
	 * Adds an item to the delete queue. Called when a PUT operation succeeds.
	 * Thread-safe, non-blocking.
	 */
	public void addDeleteItem(final I item) {
		deleteQueue.offer(item);
	}

	/**
	 * Polls the next item from the delete queue. Returns null if the queue is empty.
	 * Thread-safe, non-blocking.
	 */
	public I pollDelete() {
		return deleteQueue.poll();
	}

	/** Current number of items waiting in the delete queue. */
	public int deleteQueueSize() {
		return deleteQueue.size();
	}
}
