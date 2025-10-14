package com.dell.spt.base.item.op.list.shard;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Abstraction over the shard work queue shared by LIST workers. The queue hands out exclusive leases
 * for shards, tracks completion, and allows callers to requeue slices with updated cursors.
 */
public interface ListShardQueue {

	/**
	 * Acquire the next available shard, blocking indefinitely until one becomes available.
	 */
	Lease acquire() throws InterruptedException;

	/**
	 * Acquire a shard or return {@code null} if none becomes available within the timeout.
	 */
	Lease acquire(long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Enqueue a new shard. Returns {@code true} if the shard was accepted.
	 */
	boolean offer(ListShard shard);

	/**
	 * @return number of shards waiting to be leased.
	 */
	int pendingCount();

	/**
	 * @return currently configured metrics recorder.
	 */
	ListShardMetricsRecorder metricsRecorder();

	/**
	 * Lease representing exclusive ownership of a shard. Callers must either {@link #complete()} when
	 * finished or {@link #requeue()} to hand the shard back for further processing.
	 */
	interface Lease extends AutoCloseable {

		ListShard shard();

		/** Replace the current shard state (for example after consuming a page). */
		void update(ListShard updated);

		/** Requeue the shard for additional work (typically when truncated=true). */
		void requeue();

		/** Mark the shard complete and release the lease. */
		void complete();

		/**
		 * Atomically replace the leased shard with the provided child shards. Implementations must ensure the
		 * parent shard is not requeued after this call succeeds.
		 *
		 * @param children child shards to enqueue; may be empty to signal parent completion without new work
		 * @return {@code true} if the replacement succeeded, {@code false} if the lease was already terminal
		 */
		boolean split(List<ListShard> children);

		@Override
		default void close() {
			complete();
		}
	}

	static ListShardQueue inMemory() {
		return new InMemoryListShardQueue(ListShardMetricsRecorder.NO_OP);
	}

	static ListShardQueue inMemory(final ListShardMetricsRecorder metricsRecorder) {
		return new InMemoryListShardQueue(metricsRecorder == null ? ListShardMetricsRecorder.NO_OP : metricsRecorder);
	}

	static ListShardQueue inMemory(
					final ListShardMetricsRecorder metricsRecorder, final long leaseTimeoutMillis) {
		return new InMemoryListShardQueue(
						metricsRecorder == null ? ListShardMetricsRecorder.NO_OP : metricsRecorder,
						leaseTimeoutMillis);
	}
}
