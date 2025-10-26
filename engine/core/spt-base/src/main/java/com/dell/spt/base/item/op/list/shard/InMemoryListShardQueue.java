package com.dell.spt.base.item.op.list.shard;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class InMemoryListShardQueue implements ListShardQueue {

	private static final long DEFAULT_LEASE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
	private static final long MIN_WATCHDOG_INTERVAL_MILLIS = 500L;

	private final BlockingQueue<ListShard> queue = new LinkedBlockingQueue<>();
	private final ListShardMetricsRecorder metricsRecorder;
	private final ConcurrentMap<InMemoryLease, LeaseState> activeLeases = new ConcurrentHashMap<>();
	private final long leaseTimeoutMillis;
	private final long watchdogIntervalMillis;
	private final ScheduledExecutorService watchdog;
	private final ScheduledFuture<?> rescueTask;

	InMemoryListShardQueue(final ListShardMetricsRecorder metricsRecorder) {
		this(metricsRecorder, computeDefaultTimeout(metricsRecorder));
	}

	InMemoryListShardQueue(final ListShardMetricsRecorder metricsRecorder, final long leaseTimeoutMillis) {
		this.metricsRecorder = Objects.requireNonNull(metricsRecorder, "metricsRecorder");
		long timeout = leaseTimeoutMillis <= 0L ? computeDefaultTimeout(metricsRecorder) : leaseTimeoutMillis;
		if (timeout <= 0L) {
			timeout = DEFAULT_LEASE_TIMEOUT_MILLIS;
		}
		this.leaseTimeoutMillis = timeout;
		this.watchdogIntervalMillis = Math.max(MIN_WATCHDOG_INTERVAL_MILLIS, this.leaseTimeoutMillis / 3);
		this.watchdog = Executors.newSingleThreadScheduledExecutor(
						r -> {
							final Thread t = new Thread(r, "spt-list-shard-watchdog");
							t.setDaemon(true);
							return t;
						});
		this.rescueTask = this.watchdog.scheduleAtFixedRate(
						this::rescueStaleLeases,
						this.watchdogIntervalMillis,
						this.watchdogIntervalMillis,
						TimeUnit.MILLISECONDS);
	}

	@Override
	public Lease acquire() throws InterruptedException {
		return newLease(queue.take());
	}

	@Override
	public Lease acquire(final long timeout, final TimeUnit unit) throws InterruptedException {
		final var shard = queue.poll(timeout, unit);
		return shard == null ? null : newLease(shard);
	}

	@Override
	public boolean offer(final ListShard shard) {
		Objects.requireNonNull(shard, "shard");
		final boolean offered = queue.offer(shard);
		if (offered) {
			metricsRecorder.onRequeue(shard);
		}
		return offered;
	}

	@Override
	public int pendingCount() {
		return queue.size();
	}

	@Override
	public ListShardMetricsRecorder metricsRecorder() {
		return metricsRecorder;
	}

	private Lease newLease(final ListShard shard) {
		metricsRecorder.onLease(shard);
		return new InMemoryLease(shard);
	}

	private final class InMemoryLease implements Lease {

		private ListShard shard;
		private final AtomicBoolean terminal = new AtomicBoolean();
		private final LeaseState state;

		private InMemoryLease(final ListShard shard) {
			this.shard = shard;
			this.state = new LeaseState(System.currentTimeMillis());
			activeLeases.put(this, state);
		}

		@Override
		public ListShard shard() {
			return shard;
		}

		@Override
		public void update(final ListShard updated) {
			Objects.requireNonNull(updated, "updated");
			if (terminal.get()) {
				throw new IllegalStateException("Shard lease is already closed");
			}
			this.shard = updated;
			heartbeat();
		}

		@Override
		public void requeue() {
			if (terminal.compareAndSet(false, true)) {
				final ListShard requeued = shard;
				shard = null;
				activeLeases.remove(this);
				if (requeued != null) {
					queue.offer(requeued);
				}
			}
		}

		@Override
		public void complete() {
			if (terminal.compareAndSet(false, true)) {
				final ListShard completed = shard;
				shard = null;
				activeLeases.remove(this);
				if (completed != null) {
					metricsRecorder.onComplete(completed);
				}
			}
		}

		@Override
		public boolean split(final List<ListShard> children) {
			Objects.requireNonNull(children, "children");
			if (!terminal.compareAndSet(false, true)) {
				return false;
			}
			final ListShard parent = shard;
			shard = null;
			activeLeases.remove(this);
			if (parent != null) {
				metricsRecorder.onComplete(parent);
			}
			if (!children.isEmpty()) {
				for (final ListShard child : children) {
					InMemoryListShardQueue.this.offer(child);
				}
			}
			return true;
		}

		private void heartbeat() {
			state.heartbeat(System.currentTimeMillis());
		}

		private boolean isTerminal() {
			return terminal.get();
		}

		private void rescue() {
			if (!terminal.compareAndSet(false, true)) {
				return;
			}
			final ListShard rescued = shard;
			shard = null;
			if (rescued != null) {
				queue.offer(rescued);
				metricsRecorder.onRescue(rescued);
			}
		}
	}

	private static long computeDefaultTimeout(final ListShardMetricsRecorder metricsRecorder) {
		if (metricsRecorder == null) {
			return DEFAULT_LEASE_TIMEOUT_MILLIS;
		}
		final long fromRecorder = metricsRecorder.stallThresholdMillis();
		return fromRecorder > 0L ? fromRecorder : DEFAULT_LEASE_TIMEOUT_MILLIS;
	}

	private void rescueStaleLeases() {
		if (rescueTask.isCancelled()) {
			return;
		}
		if (activeLeases.isEmpty()) {
			return;
		}
		final long now = System.currentTimeMillis();
		for (var entry : activeLeases.entrySet()) {
			final InMemoryLease lease = entry.getKey();
			final LeaseState state = entry.getValue();
			if (lease == null || state == null) {
				continue;
			}
			if (lease.isTerminal()) {
				activeLeases.remove(lease, state);
				continue;
			}
			if (now - state.lastHeartbeatMillis > leaseTimeoutMillis && activeLeases.remove(lease, state)) {
				lease.rescue();
			}
		}
	}

	private static final class LeaseState {
		private volatile long lastHeartbeatMillis;

		private LeaseState(final long lastHeartbeatMillis) {
			this.lastHeartbeatMillis = lastHeartbeatMillis;
		}

		private void heartbeat(final long nowMillis) {
			this.lastHeartbeatMillis = nowMillis;
		}
	}
}
