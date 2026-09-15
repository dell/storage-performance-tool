package com.dell.spt.base.load.step.local.context.range;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OperationRetryPolicy;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadCirculation;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/** Range-only retry ownership. Uses the step's scheduler; creates no executor or response buffers. */
public final class RangeReadRetryCoordinator<I extends DataItem> implements AutoCloseable {
	@FunctionalInterface
	public interface Scheduler {
		Future<?> schedule(long delayMillis, Runnable task);
	}

	private final class Entry {
		final RangeReadOperation<I> operation;
		final RangeReadCirculation circulation;
		final RangeReadAttempt attempt;
		boolean started;
		Future<?> future;

		Entry(RangeReadOperation<I> operation, RangeReadCirculation circulation, RangeReadAttempt attempt) {
			this.operation = operation;
			this.circulation = circulation;
			this.attempt = attempt;
		}
	}

	private final Object lock = new Object();
	private final IdentityHashMap<RangeReadCirculation, Entry> pending = new IdentityHashMap<>();
	private final AtomicReference<IntegrityTerminalException> failure = new AtomicReference<>();
	private final LoadGenerator<I, RangeReadOperation<I>> generator;
	private final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker;
	private final RangeReadMetrics metrics;
	private final Scheduler scheduler;
	private final BiConsumer<RangeReadOperation<I>, RangeReadCirculation> publish;
	private final boolean enabled;
	private final int retryLimit;
	private final int capacity;
	private boolean open = true;

	/**
	 * Capacity is the step's logical admission bound. Publish must use the circulation's
	 * once-only terminal output claim, outside accounting locks. The step must observe failure()
	 * as an execution/reporting failure and close generator/driver admission during shutdown.
	 */
	public RangeReadRetryCoordinator(boolean enabled, int retryLimit, int capacity,
					LoadGenerator<I, RangeReadOperation<I>> generator,
					OperationLifecycleTracker<? super RangeReadOperation<I>> tracker,
					RangeReadMetrics metrics, Scheduler scheduler,
					BiConsumer<RangeReadOperation<I>, RangeReadCirculation> publish) {
		if (retryLimit < 0 || capacity <= 0)
			throw new IllegalArgumentException("Invalid range retry bounds");
		this.enabled = enabled;
		this.retryLimit = retryLimit;
		this.capacity = capacity;
		this.generator = Objects.requireNonNull(generator);
		this.tracker = Objects.requireNonNull(tracker);
		this.metrics = Objects.requireNonNull(metrics);
		this.scheduler = Objects.requireNonNull(scheduler);
		this.publish = Objects.requireNonNull(publish);
		if (!tracker.isEnabled())
			throw new IllegalArgumentException("Range retries require lifecycle tracking");
	}

	/** Sole attempt accounting/retry-decision entry point; stale or duplicate callbacks return false. */
	public boolean completed(RangeReadOperation<I> operation, RangeReadCirculation circulation, RangeReadAttempt attempt) {
		final RangeReadAttempt retry;
		final int retryNumber;
		synchronized (circulation.lifecycle()) {
			if (operation.circulation() != circulation
							|| !tracker.hasOutstandingCustody(operation, circulation.lifecycle())
							|| !metrics.recordAttempt(circulation, attempt))
				return false;
			retry = enabled && circulation.retryCount() < retryLimit ? circulation.beginAttempt(attempt) : null;
			retryNumber = circulation.retryCount();
		}
		if (retry == null) {
			try {
				circulation.complete(operation, tracker, attempt);
			} catch (RuntimeException accountingFailure) {
				recordFailure(accountingFailure);
			} finally {
				publishTerminal(operation, circulation);
			}
			return true;
		}
		final var entry = new Entry(operation, circulation, retry);
		boolean admitted = false;
		synchronized (lock) {
			if (open) {
				final var previous = pending.get(circulation);
				if ((previous == null && pending.size() < capacity) || (previous != null && previous.started)) {
					pending.put(circulation, entry);
					admitted = true;
				} else {
					recordFailure(new IllegalStateException("Range retry ownership exceeded its admission bound"));
				}
			}
		}
		if (!admitted) {
			settleWaiting(entry);
			return true;
		}
		try {
			final var future = Objects.requireNonNull(
							scheduler.schedule(OperationRetryPolicy.backoffMillis(retryNumber), () -> handoff(entry)));
			final boolean cancel;
			synchronized (lock) {
				entry.future = future;
				cancel = !open || pending.get(circulation) != entry;
			}
			if (cancel)
				future.cancel(false);
		} catch (RuntimeException schedulingFailure) {
			remove(entry);
			settleWaiting(entry);
			recordFailure(schedulingFailure);
		}
		return true;
	}

	private void handoff(Entry entry) {
		synchronized (lock) {
			if (!open || pending.get(entry.circulation) != entry || entry.started)
				return;
			entry.started = true;
		}
		try {
			if (!entry.circulation.isPendingAttempt(entry.attempt))
				return;
			if (!generator.retryRange(entry.operation, entry.attempt))
				settleWaiting(entry);
		} catch (RuntimeException handoffFailure) {
			settleWaiting(entry);
			recordFailure(handoffFailure);
		} finally {
			remove(entry); // A synchronous successor must survive this old handoff's return.
		}
	}

	private void remove(Entry entry) {
		synchronized (lock) {
			pending.remove(entry.circulation, entry);
		}
	}

	private void settleWaiting(Entry entry) {
		try {
			synchronized (entry.circulation.lifecycle()) {
				synchronized (entry.attempt) {
					if (entry.operation.circulation() == entry.circulation
									&& entry.circulation.isPendingAttempt(entry.attempt)) {
						entry.circulation.settleAtDeadline(entry.operation, tracker);
					}
				}
			}
		} catch (RuntimeException accountingFailure) {
			recordFailure(accountingFailure);
		} finally {
			publishTerminal(entry.operation, entry.circulation);
		}
	}

	private void publishTerminal(RangeReadOperation<I> operation, RangeReadCirculation circulation) {
		if (operation.circulation() == circulation
						&& circulation.lifecycle().state() == OperationLifecycleState.TERMINAL) {
			// A synchronous terminal callback releases its logical slot before optional output,
			// even when the generator handoff which invoked it has not returned yet.
			synchronized (lock) {
				pending.remove(circulation);
			}
			try {
				publish.accept(operation, circulation);
			} catch (RuntimeException outputFailure) {
				recordFailure(outputFailure);
			}
		}
	}

	private void recordFailure(RuntimeException cause) {
		failure.compareAndSet(null, new IntegrityTerminalException(IntegrityTerminalException.Category.EXECUTION,
						"Range retry or result publication failed", cause));
	}

	public IntegrityTerminalException failure() {
		return failure.get();
	}

	/** Includes unsettled generator handoffs, until they return or publish a terminal outcome. */
	public int pendingCount() {
		synchronized (lock) {
			return pending.size();
		}
	}

	/** Cancels queued retries, retaining known failures; active transport remains in the driver drain. */
	@Override
	public void close() {
		final List<Entry> entries;
		synchronized (lock) {
			open = false;
			entries = List.copyOf(pending.values());
			pending.clear();
		}
		for (final var entry : entries) {
			final Future<?> future;
			synchronized (lock) {
				future = entry.future;
			}
			try {
				if (future != null)
					future.cancel(false);
			} catch (RuntimeException cancellationFailure) {
				recordFailure(cancellationFailure);
			} finally {
				settleWaiting(entry);
			}
		}
	}
}
