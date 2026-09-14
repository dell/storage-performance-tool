package com.dell.spt.storage.driver.coop.range;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Composable cooperative adapter custody for queued and polled range attempts. Entries require
 * a runtime admission reservation, so the map is bounded by logical outstanding/result capacity.
 * Adapters delegate their protected queue/dispatch/recovery hooks here and override
 * successfulSubmitStartsTransport to false. Transport callbacks retain captured tokens.
 */
public final class RangeReadQueue<I extends DataItem> {
	private record Entry<I extends DataItem>(
	RangeReadOperation<I> operation, RangeReadAttempt attempt)
	{}
	private final RangeReadRuntime<I> runtime;
	private final ConcurrentMap<RangeReadCirculation, Entry<I>> pending = new ConcurrentHashMap<>();

	public RangeReadQueue(RangeReadRuntime<I> runtime) {
		this.runtime = Objects.requireNonNull(runtime);
	}

	/** Called under the cooperative admission/queue locks; never retains the queue reference. */
	public boolean offer(RangeReadOperation<I> op, BlockingQueue<RangeReadOperation<I>> queue) {
		final var circulation = op.circulation();
		if (!runtime.admission().isAdmitted(circulation))
			return false;
		synchronized (circulation.lifecycle()) {
			if (!runtime.tracker().hasOutstandingCustody(op, circulation.lifecycle())
							|| pending.containsKey(circulation))
				return false;
			var attempt = circulation.pendingAttempt();
			final var state = circulation.lifecycle().state();
			if (attempt == null && state != OperationLifecycleState.GENERATOR_BUFFERED
							&& state != OperationLifecycleState.DRIVER_QUEUED)
				return false;
			if (!queue.offer(op))
				return false;
			if (attempt == null) {
				// Lock-order invariant: this is pre-publication initial queue ownership.
				// driverQueued may take the operation monitor under the lifecycle monitor;
				// result claims take those monitors in reverse order, but only after terminal
				// settlement, and publication/recycling uses detached result copies.
				// Never add result publication here or reuse a publishing instance for admission.
				if (!runtime.tracker().driverQueued(op)) {
					queue.removeIf(queued -> queued == op);
					return false;
				}
				attempt = circulation.beginAttempt(null);
				if (attempt == null) {
					queue.removeIf(queued -> queued == op);
					return false;
				}
			}
			pending.put(circulation, new Entry<>(op, attempt));
			return true;
		}
	}

	/** Capture once when submitting; never look up a newer token from an old transport callback. */
	public RangeReadAttempt capture(RangeReadOperation<I> op) {
		final var circulation = op.circulation();
		final var entry = pending.get(circulation);
		return entry != null && entry.operation() == op && circulation.isPendingAttempt(entry.attempt())
						? entry.attempt()
						: null;
	}

	/**
	 * Called immediately before execute/write in the driver's withDispatchAdmission action.
	 * The same action performs transport submission; completion/output must occur outside that
	 * admission action. Initial logical dispatch is recorded once; retries retain it.
	 */
	public boolean requestHandoff(RangeReadOperation<I> op, RangeReadAttempt token) {
		final var circulation = op.circulation();
		synchronized (circulation.lifecycle()) {
			final var entry = pending.get(circulation);
			if (entry == null || entry.operation() != op || entry.attempt() != token)
				return false;
			synchronized (token) {
				if (!circulation.isPendingAttempt(token)
								|| !runtime.tracker().hasOutstandingCustody(op, circulation.lifecycle()))
					return false;
				if (circulation.lifecycle().state() == OperationLifecycleState.DRIVER_QUEUED
								&& !runtime.tracker().explicitlyDispatched(op))
					return false;
				if (!runtime.metrics().requestHandoff(token))
					return false;
				pending.remove(circulation, entry);
				return true;
			}
		}
	}

	/** Remove only this completed attempt before the coordinator may synchronously queue its successor. */
	public boolean completed(RangeReadOperation<I> op, RangeReadCirculation circulation, RangeReadAttempt token) {
		if (token == null || token.outcome() == null)
			return false;
		final var entry = pending.get(circulation);
		if (entry != null && entry.operation() == op && entry.attempt() == token)
			pending.remove(circulation, entry);
		return runtime.completed(op, circulation, token);
	}

	/** Includes entries the dispatcher removed into its local buffer or an asynchronous submission. */
	public List<RangeReadOperation<I>> recoveryCandidates() {
		return pending.values().stream().map(Entry::operation).toList();
	}

	/** Return true only for newly unattempted work; known failed retries stay failed. */
	public boolean recover(RangeReadOperation<I> op) {
		final var circulation = op.circulation();
		final var entry = pending.get(circulation);
		if (entry == null || entry.operation() != op)
			return false;
		boolean settled = false;
		try {
			synchronized (circulation.lifecycle()) {
				synchronized (entry.attempt()) {
					if (op.circulation() == circulation && !entry.attempt().wasHandedOff()) {
						if (entry.attempt().outcome() != null) {
							settled = circulation.complete(op, runtime.tracker(), entry.attempt());
						} else if (circulation.isPendingAttempt(entry.attempt())) {
							settled = circulation.settleAtDeadline(op, runtime.tracker());
						}
					}
				}
			}
			return settled && circulation.lifecycle().state() == OperationLifecycleState.UNATTEMPTED;
		} finally {
			pending.remove(circulation, entry);
			if (settled && circulation.lifecycle().state() == OperationLifecycleState.UNATTEMPTED)
				runtime.admission().releaseSettled(circulation);
		}
	}

	public int pendingCount() {
		return pending.size();
	}
}
