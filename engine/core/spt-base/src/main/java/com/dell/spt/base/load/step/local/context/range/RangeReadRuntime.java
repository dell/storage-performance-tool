package com.dell.spt.base.load.step.local.context.range;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.range.RangeReadAdmission;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.dell.spt.base.metrics.range.RangeReadSnapshot;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** One step's opt-in range accounting, admission, retry and result-delivery wiring. */
public final class RangeReadRuntime<I extends DataItem> implements Output<RangeReadOperation<I>> {
	private final RangeReadPolicy policy;
	private final RangeReadMetrics metrics;
	private final OperationLifecycleTracker<RangeReadOperation<I>> tracker;
	private final RangeReadAdmission<I> admission;
	private final int capacity;
	private final BiConsumer<RangeReadOperation<I>, RangeReadCirculation> publish;
	private final AtomicReference<IntegrityTerminalException> reportingFailure = new AtomicReference<>();
	private final Object resultLock = new Object();
	private final Set<RangeReadCirculation> pendingResults = Collections.newSetFromMap(new IdentityHashMap<>());
	private Consumer<RangeReadOperation<I>> terminalAccounting;
	private Consumer<RangeReadOperation<I>> resultOutput;
	private RangeReadRetryCoordinator<I> retries;
	private boolean deliveryOpen = true;

	/** Adapter installs tracker() before start; generator sends initial/retry operations to admission(). */
	public RangeReadRuntime(RangeReadPolicy policy, int capacity, Output<RangeReadOperation<I>> driver,
					BiConsumer<RangeReadOperation<I>, RangeReadCirculation> publish) {
		this.policy = Objects.requireNonNull(policy);
		this.capacity = capacity;
		this.publish = Objects.requireNonNull(publish);
		metrics = new RangeReadMetrics(policy);
		tracker = OperationLifecycleTracker.withDeadlineSettlement(this::settleAtDeadline);
		admission = new RangeReadAdmission<>(driver, tracker, capacity);
		tracker.terminalObserver(this::recordTerminal);
	}

	public OperationLifecycleTracker<RangeReadOperation<I>> tracker() {
		return tracker;
	}

	public RangeReadAdmission<I> admission() {
		return admission;
	}

	public RangeReadMetrics metrics() {
		return metrics;
	}

	public RangeReadPolicy policy() {
		return policy;
	}

	public RangeReadSnapshot snapshot() {
		return metrics.snapshot(tracker.counters());
	}

	/**
	 * Bridge selected once by the generic step constructor, after matching the exact driver tracker.
	 * Output receives each determinate result once, including failures for optional tracing.
	 * The consumer must gate successful item/timing output and recycling on SUCC.
	 */
	@SuppressWarnings("unchecked")
	public <T extends Item, O extends Operation<T>> Output<O> bind(LoadGenerator<T, O> generator,
					OperationLifecycleTracker<O> owner, boolean retryEnabled, int retryLimit,
					RangeReadRetryCoordinator.Scheduler scheduler, Consumer<O> accounting, Consumer<O> output) {
		if ((Object) owner != tracker || !tracker.isEnabled() || tracker.counters().selected() != 0
						|| retries != null)
			throw new IllegalStateException("Range runtime must bind once before operation admission");
		if (retryEnabled && !generator.supportsRangeRetry())
			throw new IllegalStateException("Generator does not support retained range retries");
		Objects.requireNonNull(accounting);
		Objects.requireNonNull(output);
		terminalAccounting = op -> accounting.accept((O) op);
		resultOutput = op -> output.accept((O) op);
		retries = new RangeReadRetryCoordinator<>(retryEnabled, Math.max(0, retryLimit), capacity,
						(LoadGenerator<I, RangeReadOperation<I>>) (LoadGenerator<?, ?>) generator,
						tracker, metrics, scheduler, publish);
		return (Output<O>) (Output<?>) this;
	}

	private void recordTerminal(RangeReadOperation<I> op) {
		final var circulation = op.circulation();
		if (!metrics.recordFinal(circulation))
			return;
		try {
			if (op.status() == Operation.Status.SUCC) {
				synchronized (resultLock) {
					if (!deliveryOpen)
						throw new IllegalStateException("Range success arrived after result drain closed");
					pendingResults.add(circulation);
				}
			}
			Objects.requireNonNull(terminalAccounting, "Range runtime is not bound").accept(op);
		} finally {
			// Successful output retains the existing admission slot, bounding pending results.
			if (op.status() != Operation.Status.SUCC)
				admission.releaseSettled(circulation);
		}
	}

	private boolean settleAtDeadline(RangeReadOperation<I> op,
					OperationLifecycleTracker<RangeReadOperation<I>> owner) {
		final var circulation = op.circulation();
		final boolean settled = circulation.settleAtDeadline(op, owner);
		if (circulation.lifecycle().state() == OperationLifecycleState.UNRESOLVED)
			metrics.recordFinal(circulation);
		if (circulation.lifecycle().state() != OperationLifecycleState.TERMINAL)
			admission.releaseSettled(circulation);
		return settled;
	}

	public boolean completed(RangeReadOperation<I> op, RangeReadCirculation circulation, RangeReadAttempt attempt) {
		return Objects.requireNonNull(retries, "Range runtime is not bound").completed(op, circulation, attempt);
	}

	@Override
	public boolean put(RangeReadOperation<I> result) {
		final var circulation = result.circulation();
		// Never acquire a lifecycle monitor under resultLock: terminal accounting takes
		// these locks in the opposite order when registering pending success delivery.
		final var snapshot = result.claimStepResult(circulation);
		if (snapshot == null)
			return false;
		synchronized (resultLock) {
			if (!deliveryOpen)
				return false;
		}
		try {
			resultOutput.accept(snapshot);
			return true;
		} catch (Exception failure) {
			recordFailure(failure);
			throw reportingFailure.get();
		} finally {
			admission.releaseSettled(circulation);
			synchronized (resultLock) {
				pendingResults.remove(circulation);
			}
		}
	}

	@Override
	public int put(List<RangeReadOperation<I>> results, int from, int to) {
		Objects.checkFromToIndex(from, to, results.size());
		int accepted = 0;
		for (int i = from; i < to && put(results.get(i)); i++)
			accepted++;
		return accepted;
	}

	@Override
	public int put(List<RangeReadOperation<I>> results) {
		return put(results, 0, results.size());
	}

	@Override
	public Input<RangeReadOperation<I>> getInput() {
		return null;
	}

	public boolean hasPendingResults() {
		synchronized (resultLock) {
			return !pendingResults.isEmpty();
		}
	}

	public void closeAdmission() {
		admission.closeAdmission();
	}

	public void closeRetries() {
		if (retries != null)
			retries.close();
	}

	/** Called at the existing step drain boundary, never an additional unbounded output wait. */
	public void finishResults() {
		final List<RangeReadCirculation> abandoned;
		synchronized (resultLock) {
			deliveryOpen = false;
			abandoned = List.copyOf(pendingResults);
			pendingResults.clear();
		}
		if (!abandoned.isEmpty())
			recordFailure(new IllegalStateException("Range result delivery exceeded the step drain bound"));
		for (var circulation : abandoned)
			admission.releaseSettled(circulation);
	}

	public void releaseRecovered(Operation<?> op) {
		if (op instanceof RangeReadOperation<?> range)
			admission.releaseSettled(range.circulation());
	}

	private void recordFailure(Exception cause) {
		reportingFailure.compareAndSet(null, new IntegrityTerminalException(
						IntegrityTerminalException.Category.PUBLICATION, "Range result delivery failed", cause));
	}

	public IntegrityTerminalException failure() {
		final var own = reportingFailure.get();
		return own != null ? own : retries == null ? null : retries.failure();
	}

	@Override
	public void close() {
		closeAdmission();
		closeRetries();
		finishResults();
	}
}
