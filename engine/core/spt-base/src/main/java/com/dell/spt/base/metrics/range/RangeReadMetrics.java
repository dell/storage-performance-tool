package com.dell.spt.base.metrics.range;

import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadCirculation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import java.util.Objects;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Fixed-size, range-only aggregate counters. No per-operation ledger or response buffers. */
public final class RangeReadMetrics {
	enum Counter {
		REQUESTS, SUCCESS_BYTES, LOCAL, HTTP, VALIDATION, TRANSPORT, HTTP_ATTEMPTS, VALIDATION_ATTEMPTS, TRANSPORT_ATTEMPTS, FAILED_BYTES, UNRESOLVED_BYTES
	}

	private final RangeReadPolicy policy;
	private final EnumMap<Counter, AtomicLong> totals = new EnumMap<>(Counter.class);
	private final AtomicBoolean overflow = new AtomicBoolean();

	public RangeReadMetrics(final RangeReadPolicy policy) {
		this.policy = Objects.requireNonNull(policy);
		for (final var counter : Counter.values()) {
			totals.put(counter, new AtomicLong());
		}
	}

	/** Invoke at the real execute/write handoff; a canceled or duplicate token never increments. */
	public boolean requestHandoff(final RangeReadAttempt attempt) {
		if (!attempt.requestHandoff()) {
			return false;
		}
		add(Counter.REQUESTS, 1);
		return true;
	}

	/** Record before scheduling another attempt; this consumes the circulation's outcome claim. */
	public boolean recordAttempt(final RangeReadCirculation circulation, final RangeReadAttempt attempt) {
		final var outcome = circulation.claimOutcome(attempt);
		if (outcome == null) {
			return false;
		}
		if (outcome.receivedBytesOverflow()) {
			overflow.set(true);
		}
		switch (outcome.category()) {
		case HTTP -> {
			add(Counter.HTTP_ATTEMPTS, 1);
			add(Counter.FAILED_BYTES, outcome.receivedBytes());
		}
		case VALIDATION -> {
			add(Counter.VALIDATION_ATTEMPTS, 1);
			add(Counter.FAILED_BYTES, outcome.receivedBytes());
		}
		case TRANSPORT -> {
			add(Counter.TRANSPORT_ATTEMPTS, 1);
			add(Counter.FAILED_BYTES, outcome.receivedBytes());
		}
		case UNRESOLVED -> add(Counter.UNRESOLVED_BYTES, outcome.receivedBytes());
		case SUCCESS -> { /* Successful bytes are published only with final logical accounting. */ }
		}
		return true;
	}

	/** Bounded counter observer for terminal commit, or explicit range deadline settlement. */
	public boolean recordFinal(final RangeReadCirculation circulation) {
		final var result = circulation.claimFinalMetrics();
		if (result == null) {
			return false;
		}
		if (result.localError() != null) {
			add(Counter.LOCAL, 1);
			return true;
		}
		if (result.attempt() != null) {
			recordAttempt(circulation, result.attempt());
		}
		switch (result.outcome().category()) {
		case SUCCESS -> add(Counter.SUCCESS_BYTES, result.outcome().receivedBytes());
		case HTTP -> add(Counter.HTTP, 1);
		case VALIDATION -> add(Counter.VALIDATION, 1);
		case TRANSPORT -> add(Counter.TRANSPORT, 1);
		case UNRESOLVED -> { /* Not a failed logical result. */ }
		}
		return true;
	}

	public RangeReadSnapshot snapshot(final OperationLifecycleCounters logical) {
		long attempted;
		final boolean logicalOverflow = logical.selected() < 0 || logical.accepted() < 0 || logical.failed() < 0
						|| logical.terminalResults() < 0 || logical.unattempted() < 0 || logical.unresolved() < 0;
		boolean snapshotOverflow = overflow.get() || logicalOverflow;
		try {
			attempted = logicalOverflow ? Long.MAX_VALUE
							: Math.addExact(Math.addExact(logical.accepted(), logical.failed()), logical.unresolved());
		} catch (ArithmeticException exceeded) {
			attempted = Long.MAX_VALUE;
			snapshotOverflow = true;
		}
		return new RangeReadSnapshot(1, policy, logical, attempted,
						value(Counter.REQUESTS), value(Counter.SUCCESS_BYTES), value(Counter.LOCAL), value(Counter.HTTP),
						value(Counter.VALIDATION), value(Counter.TRANSPORT), value(Counter.HTTP_ATTEMPTS),
						value(Counter.VALIDATION_ATTEMPTS), value(Counter.TRANSPORT_ATTEMPTS),
						value(Counter.FAILED_BYTES), value(Counter.UNRESOLVED_BYTES), snapshotOverflow || overflow.get());
	}

	private long value(final Counter counter) {
		return totals.get(counter).get();
	}

	void add(final Counter counter, final long delta) {
		if (delta < 0) {
			throw new IllegalArgumentException("Negative range counter increment");
		}
		final var total = totals.get(counter);
		while (true) {
			final long previous = total.get();
			final boolean exceeded = delta > Long.MAX_VALUE - previous;
			final long next = exceeded ? Long.MAX_VALUE : previous + delta;
			if (exceeded) {
				overflow.set(true);
			}
			if (total.compareAndSet(previous, next)) {
				return;
			}
		}
	}
}
