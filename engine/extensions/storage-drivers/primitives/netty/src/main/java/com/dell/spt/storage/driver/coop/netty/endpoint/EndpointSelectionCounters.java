package com.dell.spt.storage.driver.coop.netty.endpoint;

import java.net.InetSocketAddress;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-driver endpoint-selection counters. Once the driver has stopped, each counted selection has
 * exactly one connection outcome: new, reused, failed, or cancelled because the driver stopped
 * while it was connecting. Closes count established connections only. Selection counts are kept for at most
 * {@link EndpointSelectionConstants#MAX_TRACKED_DESTINATIONS} distinct destinations; later ones are
 * counted under {@link #OTHER}, so a changing DNS population cannot grow memory without bound.
 */
public final class EndpointSelectionCounters {

	public static final String OTHER = "other";

	private final ConcurrentMap<String, LongAdder> selections = new ConcurrentHashMap<>();
	private final LongAdder otherSelections = new LongAdder();
	private final LongAdder lookups = new LongAdder();
	private final Map<DnsLookupException.Kind, LongAdder> lookupFailures = new EnumMap<>(DnsLookupException.Kind.class);
	private final LongAdder otherLookupFailures = new LongAdder();
	private final LongAdder lookupNanos = new LongAdder();
	private final AtomicLong minLookupNanos = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong maxLookupNanos = new AtomicLong();
	private final LongAdder connectsNew = new LongAdder();
	private final LongAdder connectsReused = new LongAdder();
	private final LongAdder connectsFailed = new LongAdder();
	private final LongAdder connectsCancelled = new LongAdder();
	private final LongAdder closes = new LongAdder();

	/** Immutable view for reporting and tests. Durations are nanoseconds. */
	public record Snapshot(
					Map<String, Long> selections,
					long lookups,
					Map<String, Long> lookupFailures,
					long minLookupNanos,
					long meanLookupNanos,
					long maxLookupNanos,
					long connectsNew,
					long connectsReused,
					long connectsFailed,
					long connectsCancelled,
					long closes) {}

	public EndpointSelectionCounters() {
		for (final var kind : DnsLookupException.Kind.values()) {
			lookupFailures.put(kind, new LongAdder());
		}
	}

	public void selected(final InetSocketAddress destination) {
		final var key = destination.getAddress().getHostAddress() + ":" + destination.getPort();
		var counter = selections.get(key);
		if (counter == null) {
			if (selections.size() >= EndpointSelectionConstants.MAX_TRACKED_DESTINATIONS) {
				otherSelections.increment();
				return;
			}
			counter = selections.computeIfAbsent(key, ignored -> new LongAdder());
		}
		counter.increment();
	}

	public void lookupCompleted(final long nanos, final Throwable failure) {
		lookups.increment();
		lookupNanos.add(nanos);
		minLookupNanos.accumulateAndGet(nanos, Math::min);
		maxLookupNanos.accumulateAndGet(nanos, Math::max);
		if (failure instanceof DnsLookupException lookupFailure) {
			lookupFailures.get(lookupFailure.kind()).increment();
		} else if (failure != null) {
			otherLookupFailures.increment();
		}
	}

	public void connected(final boolean reused) {
		(reused ? connectsReused : connectsNew).increment();
	}

	public void connectFailed() {
		connectsFailed.increment();
	}

	/** A connect for a counted selection that ended, or never started, because the driver stopped. */
	public void connectCancelled() {
		connectsCancelled.increment();
	}

	public void closed() {
		closes.increment();
	}

	public Snapshot snapshot() {
		final Map<String, Long> selectionCounts = new TreeMap<>();
		selections.forEach((key, counter) -> selectionCounts.put(key, counter.sum()));
		final var other = otherSelections.sum();
		if (other > 0) {
			selectionCounts.put(OTHER, other);
		}
		final Map<String, Long> failures = new TreeMap<>();
		lookupFailures.forEach((kind, counter) -> {
			final var count = counter.sum();
			if (count > 0) {
				failures.put(kind.name(), count);
			}
		});
		if (otherLookupFailures.sum() > 0) {
			failures.put(OTHER, otherLookupFailures.sum());
		}
		final var lookupCount = lookups.sum();
		return new Snapshot(
						Map.copyOf(selectionCounts),
						lookupCount,
						Map.copyOf(failures),
						lookupCount == 0 ? 0 : minLookupNanos.get(),
						lookupCount == 0 ? 0 : lookupNanos.sum() / lookupCount,
						maxLookupNanos.get(),
						connectsNew.sum(),
						connectsReused.sum(),
						connectsFailed.sum(),
						connectsCancelled.sum(),
						closes.sum());
	}

	/** One-line summary for the driver's close log. */
	public String summary() {
		final var s = snapshot();
		final var text = new StringBuilder()
						.append("selections ").append(new TreeMap<>(s.selections()))
						.append("; connections new=").append(s.connectsNew())
						.append(" reused=").append(s.connectsReused())
						.append(" failed=").append(s.connectsFailed())
						.append(" cancelled=").append(s.connectsCancelled())
						.append(" closed=").append(s.closes());
		if (s.lookups() > 0) {
			text.append("; DNS lookups=").append(s.lookups())
							.append(" failures=").append(new TreeMap<>(s.lookupFailures()))
							.append(" latency ms min/mean/max=")
							.append(millis(s.minLookupNanos())).append('/')
							.append(millis(s.meanLookupNanos())).append('/')
							.append(millis(s.maxLookupNanos()));
		}
		return text.toString();
	}

	private static String millis(final long nanos) {
		return String.format(Locale.ROOT, "%.2f", nanos / (double) TimeUnit.MILLISECONDS.toNanos(1));
	}
}
