package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.logging.Loggers;
import java.util.Arrays;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The Steady-State Window of one metrics context.
 *
 * <p>The metrics manager calls {@link #observe()} on each iteration; it starts the window at the
 * first observation where completions since step start reach at least one and at least the
 * warm-up turnovers times the operations in flight. Above one warm-up turnover, the operations in
 * flight are the larger of the gauge at that observation and its mean sampled since step start,
 * and the window waits for the first such sample: at high rates the gauge can fall near zero for a
 * few milliseconds, and one such observation must not cut a long warm-up short. The first thread
 * that observes an end event calls {@link #close(String)}, which takes the end cut on that thread
 * and computes the result once. Later closers acquire no cut and wait for that result.
 *
 * <p>A monitor guards the transitions. Only the manager's start transition and the closers take
 * it, never the operation recording path. A close waits for a start cut in progress, and no start
 * begins after a close, so a context acquires at most two cuts.
 */
public final class SteadyWindow {

	/**
	 * Totals of a context's in-flight gauge samples, read together.
	 *
	 * @param sum the sum of the samples taken so far
	 * @param count the number of samples taken so far
	 */
	public record SampleTotals(long sum, long count) {

		/** No sample taken yet. */
		public static final SampleTotals NONE = new SampleTotals(0, 0);
	}

	/** Reads one metrics context's meters. */
	public interface Source {

		/** Succeeded operations since step start. */
		long succeeded();

		/** Failed operations since step start. */
		long failed();

		/** Operations in flight now, read fresh rather than from a cached snapshot. */
		int inFlight();

		/**
		 * The totals of the in-flight gauge's periodic samples, as one coherent pair. Read only when
		 * {@link SteadySettings#startFloorsInFlightAtItsMean()} holds.
		 */
		SampleTotals inFlightSamples();

		/** Reads every meter a cut records. */
		SteadyCut.Readings read();
	}

	/**
	 * Hears this window's two events, in order and at most once each. Called with the window's
	 * monitor held and, at scheduling exhaustion, on the metrics manager's thread, so a listener
	 * only queues work: it never waits and never reads a meter.
	 */
	public interface Listener {

		/** The start cut was taken at {@code startNanos}. */
		void onWindowStart(long startNanos);

		/**
		 * The end cut was taken at {@code endNanos}, or at the same clock when it failed.
		 *
		 * @param started whether the window ever started
		 * @param result the published result
		 */
		void onWindowClose(long endNanos, boolean started, SteadyResult result);
	}

	/** The stability series holds at most this many samples, about 12 KiB per window. */
	static final int SERIES_CAPACITY = 512;

	private enum State {
		OPEN, STARTED, CLOSED
	}

	private final SteadySettings settings;
	private final Source source;
	private final LongSupplier nanoClock;
	private final Object lock = new Object();
	private volatile boolean stepStarted;
	// The in-flight sample totals at step start, the baseline of the start rule's mean. Set only
	// when the settings floor the gauge at its mean.
	private volatile SampleTotals inFlightBaseline = SampleTotals.NONE;
	private volatile BooleanSupplier schedulingDrained;
	private volatile State state = State.OPEN;
	private volatile SteadyResult result;
	private volatile Listener listener;
	private long stepStartNanos;
	private SteadyCut startCut;
	private int cutCount;
	// The stability series: one sample per manager iteration (or per stride once halved), the
	// start cut first and the end cut last. Allocated at the start, dropped at close.
	private long[] sampleTimes, sampleSucceeded, sampleFailed;
	private int sampleCount, sampleStride = 1, samplesSkipped;

	public SteadyWindow(final SteadySettings settings, final Source source, final LongSupplier nanoClock) {
		this.settings = settings;
		this.source = source;
		this.nanoClock = nanoClock;
	}

	/** Sets the listener of this window's events, before the step starts. */
	public void listener(final Listener windowListener) {
		listener = windowListener;
	}

	/**
	 * Marks step start, the origin of the start offset and of the start rule's sampled mean. The
	 * metrics context calls this holding its refresh lock, so neither a {@link Source} nor a
	 * {@link Listener} may take that lock.
	 */
	public void markStepStart() {
		synchronized (lock) {
			stepStartNanos = nanoClock.getAsLong();
			if (settings.startFloorsInFlightAtItsMean()) {
				inFlightBaseline = source.inFlightSamples();
			}
			stepStarted = true;
		}
	}

	/**
	 * Arms the end edge of a step whose scheduling ended (a count limit or exhausted input). The
	 * generator hands its last operation to the driver's queue long before the driver dispatches
	 * it, so the window closes at the first observation at which nothing is left to dispatch.
	 *
	 * @param drained whether every operation the driver accepted has been dispatched
	 */
	public void markSchedulingEnded(final BooleanSupplier drained) {
		schedulingDrained = drained;
	}

	/**
	 * One metrics-manager observation: starts the window when the start rule holds, appends to
	 * its stability series while it is open, and closes it once scheduling ended and drained.
	 */
	public void observe() {
		if (!stepStarted) {
			return;
		}
		final BooleanSupplier drained = schedulingDrained;
		if (drained != null && state != State.CLOSED && drained.getAsBoolean()) {
			close("scheduling exhaustion");
			return;
		}
		final State current = state;
		if (current == State.STARTED) {
			synchronized (lock) {
				if (state == State.STARTED && ++samplesSkipped >= sampleStride) {
					samplesSkipped = 0;
					appendSample(nanoClock.getAsLong(), source.succeeded(), source.failed());
				}
			}
			return;
		}
		if (current != State.OPEN) {
			return;
		}
		final long completions = source.succeeded() + source.failed();
		final double inFlight;
		if (settings.startFloorsInFlightAtItsMean()) {
			final SampleTotals baseline = inFlightBaseline;
			final SampleTotals samples = source.inFlightSamples();
			final long count = samples.count() - baseline.count();
			if (count <= 0) {
				return; // no sample since step start yet
			}
			inFlight = Math.max(source.inFlight(), (double) (samples.sum() - baseline.sum()) / count);
		} else {
			inFlight = source.inFlight();
		}
		if (completions < Math.max(1.0, settings.warmupTurnovers() * inFlight)) {
			return;
		}
		synchronized (lock) {
			if (state == State.OPEN) {
				startCut = cut();
				sampleTimes = new long[SERIES_CAPACITY];
				sampleSucceeded = new long[SERIES_CAPACITY];
				sampleFailed = new long[SERIES_CAPACITY];
				appendSample(startCut);
				state = State.STARTED;
				notifyStart(startCut.timeNanos());
			}
		}
	}

	/**
	 * Closes the window at an end event, on the calling thread. The first caller takes the end cut
	 * and computes the result; every caller returns that result.
	 *
	 * @param cause what ended the window, for diagnostics
	 */
	public SteadyResult close(final String cause) {
		final SteadyResult published = result;
		if (published != null) {
			return published;
		}
		synchronized (lock) {
			if (result == null) {
				SteadyResult computed;
				final boolean started = startCut != null;
				long endNanos = Long.MIN_VALUE;
				try {
					final SteadyCut endCut = cut();
					endNanos = endCut.timeNanos();
					computed = SteadyComputation.compute(settings, stepStartNanos, startCut, endCut, cause);
					if (computed.status().hasStatistics()) {
						appendSample(endCut);
						final SteadyStability.Indicators indicators = SteadyStability.compute(
										sampleTimes,
										sampleSucceeded,
										sampleFailed,
										sampleCount,
										computed.statistics().concurrencySnapshot().mean());
						computed = computed.withStability(indicators.trendPercent(), indicators.cv());
					}
				} catch (final RuntimeException e) {
					// A close never fails the stop path that calls it, and never takes a third cut.
					Loggers.MSG.warn("Steady-state window closed by {} could not be measured: {}", cause, e.toString());
					computed = SteadyResult.notReached(cause + "; measurement failed", SteadyResult.NONE);
				}
				startCut = null;
				sampleTimes = sampleSucceeded = sampleFailed = null;
				state = State.CLOSED;
				// Before the result is visible: a closer that takes the fast path must find this
				// window's bucket work already queued, or it would return from the fence without it.
				try {
					notifyClose(endNanos == Long.MIN_VALUE ? nanoClock.getAsLong() : endNanos, started, computed);
				} finally {
					// Even if the listener throws an Error: a later closer must never take a third cut.
					result = computed;
				}
				if (Loggers.MSG.isDebugEnabled()) {
					Loggers.MSG.debug(
									"Steady-state window closed by {}: {}, cut acquisition {} ns (start) and {} ns (end)",
									cause,
									result,
									result.startAcquisitionNanos(),
									result.endAcquisitionNanos());
				}
			}
			return result;
		}
	}

	private void notifyStart(final long startNanos) {
		final Listener current = listener;
		if (current != null) {
			try {
				current.onWindowStart(startNanos);
			} catch (final RuntimeException e) {
				Loggers.MSG.warn("Steady-state window start listener failed: {}", e.toString());
			}
		}
	}

	private void notifyClose(final long endNanos, final boolean started, final SteadyResult closed) {
		final Listener current = listener;
		if (current != null) {
			try {
				current.onWindowClose(endNanos, started, closed);
			} catch (final RuntimeException e) {
				Loggers.MSG.warn("Steady-state window close listener failed: {}", e.toString());
			}
		}
	}

	/** The published result, or null while the window is open. */
	public SteadyResult result() {
		return result;
	}

	public boolean isStarted() {
		return state != State.OPEN;
	}

	/** The number of cuts acquired so far; never more than two. */
	int cutCount() {
		synchronized (lock) {
			return cutCount;
		}
	}

	/** A copy of the stability series' sample times while the window is open. */
	long[] sampleTimes() {
		synchronized (lock) {
			return sampleTimes == null ? new long[0] : Arrays.copyOf(sampleTimes, sampleCount);
		}
	}

	private void appendSample(final SteadyCut cut) {
		appendSample(cut.timeNanos(), cut.readings().succeeded(), cut.readings().failed());
	}

	/** Appends one sample; a full series keeps every other sample and doubles its stride. */
	private void appendSample(final long time, final long succeeded, final long failed) {
		if (sampleCount == SERIES_CAPACITY) {
			final int kept = (SERIES_CAPACITY + 1) / 2;
			for (int i = 1; i < kept; i++) {
				sampleTimes[i] = sampleTimes[2 * i];
				sampleSucceeded[i] = sampleSucceeded[2 * i];
				sampleFailed[i] = sampleFailed[2 * i];
			}
			sampleCount = kept;
			sampleStride *= 2;
		}
		sampleTimes[sampleCount] = time;
		sampleSucceeded[sampleCount] = succeeded;
		sampleFailed[sampleCount] = failed;
		sampleCount++;
	}

	private SteadyCut cut() {
		final long before = nanoClock.getAsLong();
		final SteadyCut.Readings readings = source.read();
		final long after = nanoClock.getAsLong();
		cutCount++;
		return new SteadyCut(before, after - before, readings);
	}
}
