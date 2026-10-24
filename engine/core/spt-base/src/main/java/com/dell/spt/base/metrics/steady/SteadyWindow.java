package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.logging.Loggers;
import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * The Steady-State Window of one metrics context.
 *
 * <p>The metrics manager calls {@link #observe()} on each iteration; it starts the window at the
 * first observation where completions since step start reach at least one and at least the
 * warm-up turnovers times the operations in flight. The first thread that observes an end event
 * calls {@link #close(String)}, which takes the end cut on that thread and computes the result
 * once. Later closers acquire no cut and wait for that result.
 *
 * <p>A monitor guards the transitions. Only the manager's start transition and the closers take
 * it, never the operation recording path. A close waits for a start cut in progress, and no start
 * begins after a close, so a context acquires at most two cuts.
 */
public final class SteadyWindow {

	/** Reads one metrics context's meters. */
	public interface Source {

		/** Succeeded operations since step start. */
		long succeeded();

		/** Failed operations since step start. */
		long failed();

		/** Operations in flight now, read fresh rather than from a cached snapshot. */
		int inFlight();

		/** Reads every meter a cut records. */
		SteadyCut.Readings read();
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
	private volatile State state = State.OPEN;
	private volatile SteadyResult result;
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

	/** Marks step start, the origin of the start offset. */
	public void markStepStart() {
		synchronized (lock) {
			stepStartNanos = nanoClock.getAsLong();
			stepStarted = true;
		}
	}

	/**
	 * One metrics-manager observation: starts the window when the start rule holds, and while the
	 * window is open appends to its stability series.
	 */
	public void observe() {
		if (!stepStarted) {
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
		if (completions < Math.max(1.0, settings.warmupTurnovers() * source.inFlight())) {
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
				final SteadyCut endCut = cut();
				SteadyResult computed = SteadyComputation.compute(settings, stepStartNanos, startCut, endCut, cause);
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
				result = computed;
				startCut = null;
				sampleTimes = sampleSucceeded = sampleFailed = null;
				state = State.CLOSED;
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
