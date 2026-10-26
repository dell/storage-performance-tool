package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import java.io.Serializable;

/**
 * The immutable result of a closed Steady-State Window: its status, its coverage, and its
 * Steady-Window Statistics in the shape of a whole-step snapshot.
 */
public final class SteadyResult implements Serializable {

	/** Marks an absent acquisition or start offset. */
	public static final long NONE = -1L;

	private final SteadyStatus status;
	private final String cause;
	private final long startOffsetMillis;
	private final long durationNanos;
	private final double turnovers;
	private final Double trendPercent;
	private final Double cv;
	private final long startAcquisitionNanos;
	private final long endAcquisitionNanos;
	private final AllMetricsSnapshot statistics;

	public SteadyResult(
					final SteadyStatus status,
					final String cause,
					final long startOffsetMillis,
					final long durationNanos,
					final double turnovers,
					final Double trendPercent,
					final Double cv,
					final long startAcquisitionNanos,
					final long endAcquisitionNanos,
					final AllMetricsSnapshot statistics) {
		this.status = status;
		this.cause = cause;
		this.startOffsetMillis = startOffsetMillis;
		this.durationNanos = durationNanos;
		this.turnovers = turnovers;
		this.trendPercent = trendPercent;
		this.cv = cv;
		this.startAcquisitionNanos = startAcquisitionNanos;
		this.endAcquisitionNanos = endAcquisitionNanos;
		this.statistics = status.hasStatistics() ? statistics : null;
	}

	/** The Entry Node's result when no expected contributor closed a window it could report. */
	public static SteadyResult incomplete(final String cause) {
		return new SteadyResult(
						SteadyStatus.INCOMPLETE, cause, NONE, 0L, 0.0, null, null, NONE, NONE, null);
	}

	static SteadyResult notReached(final String cause, final long endAcquisitionNanos) {
		return new SteadyResult(
						SteadyStatus.NOT_REACHED, cause, NONE, 0L, 0.0, null, null, NONE, endAcquisitionNanos, null);
	}

	public SteadyStatus status() {
		return status;
	}

	/** What closed the window, such as a stop-path entry point or scheduling exhaustion. */
	public String cause() {
		return cause;
	}

	/** Window start measured from step start, or {@link #NONE} when the window never started. */
	public long startOffsetMillis() {
		return startOffsetMillis;
	}

	public long durationNanos() {
		return durationNanos;
	}

	/** Window completions, succeeded plus failed, per mean operation in flight. */
	public double turnovers() {
		return turnovers;
	}

	/** {@code SteadyTrend[%]}, or null when the window has too few bins to judge. */
	public Double trendPercent() {
		return trendPercent;
	}

	/** {@code SteadyCV}, or null when the window has too few bins to judge. */
	public Double cv() {
		return cv;
	}

	public long startAcquisitionNanos() {
		return startAcquisitionNanos;
	}

	public long endAcquisitionNanos() {
		return endAcquisitionNanos;
	}

	/** The window's statistics, or null unless the status reports them. */
	public AllMetricsSnapshot statistics() {
		return statistics;
	}

	/** A copy with the stability indicators computed at close. */
	SteadyResult withStability(final Double trendPercent, final Double cv) {
		return new SteadyResult(
						status,
						cause,
						startOffsetMillis,
						durationNanos,
						turnovers,
						trendPercent,
						cv,
						startAcquisitionNanos,
						endAcquisitionNanos,
						statistics);
	}

	@Override
	public String toString() {
		return "SteadyResult(" + status.label() + ", cause=" + cause + ", start=" + startOffsetMillis
						+ " ms, duration=" + durationNanos / 1_000_000 + " ms, turnovers=" + turnovers + ")";
	}
}
