package com.dell.spt.base.metrics.context;

import java.util.function.BooleanSupplier;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.github.akurilov.commons.system.SizeInBytes;

import java.util.Map;

/** Created by andrey on 14.07.16. */
public interface MetricsContext<S extends AllMetricsSnapshot>
				extends AutoCloseable, Comparable<MetricsContext<S>> {

	int DEFAULT_SNAPSHOT_UPDATE_PERIOD_MILLIS = 100;
	int DEFAULT_RESERVOIR_SIZE = 1028;

	// these are useful as labels/tags
	Map metadata();

	String loadStepId();

	long runId();

	OpType opType();

	int concurrencyLimit();

	SizeInBytes itemDataSize();
	// metrics accounting methods

	void markSucc(final long bytes, final long duration, final long latency);

	void markSucc(final long bytes, final long duration, final long latency, final long ttfb);

	void markPartSucc(final long bytes, final long duration, final long latency);

	void markPartSucc(final long bytes, final long duration, final long latency, final long ttfb);

	void markSucc(
					final long count, final long bytes, final long durationValues[], final long latencyValues[]);

	void markSucc(
					final long count,
					final long bytes,
					final long durationValues[],
					final long latencyValues[],
					final long ttfbValues[]);

	void markPartSucc(final long bytes, final long durationValues[], final long latencyValues[]);

	void markPartSucc(
					final long bytes,
					final long durationValues[],
					final long latencyValues[],
					final long ttfbValues[]);

	void markFail();

	/** Records a failed logical request together with its request-scoped timings. */
	default void markFail(final long duration, final long latency) {
		markFail();
	}

	void markFail(final long count);

	/** Marks one terminal corruption, which is also a failed operation. */
	default void markCorrupt() {
		markFail();
	}

	void start();

	boolean isStarted();

	long startTimeStamp();

	void refreshLastSnapshot();

	default void refreshLastSnapshot(final boolean force) {
		refreshLastSnapshot();
	}

	S lastSnapshot();

	/**
	 * One metrics-manager observation of the Steady-State Window: starts the window when the
	 * start rule holds. Contexts that measure no window ignore it.
	 */
	default void observeSteadyWindow() {}

	/**
	 * Closes the Steady-State Window on the calling thread at an end event, if it is not already
	 * closed, and returns its result. Every caller gets the first closer's result.
	 *
	 * @param cause what ended the window, for diagnostics
	 * @return the result, or null for a context that measures no window
	 */
	default SteadyResult closeSteadyWindow(final String cause) {
		return null;
	}

	/**
	 * Arms the Steady-State Window's end edge once the step's scheduling ended: the metrics manager
	 * closes the window at its first observation at which {@code drained} holds.
	 */
	default void markSchedulingEnded(final BooleanSupplier drained) {}

	/** The Steady-State Window's result once it is closed, otherwise null. */
	default SteadyResult steadyResult() {
		return null;
	}

	// threshold-related accounting methods below

	int concurrencyThreshold();

	boolean thresholdStateEntered();

	void enterThresholdState() throws IllegalStateException;

	boolean thresholdStateExited();

	MetricsContext thresholdMetrics();

	void exitThresholdState() throws IllegalStateException;
	// output configuration methods below

	boolean stdOutColorEnabled();

	boolean avgPersistEnabled();

	boolean sumPersistEnabled();

	boolean timingPersistEnabled();

	long outputPeriodMillis();

	long lastOutputTs();

	void lastOutputTs(final long ts);

	long elapsedTimeMillis();

	String comment();

	@Override
	void close();
}
