package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
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

	void markPartSucc(final long bytes, final long duration, final long latency);

	void markSucc(
					final long count, final long bytes, final long durationValues[], final long latencyValues[]);

	void markPartSucc(final long bytes, final long durationValues[], final long latencyValues[]);

	void markFail();

	void markFail(final long count);

	void start();

	boolean isStarted();

	long startTimeStamp();

	void refreshLastSnapshot();

	S lastSnapshot();

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
