package com.dell.spt.base.metrics.type;

import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;

/** Meter that tracks rate metrics over a configurable sampling period. */
public interface RateMeter<S extends RateMetricSnapshot> extends LongMeter<S> {

	int DEFAULT_PERIOD_SECONDS = 1;

	void resetStartTime();
}
