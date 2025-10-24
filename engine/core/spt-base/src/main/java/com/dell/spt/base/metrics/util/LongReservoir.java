package com.dell.spt.base.metrics.util;

/** Reservoir interface for capturing rolling sequences of long values. */
public interface LongReservoir {

	int size();

	void update(final long value);

	long[] snapshot();
}
