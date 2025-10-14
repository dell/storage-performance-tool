package com.dell.spt.base.metrics.util;

/** @author veronika K. on 03.10.18 */
public interface LongReservoir {

	int size();

	void update(final long value);

	long[] snapshot();
}
