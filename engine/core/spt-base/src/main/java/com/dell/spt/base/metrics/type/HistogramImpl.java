package com.dell.spt.base.metrics.type;

import com.dell.spt.base.metrics.snapshot.HistogramSnapshotImpl;
import com.dell.spt.base.metrics.util.LongReservoir;
import com.dell.spt.base.metrics.snapshot.HistogramSnapshot;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe histogram meter backed by a {@link LongReservoir}. */
public class HistogramImpl implements LongMeter<HistogramSnapshot> {

	private final LongReservoir reservoir;
	private final LongAdder count;

	public HistogramImpl(final LongReservoir reservoir) {
		this.reservoir = reservoir;
		this.count = new LongAdder();
	}

	@Override
	public void update(final long value) {
		count.increment();
		reservoir.update(value);
	}

	@Override
	public HistogramSnapshotImpl snapshot() {
		return new HistogramSnapshotImpl(reservoir.snapshot());
	}
}
