package com.dell.spt.base.metrics.type;

import java.io.Serializable;

/** Meter abstraction that records long values and exposes a snapshot. */
public interface LongMeter<S extends Serializable> {

	void update(final long v);

	S snapshot();
}
