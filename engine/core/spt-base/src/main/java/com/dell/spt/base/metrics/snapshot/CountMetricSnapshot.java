package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;

/** Snapshot reporting a simple count metric. */
public interface CountMetricSnapshot extends Serializable {

	long count();
}
