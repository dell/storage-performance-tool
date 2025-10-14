package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;

public interface ElapsedTimeMetricSnapshot extends Serializable {

	long elapsedTimeMillis();
}
