package com.dell.spt.base.metrics.context;

import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.function.IntSupplier;

/** Fluent builder for constructing {@link MetricsContext} implementations. */
public interface ContextBuilder<B extends ContextBuilder, C extends MetricsContext> {

	C build();

	B loadStepId(final String id);

	B comment(final String comment);

	B opType(final OpType opType);

	B concurrencyLimit(final int concurrencyLimit);

	B concurrencyThreshold(final int concurrencyThreshold);

	B itemDataSize(final SizeInBytes itemDataSize);

	B stdOutColorFlag(final boolean stdOutColorFlag);

	B outputPeriodSec(final int outputPeriodSec);

	B actualConcurrencyGauge(final IntSupplier actualConcurrencyGauge);

	B runId(final long id);
}
