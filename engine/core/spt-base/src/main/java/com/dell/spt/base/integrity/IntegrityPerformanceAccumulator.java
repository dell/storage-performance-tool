package com.dell.spt.base.integrity;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Lock-free aggregate digest telemetry; no per-object samples are retained. */
public final class IntegrityPerformanceAccumulator {

	private final LongAdder objects = new LongAdder();
	private final LongAdder bytes = new LongAdder();
	private final LongAdder workerNanos = new LongAdder();
	private final LongAdder additionalPayloadPasses = new LongAdder();
	private final AtomicLong firstPrehashStartedNanos = new AtomicLong();
	private final AtomicLong firstRequestDelayNanos = new AtomicLong(-1);

	public void recordDigest(final long byteCount, final long digestWorkerNanos) {
		if (byteCount < 0 || digestWorkerNanos < 0) {
			throw new IllegalArgumentException("integrity digest telemetry must be nonnegative");
		}
		objects.increment();
		bytes.add(byteCount);
		workerNanos.add(digestWorkerNanos);
	}

	public void markPrehashStarted(final long nowNanos) {
		firstPrehashStartedNanos.compareAndSet(0, nowNanos);
	}

	public void markFirstRequestDispatched(final long nowNanos) {
		final long started = firstPrehashStartedNanos.get();
		if (started > 0) {
			firstRequestDelayNanos.compareAndSet(-1, Math.max(0, nowNanos - started));
		}
	}

	public void recordAdditionalPayloadPass() {
		additionalPayloadPasses.increment();
	}

	public Snapshot snapshot() {
		return new Snapshot(
						objects.sum(),
						bytes.sum(),
						workerNanos.sum(),
						firstRequestDelayNanos.get(),
						additionalPayloadPasses.sum());
	}

	public record Snapshot(
					long objects,
					long bytes,
					long workerNanos,
					long firstRequestDelayNanos,
					long additionalPayloadPasses) {}
}
