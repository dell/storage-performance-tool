package com.dell.spt.storage.driver.coop.aws.s3.metrics;

import com.dell.spt.storage.driver.coop.aws.s3.config.ObjectSizeProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Workload metrics collector for adaptive tuning.
 * Collects statistics about object sizes, latencies, and operation counts
 * to enable data-driven configuration decisions.
 */
public class WorkloadMetrics {
	private static final Logger LOG = LoggerFactory.getLogger(WorkloadMetrics.class);

	private final AtomicLong totalOperations = new AtomicLong();
	private final AtomicLong totalBytes = new AtomicLong();
	private final AtomicLong totalLatencyNanos = new AtomicLong();
	private final LongAdder[] operationCountsBySize;
	private final LongAdder[] latencyBySize;
	private final LongAdder[] bytesBySize;

	public WorkloadMetrics() {
		int sizeBuckets = ObjectSizeProfile.values().length;
		operationCountsBySize = new LongAdder[sizeBuckets];
		latencyBySize = new LongAdder[sizeBuckets];
		bytesBySize = new LongAdder[sizeBuckets];
		for (int i = 0; i < sizeBuckets; i++) {
			operationCountsBySize[i] = new LongAdder();
			latencyBySize[i] = new LongAdder();
			bytesBySize[i] = new LongAdder();
		}
	}

	/**
	 * Record a completed operation.
	 *
	 * @param size The object size in bytes
	 * @param latencyNanos The operation latency in nanoseconds
	 */
	public void recordOperation(final long size, final long latencyNanos) {
		totalOperations.incrementAndGet();
		totalBytes.addAndGet(size);
		totalLatencyNanos.addAndGet(latencyNanos);

		int bucket = getSizeBucket(size);
		operationCountsBySize[bucket].increment();
		latencyBySize[bucket].add(latencyNanos);
		bytesBySize[bucket].add(size);
	}

	/**
	 * Get the total number of operations recorded.
	 *
	 * @return Total operation count
	 */
	public long getTotalOperations() {
		return totalOperations.get();
	}

	/**
	 * Get the total bytes transferred.
	 *
	 * @return Total bytes
	 */
	public long getTotalBytes() {
		return totalBytes.get();
	}

	/**
	 * Get the average object size.
	 *
	 * @return Average object size in bytes
	 */
	public double getAverageObjectSize() {
		long ops = totalOperations.get();
		return ops > 0 ? (double) totalBytes.get() / ops : 0;
	}

	/**
	 * Get the average latency in milliseconds.
	 *
	 * @return Average latency in milliseconds
	 */
	public double getAverageLatencyMs() {
		long ops = totalOperations.get();
		return ops > 0 ? (totalLatencyNanos.get() / 1_000_000.0) / ops : 0;
	}

	/**
	 * Get the average latency in nanoseconds.
	 *
	 * @return Average latency in nanoseconds
	 */
	public double getAverageLatencyNanos() {
		long ops = totalOperations.get();
		return ops > 0 ? (double) totalLatencyNanos.get() / ops : 0;
	}

	/**
	 * Get the operation count for a specific size profile.
	 *
	 * @param profile The object size profile
	 * @return Operation count for that profile
	 */
	public long getOperationCount(final ObjectSizeProfile profile) {
		return operationCountsBySize[profile.ordinal()].sum();
	}

	/**
	 * Get the average latency for a specific size profile.
	 *
	 * @param profile The object size profile
	 * @return Average latency in milliseconds for that profile
	 */
	public double getAverageLatencyMs(final ObjectSizeProfile profile) {
		long count = operationCountsBySize[profile.ordinal()].sum();
		if (count == 0) {
			return 0;
		}
		return (latencyBySize[profile.ordinal()].sum() / 1_000_000.0) / count;
	}

	/**
	 * Get the total bytes for a specific size profile.
	 *
	 * @param profile The object size profile
	 * @return Total bytes for that profile
	 */
	public long getTotalBytes(final ObjectSizeProfile profile) {
		return bytesBySize[profile.ordinal()].sum();
	}

	/**
	 * Get the dominant object size profile based on operation count.
	 *
	 * @return The profile with the most operations
	 */
	public ObjectSizeProfile getDominantProfile() {
		ObjectSizeProfile dominant = ObjectSizeProfile.MEDIUM;
		long maxCount = 0;

		for (ObjectSizeProfile profile : ObjectSizeProfile.values()) {
			long count = operationCountsBySize[profile.ordinal()].sum();
			if (count > maxCount) {
				maxCount = count;
				dominant = profile;
			}
		}

		return dominant;
	}

	/**
	 * Get a summary of the metrics.
	 *
	 * @return A string summary
	 */
	public String getSummary() {
		return String.format(
						"WorkloadMetrics{totalOps=%d, totalBytes=%d, avgSize=%.2fKB, avgLatency=%.2fms, dominantProfile=%s}",
						getTotalOperations(),
						getTotalBytes(),
						getAverageObjectSize() / 1024.0,
						getAverageLatencyMs(),
						getDominantProfile());
	}

	/**
	 * Reset all metrics.
	 */
	public void reset() {
		totalOperations.set(0);
		totalBytes.set(0);
		totalLatencyNanos.set(0);
		for (int i = 0; i < operationCountsBySize.length; i++) {
			operationCountsBySize[i].reset();
			latencyBySize[i].reset();
			bytesBySize[i].reset();
		}
		LOG.debug("WorkloadMetrics reset");
	}

	/**
	 * Map an object size to a bucket index.
	 *
	 * @param size The object size in bytes
	 * @return The bucket index (0-3)
	 */
	private int getSizeBucket(final long size) {
		if (size < 64 * 1024) {
			return ObjectSizeProfile.SMALL.ordinal();
		} else if (size < 8 * 1024 * 1024) {
			return ObjectSizeProfile.MEDIUM.ordinal();
		} else if (size < 100 * 1024 * 1024) {
			return ObjectSizeProfile.LARGE.ordinal();
		} else {
			return ObjectSizeProfile.VERY_LARGE.ordinal();
		}
	}
}
