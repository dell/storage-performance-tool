package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;

/**
 * The statistics the S3-RDMA driver reports through {@link DriverStatsSnapshot}.
 *
 * <p>The names appear unchanged in the metrics JSON and in stored results, so a name keeps its
 * meaning once shipped. Every name is always reported, with zero when the buffer pool is disabled,
 * so that the participants of a fleet report the same set.
 */
final class RdmaDriverStats {

	static final String DRIVER = S3RdmaStorageDriverExtension.NAME;

	static final String RDMA_REQUESTS_SENT = "rdma_requests_sent";
	static final String RDMA_TRANSFERRED = "rdma_transferred";
	static final String RDMA_BYTES_ASSUMED = "rdma_bytes_assumed";
	static final String RDMA_DECLINED_SERVED_OVER_HTTP = "rdma_declined_served_over_http";
	static final String RDMA_DECLINED_FAILED = "rdma_declined_failed";
	static final String RDMA_REQUEST_FAILED = "rdma_request_failed";
	static final String RDMA_PROTOCOL_ERROR = "rdma_protocol_error";
	static final String RDMA_TIMED_OUT = "rdma_timed_out";
	static final String HTTP_RDMA_UNAVAILABLE = "http_rdma_unavailable";
	static final String HTTP_BELOW_THRESHOLD = "http_below_threshold";
	static final String HTTP_INELIGIBLE = "http_ineligible";
	static final String HTTP_OVERSIZE = "http_oversize";
	static final String HTTP_FALLBACK = "http_fallback";
	static final String PREPARE_FAILED = "prepare_failed";

	static final String REGISTRATIONS_POOLED = "registrations_pooled";
	static final String REGISTRATIONS_ON_DEMAND = "registrations_on_demand";
	static final String REGISTRATION_FAILED_POOLED = "registration_failed_pooled";
	static final String REGISTRATION_FAILED_ON_DEMAND = "registration_failed_on_demand";
	static final String ALLOCATION_FAILED_POOLED = "allocation_failed_pooled";
	static final String ALLOCATION_FAILED_ON_DEMAND = "allocation_failed_on_demand";
	static final String TOKEN_GENERATION_FAILED = "token_generation_failed";

	static final String POOL_HITS = "pool_hits";
	static final String POOL_EXHAUSTED = "pool_exhausted";
	static final String POOL_OVERSIZE = "pool_oversize";
	static final String POOL_EVICTED = "pool_evicted";
	static final String POOL_DISCARDED = "pool_discarded";
	static final String POOL_INVALID_RETURNS = "pool_invalid_returns";

	/** Gauge: 1 per driver, so a fleet total gives the number of drivers summed. */
	static final String DRIVERS = "drivers";
	/** Gauge: 1 while the RDMA transport is initialized. */
	static final String TRANSPORT_AVAILABLE = "transport_available";
	/** Gauge: capacity the pool has reserved, including a buffer that is still being created. */
	static final String POOL_RESERVED_BYTES = "pool_reserved_bytes";
	/** Gauge: the most the pool may reserve. */
	static final String POOL_BUDGET_BYTES = "pool_budget_bytes";
	/** Gauge: buffers the pool holds, leased or idle. */
	static final String POOL_BUFFERS = "pool_buffers";
	/** Gauge: pool buffers no request holds. */
	static final String POOL_IDLE_BUFFERS = "pool_idle_buffers";
	/** Gauge: the largest {@link #POOL_RESERVED_BYTES} since the driver started. */
	static final String POOL_PEAK_RESERVED_BYTES = "pool_peak_reserved_bytes";

	private static final RdmaBufferPool.Levels NO_POOL = new RdmaBufferPool.Levels(0, 0, 0, 0, 0);

	private RdmaDriverStats() {}

	/**
	 * Reads the counters and the pool's levels into a snapshot. The levels are one read under the
	 * pool's lock, so they agree with one another; the counters are read one by one and may be a few
	 * requests apart. Calls nothing native.
	 *
	 * @param path the driver's data path counts
	 * @param pool the driver's buffer pool, or {@code null} when it is disabled
	 * @param transportAvailable whether the RDMA transport is initialized
	 */
	static DriverStatsSnapshot snapshot(
					final RdmaPathStats path, final RdmaBufferPool pool, final boolean transportAvailable) {
		final RdmaBufferPool.Levels levels = pool == null ? NO_POOL : pool.levels();
		return DriverStatsSnapshot.builder(DRIVER)
						.counter(RDMA_REQUESTS_SENT, path.rdmaRequestsSent.sum())
						.counter(RDMA_TRANSFERRED, path.rdmaTransferred.sum())
						.counter(RDMA_BYTES_ASSUMED, path.rdmaBytesAssumed.sum())
						.counter(RDMA_DECLINED_SERVED_OVER_HTTP, path.rdmaDeclinedServedOverHttp.sum())
						.counter(RDMA_DECLINED_FAILED, path.rdmaDeclinedFailed.sum())
						.counter(RDMA_REQUEST_FAILED, path.rdmaHttpError.sum())
						.counter(RDMA_PROTOCOL_ERROR, path.rdmaProtocolError.sum())
						.counter(RDMA_TIMED_OUT, path.rdmaTimedOut.sum())
						.counter(HTTP_RDMA_UNAVAILABLE, path.httpRdmaUnavailable.sum())
						.counter(HTTP_BELOW_THRESHOLD, path.httpBelowThreshold.sum())
						.counter(HTTP_INELIGIBLE, path.httpIneligible.sum())
						.counter(HTTP_OVERSIZE, path.httpOversize.sum())
						.counter(HTTP_FALLBACK, path.httpFallback.sum())
						.counter(PREPARE_FAILED, path.prepareFailed.sum())
						.counter(REGISTRATIONS_POOLED, pool == null ? 0 : pool.created.sum())
						.counter(REGISTRATIONS_ON_DEMAND, path.registrationsOnDemand.sum())
						.counter(REGISTRATION_FAILED_POOLED, pool == null ? 0 : pool.registrationFailed.sum())
						.counter(REGISTRATION_FAILED_ON_DEMAND, path.registrationFailedOnDemand.sum())
						.counter(ALLOCATION_FAILED_POOLED, pool == null ? 0 : pool.allocationFailed.sum())
						.counter(ALLOCATION_FAILED_ON_DEMAND, path.allocationFailedOnDemand.sum())
						.counter(TOKEN_GENERATION_FAILED, path.tokenGenerationFailed.sum())
						.counter(POOL_HITS, pool == null ? 0 : pool.hits.sum())
						.counter(POOL_EXHAUSTED, pool == null ? 0 : pool.exhausted.sum())
						.counter(POOL_OVERSIZE, pool == null ? 0 : pool.unpooled.sum())
						.counter(POOL_EVICTED, pool == null ? 0 : pool.evicted.sum())
						.counter(POOL_DISCARDED, pool == null ? 0 : pool.discarded.sum())
						.counter(POOL_INVALID_RETURNS, pool == null ? 0 : pool.invalidReturns.sum())
						.gauge(DRIVERS, 1)
						.gauge(TRANSPORT_AVAILABLE, transportAvailable ? 1 : 0)
						.gauge(POOL_RESERVED_BYTES, levels.reservedBytes())
						.gauge(POOL_BUDGET_BYTES, levels.budgetBytes())
						.gauge(POOL_BUFFERS, levels.buffers())
						.gauge(POOL_IDLE_BUFFERS, levels.idleBuffers())
						.gauge(POOL_PEAK_RESERVED_BYTES, levels.peakReservedBytes())
						.build();
	}
}
