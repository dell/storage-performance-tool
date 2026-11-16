package com.dell.spt.base.metrics.util;

/**
 * The rule that turns an active metrics context's progress into its {@code test_state} value.
 *
 * <p>Both the JSON metrics endpoints ({@code MetricsJsonResponder}) and the Prometheus
 * {@code /metrics} gauge ({@link PrometheusMetricsExporterImpl}) use it, so the two report
 * the same state for the same inputs. It covers active contexts only: Completed for a step
 * with no limit comes from the JSON terminal entry, or from the Prometheus series ending.
 */
public final class TestStateRule {

	/** The context has no snapshot yet. */
	public static final int IDLE = 0;
	/** The step is running. */
	public static final int RUNNING = 1;
	/** The step has reached its count or time limit. */
	public static final int COMPLETED = 2;

	private TestStateRule() {}

	/**
	 * Returns the state of an active context.
	 *
	 * @param timeLimitSec the context's time limit in seconds; 0 or less means none
	 * @param countLimit the context's operation count limit; 0 or less means none
	 * @param elapsedMillis the context's elapsed time in milliseconds
	 * @param successCount the operations completed successfully
	 * @param failsCount the operations failed
	 * @param lastConcurrency the last concurrency sample
	 * @return {@link #RUNNING} or {@link #COMPLETED}
	 */
	public static int activeState(
					final long timeLimitSec, final long countLimit, final long elapsedMillis,
					final long successCount, final long failsCount, final long lastConcurrency) {
		final long totalOps = successCount + failsCount;
		if (totalOps <= 0) {
			return RUNNING; // no operations issued yet
		}
		if (lastConcurrency > 0) {
			return RUNNING; // operations in flight
		}
		// concurrency == 0 with operations already issued is ambiguous: it can be a
		// genuine end-of-test, or merely a transient gap between asynchronous
		// operations (e.g. a rate-limited step, or the Netty S3 driver releasing its
		// concurrency permit between ops at low thread counts). For a duration-bounded
		// run, do not report Completed until the configured time limit has elapsed.
		if (timeLimitSec > 0 && elapsedMillis < timeLimitSec * 1000L) {
			return RUNNING; // still within the configured duration: transient gap
		}
		// Op-count-bounded runs hit the same gap. The count limit is per context, matching
		// the per-context totalOps, so do not report Completed until it has been reached.
		if (countLimit > 0 && totalOps < countLimit) {
			return RUNNING; // still below the configured op count: transient gap
		}
		// A step with neither a time nor a count limit (for example a READ of a listing
		// run to exhaustion, or a LIST) has no limit to tell a gap from the end, so its
		// active context always reports Running.
		if (timeLimitSec <= 0 && countLimit <= 0) {
			return RUNNING;
		}
		return COMPLETED;
	}
}
