package com.dell.spt.base.load.generator;

/**
 * Exponential back-off for the generator's "output made no progress" wait.
 *
 * <p>When the driver's input queue is full the generator can only re-check after a drain, and
 * drains happen in batches of thousands of operations a few times per second. Polling that
 * state every 50 µs costs ~10K timed-park wake-ups per second on a core the transport threads
 * share. The park therefore starts short, so a briefly full queue or a throttle refusal is
 * re-checked promptly, and doubles on each consecutive no-progress iteration up to a cap that
 * is still far below the drain interval.
 *
 * <p>Only a fully accepted output resets the wait. While the queue is full the dispatcher
 * frees one slot per completed operation, so the generator sees a trickle of one-operation
 * successes between refusals; treating each as "unblocked" would restart the short wait
 * every time and keep the wake-up rate at request frequency. A partial acceptance therefore
 * holds the current wait, which at the cap lets several slots accumulate per wake-up.
 *
 * <p>A rate throttle refusing permits is not backpressure: permits return on the configured
 * period (hundreds of microseconds at typical limits), so that wait stays at the short fixed
 * duration and leaves the queue back-off untouched. Growing it there capped the generator's
 * wake-up rate below the permit rate and collapsed rate-limited throughput.
 *
 * <p>Not used for the recycle wait: recycled operations return at request-latency cadence
 * (hundreds of microseconds at low concurrency), where a growing park would sit directly on
 * the re-admission path.
 */
final class OutputBackoff {

	static final long INITIAL_NANOS = 50_000L;
	static final long MAX_NANOS = 1_000_000L;

	private long nextNanos = INITIAL_NANOS;

	/**
	 * Decides the wait after one output attempt. Returns the nanoseconds to park, or zero when
	 * operations were accepted and the generator should continue immediately.
	 *
	 * @param permitted operations the throttles allowed this iteration
	 * @param accepted operations the output actually took
	 */
	long parkNanosAfterOutput(final int permitted, final int accepted) {
		if (permitted <= 0) {
			return INITIAL_NANOS;
		}
		if (accepted <= 0) {
			return nextParkNanos();
		}
		onProgress(accepted, permitted);
		return 0L;
	}

	/** Returns the park duration for this no-progress iteration and grows the next one. */
	long nextParkNanos() {
		final var current = nextNanos;
		nextNanos = Math.min(MAX_NANOS, current << 1);
		return current;
	}

	/**
	 * Records that {@code accepted} of {@code requested} operations were output. A complete
	 * acceptance means the output is no longer blocked and the next wait starts short again; a
	 * partial one leaves the wait unchanged.
	 */
	void onProgress(final int accepted, final int requested) {
		if (accepted >= requested) {
			nextNanos = INITIAL_NANOS;
		}
	}
}
