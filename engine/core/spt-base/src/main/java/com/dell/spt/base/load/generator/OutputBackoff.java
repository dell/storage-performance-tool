package com.dell.spt.base.load.generator;

/**
 * Exponential back-off for the generator's "output made no progress" wait.
 *
 * <p>When the driver's input queue is full the generator can only re-check after a drain, and
 * drains happen in batches of thousands of operations a few times per second. Polling that
 * state every 50 µs costs ~10K timed-park wake-ups per second on a core the transport threads
 * share. The park therefore starts short, so a briefly full queue or a throttle refusal is
 * re-checked promptly, and doubles on each consecutive no-progress iteration up to a cap that
 * is still far below the drain interval. Any progress resets it.
 *
 * <p>Not used for the recycle wait: recycled operations return at request-latency cadence
 * (hundreds of microseconds at low concurrency), where a growing park would sit directly on
 * the re-admission path.
 */
final class OutputBackoff {

	static final long INITIAL_NANOS = 50_000L;
	static final long MAX_NANOS = 1_000_000L;

	private long nextNanos = INITIAL_NANOS;

	/** Returns the park duration for this no-progress iteration and grows the next one. */
	long nextParkNanos() {
		final var current = nextNanos;
		nextNanos = Math.min(MAX_NANOS, current << 1);
		return current;
	}

	/** Progress was made; the next no-progress wait starts short again. */
	void reset() {
		nextNanos = INITIAL_NANOS;
	}
}
