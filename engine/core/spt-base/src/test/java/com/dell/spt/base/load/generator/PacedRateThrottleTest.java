package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.akurilov.commons.concurrent.throttle.RateThrottle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

class PacedRateThrottleTest {

	private static final int BATCH = 32_768;
	private static final long MILLI = 1_000_000L;

	private final AtomicLong clock = new AtomicLong(5 * MILLI);

	private PacedRateThrottle throttle(final double rate) {
		return new PacedRateThrottle(rate, clock::get);
	}

	@Test
	void firstAcquisitionGrantsOnePermitNotTheWholeRequest() {
		final var throttle = throttle(1000);
		assertEquals(1, throttle.tryAcquire(BATCH));
		assertEquals(0, throttle.tryAcquire(BATCH), "no further permit within the first period");
	}

	/** The defect this class fixes: the library throttle grants the generator's whole first batch. */
	@Test
	void libraryRateThrottleGrantsTheWholeFirstRequest() {
		assertEquals(BATCH, new RateThrottle(1000).tryAcquire(BATCH));
	}

	@Test
	void permitsAccrueOnePerPeriod() {
		final var throttle = throttle(1000);
		assertEquals(1, throttle.tryAcquire(BATCH));
		clock.addAndGet(MILLI / 2);
		assertEquals(0, throttle.tryAcquire(BATCH));
		clock.addAndGet(MILLI / 2);
		assertEquals(1, throttle.tryAcquire(BATCH));
		clock.addAndGet(9 * MILLI);
		assertEquals(9, throttle.tryAcquire(BATCH));
	}

	@Test
	void unusedAllowanceStaysAvailableButIsNeverExceeded() {
		final var throttle = throttle(100);
		assertEquals(1, throttle.tryAcquire(BATCH));
		clock.addAndGet(1000 * MILLI);
		assertEquals(40, throttle.tryAcquire(40));
		assertEquals(60, throttle.tryAcquire(BATCH));
		assertEquals(0, throttle.tryAcquire(BATCH));
	}

	@Test
	void grantsFollowElapsedTimeWithoutAStartBurst() {
		final double rate = 8000;
		final var throttle = throttle(rate);
		final long start = clock.get();
		final long step = 100_000L; // the generator asks for a whole batch every 100 us
		long granted = 0;
		int largest = 0;
		for (long t = 0; t <= 60_000 * MILLI; t += step) {
			clock.set(start + t);
			final int n = throttle.tryAcquire(BATCH);
			assertTrue(n >= 0 && n <= BATCH);
			granted += n;
			largest = Math.max(largest, n);
		}
		assertEquals(60 * 8000 + 1, granted, "one permit at the start plus one per period");
		assertTrue(largest <= 2, "no call may take more than one step's allowance, got " + largest);
	}

	@Test
	void periodIsTruncatedToWholeNanosecondsLikeTheLibraryThrottle() {
		final var throttle = throttle(3);
		assertEquals(1, throttle.tryAcquire(BATCH));
		clock.addAndGet(333_333_332L);
		assertEquals(0, throttle.tryAcquire(BATCH));
		clock.addAndGet(1);
		assertEquals(1, throttle.tryAcquire(BATCH));
	}

	@Test
	void nonPositiveRequestsGrantNothingAndDoNotStartTheClock() {
		final var throttle = throttle(1000);
		assertEquals(0, throttle.tryAcquire(0));
		assertEquals(0, throttle.tryAcquire(-5));
		clock.addAndGet(10 * MILLI);
		assertEquals(1, throttle.tryAcquire(BATCH), "the clock starts at the first real request");
	}

	@Test
	void singlePermitAcquire() {
		final var throttle = throttle(1000);
		assertTrue(throttle.tryAcquire());
		assertFalse(throttle.tryAcquire());
		clock.addAndGet(MILLI);
		assertTrue(throttle.tryAcquire());
	}

	@Test
	void rejectsRatesThatAreNotPositive() {
		assertThrows(IllegalArgumentException.class, () -> new PacedRateThrottle(0));
		assertThrows(IllegalArgumentException.class, () -> new PacedRateThrottle(-1));
		assertThrows(IllegalArgumentException.class, () -> new PacedRateThrottle(Double.NaN));
	}

	@Test
	void concurrentAcquisitionsNeverExceedTheAllowance() throws Exception {
		final var throttle = throttle(1000);
		throttle.tryAcquire(1);
		clock.addAndGet(10_000 * MILLI); // 10,000 permits accrued
		final var granted = new LongAdder();
		final var go = new CountDownLatch(1);
		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			threads.add(Thread.ofPlatform().start(() -> {
				try {
					go.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
				for (int j = 0; j < 5_000; j++) {
					granted.add(throttle.tryAcquire(7));
				}
			}));
		}
		go.countDown();
		for (final Thread thread : threads) {
			thread.join();
		}
		assertEquals(10_000, granted.sum());
	}
}
