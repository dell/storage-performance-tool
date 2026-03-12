package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

/**
 * Tests for CoopStorageDriverBase concurrency management.
 * Verifies that the concurrency throttle / counters work correctly.
 */
@SuppressWarnings("unchecked")
class CoopStorageDriverBaseTest {

	@Test
	void scheduledAndCompletedCountersAreIndependent() throws Exception {
		final var scheduledField = CoopStorageDriverBase.class.getDeclaredField("scheduledOpCount");
		scheduledField.setAccessible(true);
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);

		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		scheduledField.set(driver, new LongAdder());
		completedField.set(driver, new LongAdder());

		assertEquals(0, driver.scheduledOpCount());
		assertEquals(0, driver.completedOpCount());
	}

	@Test
	void activeOpCountReflectsSemaphoreState() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertEquals(0, driver.activeOpCount(), "all permits free = 0 active");

		sem.acquire(2);
		assertEquals(2, driver.activeOpCount(), "2 permits taken = 2 active");

		sem.release(2);
		assertEquals(0, driver.activeOpCount(), "all permits released = 0 active");
	}

	@Test
	void isIdleWhenAllPermitsFree() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertTrue(driver.isIdle(), "should be idle when all permits free");

		sem.acquire(1);
		assertFalse(driver.isIdle(), "should not be idle with permit held");

		sem.release(1);
		assertTrue(driver.isIdle(), "should be idle after release");
	}
}
