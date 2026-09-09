package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadAttemptTest {
	private static RangeReadAttempt attempt() {
		return new RangeReadAttempt(new OperationLifecycle(), new ByteRange(3, 2));
	}

	private static void headers(RangeReadAttempt a) {
		assertTrue(a.headers(206, Status.SUCC, List.of("bytes 3-4/10"), List.of("2"), List.of(), false));
	}

	@Test
	void successRequiresFramingAndRejectsLateCallbacks() {
		var a = attempt();
		assertTrue(a.requestHandoff());
		assertFalse(a.requestHandoff());
		headers(a);
		assertTrue(a.bodyBytes(2));
		assertNull(a.outcome());
		assertTrue(a.finish(true));
		var result = a.outcome();
		assertEquals(RangeReadAttempt.Category.SUCCESS, result.category());
		assertEquals(2, result.receivedBytes());
		assertFalse(a.transportFailure(Status.FAIL_TIMEOUT));
		assertFalse(a.unresolved());
		assertFalse(a.bodyBytes(100));
		assertFalse(a.finish(true));
		assertSame(result, a.outcome());
	}

	@Test
	void retryRetainsSelectionWithIndependentAttemptState() {
		var first = attempt();
		first.requestHandoff();
		headers(first);
		first.bodyBytes(1);
		assertTrue(first.transportFailure(Status.FAIL_IO));
		var retained = first.outcome();
		var retry = new RangeReadAttempt(first.lifecycle(), first.range());
		assertNotSame(first, retry);
		assertSame(first.lifecycle(), retry.lifecycle());
		retry.requestHandoff();
		headers(retry);
		assertFalse(first.bodyBytes(1));
		assertFalse(first.finish(true));
		retry.bodyBytes(2);
		retry.finish(true);
		assertSame(retained, first.outcome());
		assertEquals(1, retained.receivedBytes());
		assertEquals(RangeReadAttempt.Category.SUCCESS, retry.outcome().category());
	}

	@Test
	void httpTimeoutMappingRetainsHttpCategory() {
		var a = attempt();
		a.requestHandoff();
		assertFalse(a.headers(504, Status.FAIL_TIMEOUT, List.of(), List.of(), List.of(), false));
		assertEquals(RangeReadAttempt.Category.HTTP, a.outcome().category());
		assertEquals(Status.FAIL_TIMEOUT, a.outcome().status());
		assertEquals(504, a.outcome().httpStatus());
		assertFalse(a.bodyBytes(999));
		assertEquals(0, a.outcome().receivedBytes());
	}

	@Test
	void oversizedAndSubmissionFailuresRemainDistinct() {
		var a = attempt();
		a.requestHandoff();
		headers(a);
		assertFalse(a.bodyBytes(100));
		assertEquals(RangeReadAttempt.Category.VALIDATION, a.outcome().category());
		assertEquals(100, a.outcome().receivedBytes());
		assertEquals(RangeResponseValidator.Failure.OVERSIZED_BODY, a.outcome().validationFailure());
		var rejected = attempt();
		assertFalse(rejected.unresolved());
		assertThrows(IllegalStateException.class, () -> rejected.bodyBytes(1));
		assertTrue(rejected.transportFailure(Status.FAIL_IO));
		assertFalse(rejected.outcome().requestHandedOff());
		assertFalse(rejected.requestHandoff());
	}

	@Test
	void drainCompletionRaceRetainsOneOutcome() throws Exception {
		try (var pool = Executors.newFixedThreadPool(2)) {
			for (int i = 0; i < 100; i++) {
				var a = attempt();
				a.requestHandoff();
				headers(a);
				a.bodyBytes(2);
				var start = new CountDownLatch(1);
				var finish = pool.submit(() -> {
					start.await();
					return a.finish(true);
				});
				var drain = pool.submit(() -> {
					start.await();
					return a.unresolved();
				});
				start.countDown();
				assertNotEquals(finish.get(5, TimeUnit.SECONDS), drain.get(5, TimeUnit.SECONDS));
				assertNotNull(a.outcome());
				assertEquals(2, a.outcome().receivedBytes());
				assertFalse(a.transportFailure(Status.FAIL_IO));
			}
		}
	}
}
