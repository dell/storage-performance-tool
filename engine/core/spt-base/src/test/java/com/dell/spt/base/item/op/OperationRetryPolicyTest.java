package com.dell.spt.base.item.op;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OperationRetryPolicyTest {
	@Test
	void fullJitterIncludesBothEndpointsAndCapsWithoutIntegerOverflow() {
		int[] attempts = {Integer.MIN_VALUE, 0, 1, 2, 3, 4, 1000, Integer.MAX_VALUE
		};
		long[] upperBounds = {200, 200, 200, 400, 800, 1000, 1000, 1000
		};
		for (int i = 0; i < attempts.length; i++) {
			var observedBound = new AtomicLong();
			long delay = OperationRetryPolicy.backoffMillis(attempts[i], bound -> {
				observedBound.set(bound);
				return bound - 1;
			});
			assertEquals(upperBounds[i], delay);
			assertEquals(upperBounds[i] + 1, observedBound.get());
			assertEquals(0, OperationRetryPolicy.backoffMillis(attempts[i], bound -> 0));
		}
		assertThrows(IllegalArgumentException.class, () -> OperationRetryPolicy.backoffMillis(1, bound -> bound));
		assertThrows(IllegalArgumentException.class, () -> OperationRetryPolicy.backoffMillis(1, bound -> -1));
	}
}
