package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StorageDriverBasePathCacheTest {

	@Test
	void pathInitializationCacheKeyDefaultsToTheCompletePath() {
		final StorageDriverBase<?, ?> driver = Mockito.mock(
						StorageDriverBase.class,
						Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));

		assertEquals("/bucket/parent", driver.requestNewPathCacheKey("/bucket/parent"));
	}
}
