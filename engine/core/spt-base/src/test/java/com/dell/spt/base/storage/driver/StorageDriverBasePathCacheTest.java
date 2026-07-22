package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
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

	@Test
	@SuppressWarnings("unchecked")
	void credentialCacheUsesTheSameNormalizedResourceIdentityAsPathInitialization()
					throws Exception {
		final StorageDriverBase<Item, Operation<Item>> driver = Mockito.mock(
						StorageDriverBase.class,
						Mockito.withSettings()
										.useConstructor(
														"test",
														null,
														TestConfigBuilder.config().configVal("storage"),
														false)
										.defaultAnswer(CALLS_REAL_METHODS));
		doReturn("/bucket").when(driver).requestNewPathCacheKey(any());
		doReturn("/bucket").when(driver).requestNewPath(any());
		doReturn(null).when(driver).requestNewAuthToken(any());
		final Credential credential = Credential.getInstance("user", "secret");

		for (final String path : new String[]{"/bucket/s0000001", "/bucket/s0000002"
		}) {
			final Operation<Item> op = Mockito.mock(Operation.class);
			doReturn(path).when(op).dstPath();
			doReturn(credential).when(op).credential();
			driver.prepare(op);
		}

		assertEquals(1, driver.pathToCredMap.size());
		assertEquals(credential, driver.pathToCredMap.get("/bucket"));
	}
}
