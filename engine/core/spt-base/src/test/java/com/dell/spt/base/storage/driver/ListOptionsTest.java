package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ListOptionsTest {

	@Test
	void builderProducesImmutableCopy() {
		final var options = ListOptions.builder()
						.delimiter("/")
						.fetchMetadata(true)
						.includeVersions(true)
						.maxKeys(500)
						.continuationToken("token")
						.startAfter("last")
						.keyMarker("keyMarker")
						.versionIdMarker("vid")
						.build();

		final var copy = options.toBuilder().build();
		assertEquals(options, copy);
		assertNotSame(options, copy);
	}

	@Test
	void defaultInstanceMatchesBuilderDefaults() {
		assertEquals(ListOptions.builder().build(), ListOptions.DEFAULT);
	}
}
