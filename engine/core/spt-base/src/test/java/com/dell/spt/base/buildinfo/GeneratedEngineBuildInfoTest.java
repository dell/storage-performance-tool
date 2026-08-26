package com.dell.spt.base.buildinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class GeneratedEngineBuildInfoTest {

	@Test
	void generatedResourceLoadsAsImmutableDevelopmentIdentity() {
		final var resource = getClass().getClassLoader().getResource(EngineBuildInfoProvider.RESOURCE_PATH);
		assertNotNull(resource);

		final var buildInfo = EngineBuildInfoProvider.global().snapshot();
		assertEquals("spt-engine", buildInfo.product());
		assertEquals(System.getProperty("spt.test.expected-build-version"), buildInfo.version());
		assertEquals(System.getProperty("spt.test.expected-build-revision"), buildInfo.revision());
		assertEquals(System.getProperty("spt.test.expected-build-time"), buildInfo.buildTime());
		if (!buildInfo.buildTime().equals("unknown")) {
			Instant.parse(buildInfo.buildTime());
		}
		assertEquals(
						Boolean.parseBoolean(System.getProperty("spt.test.expected-build-development")),
						buildInfo.development());
		final var expectedSourceDirty = System.getProperty("spt.test.expected-build-source-dirty");
		if (expectedSourceDirty.equals("unknown")) {
			assertNull(buildInfo.sourceDirty());
		} else {
			assertEquals(Boolean.valueOf(expectedSourceDirty), buildInfo.sourceDirty());
		}
	}
}
