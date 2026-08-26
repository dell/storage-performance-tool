package com.dell.spt.base.buildinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class EngineBuildInfoRendererTest {

	@Test
	void startupLineIsConciseAndIncludesApplicableDevelopmentAndDirtyState() {
		final var buildInfo = buildInfo(true, true);

		final var line = EngineBuildInfoRenderer.startupLine(buildInfo);

		assertEquals("Engine build: 5.14.2 (0123456789ab, development, dirty)", line);
		assertFalse(line.contains(buildInfo.buildTime()));
	}

	@Test
	void cleanReleaseStartupLineDoesNotClaimDevelopmentOrDirtyState() {
		final var line = EngineBuildInfoRenderer.startupLine(buildInfo(false, false));

		assertEquals("Engine build: 5.14.2 (0123456789ab)", line);
	}

	@Test
	void versionDetailsRenderEveryFieldFromTheSnapshot() {
		final var buildInfo = new EngineBuildInfo(
						1,
						"spt-engine",
						"5.14.2",
						"unknown",
						"unknown",
						true,
						null);

		assertEquals(
						List.of(
										"Engine build schema: 1",
										"Engine product: spt-engine",
										"Engine version: 5.14.2",
										"Engine revision: unknown",
										"Engine build time: unknown",
										"Engine development: true",
										"Engine source dirty: unknown"),
						EngineBuildInfoRenderer.versionDetails(buildInfo));
		assertTrue(EngineBuildInfoRenderer.startupLine(buildInfo).contains("unknown"));
	}

	private static EngineBuildInfo buildInfo(final boolean development, final boolean sourceDirty) {
		return new EngineBuildInfo(
						1,
						"spt-engine",
						"5.14.2",
						"0123456789abcdef0123456789abcdef01234567",
						"2026-08-26T12:34:56Z",
						development,
						sourceDirty);
	}
}
