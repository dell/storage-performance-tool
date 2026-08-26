package com.dell.spt.gradle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineBuildMetadataTest {

	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-26T15:00:00Z"), ZoneOffset.UTC);

	@Test
	void explicitGradleInputsTakePrecedenceAndNormalizeBuildTimeToUtc() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2",
						Map.of(
								"sptBuildRevision", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
								"sptBuildTime", "2026-08-26T08:34:56-04:00",
								"sptBuildRelease", "true",
								"sptBuildSourceDirty", "false"),
						Map.of(
								"SPT_BUILD_REVISION", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
								"SPT_BUILD_TIME", "2025-01-01T00:00:00Z",
								"SPT_BUILD_RELEASE", "false",
								"SPT_BUILD_SOURCE_DIRTY", "true"),
						new FixedGit("cccccccccccccccccccccccccccccccccccccccc", true),
						CLOCK);

		assertEquals("5.14.2", metadata.version());
		assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", metadata.revision());
		assertEquals("2026-08-26T12:34:56Z", metadata.buildTime());
		assertFalse(metadata.development());
		assertEquals(false, metadata.sourceDirty());
	}

	@Test
	void releaseBuildRequiresRevisionBuildTimeAndDirtyStateEvidence() {
		assertThrows(
					IllegalArgumentException.class,
					() -> EngineBuildMetadata.resolve(
								"5.14.2",
								Map.of("sptBuildRelease", "true", "sptBuildTime", "2026-08-26T12:34:56Z", "sptBuildSourceDirty", "false"),
								Map.of(),
								new FixedGit(null, null),
								CLOCK));
		assertThrows(
					IllegalArgumentException.class,
					() -> EngineBuildMetadata.resolve(
								"5.14.2",
								Map.of("sptBuildRelease", "true", "sptBuildRevision", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "sptBuildSourceDirty", "false"),
								Map.of(),
								new FixedGit(null, null),
								CLOCK));
		assertThrows(
					IllegalArgumentException.class,
					() -> EngineBuildMetadata.resolve(
								"5.14.2",
								Map.of("sptBuildRelease", "true", "sptBuildRevision", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "sptBuildTime", "2026-08-26T12:34:56Z"),
								Map.of(),
								new FixedGit(null, null),
								CLOCK));
	}

	@Test
	void releaseBuildRejectsDirtySource() {
		assertThrows(
					IllegalArgumentException.class,
					() -> EngineBuildMetadata.resolve(
								"5.14.2",
								Map.of(
										"sptBuildRelease", "true",
										"sptBuildRevision", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
										"sptBuildTime", "2026-08-26T12:34:56Z",
										"sptBuildSourceDirty", "true"),
								Map.of(),
								new FixedGit(null, null),
								CLOCK));
	}

	@Test
	void buildRejectsAnAbbreviatedSourceRevision() {
		assertThrows(
					IllegalArgumentException.class,
					() -> EngineBuildMetadata.resolve(
								"5.14.2",
								Map.of("sptBuildRevision", "abc123"),
								Map.of(),
								new FixedGit(null, null),
								CLOCK));
	}

	@Test
	void sourceDateEpochProvidesAReproducibleUtcBuildTime() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2",
						Map.of(),
						Map.of("SOURCE_DATE_EPOCH", "1767225600"),
						new FixedGit("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false),
						CLOCK);

		assertEquals("2026-01-01T00:00:00Z", metadata.buildTime());
		assertTrue(metadata.development());
	}

	@Test
	void localGitEvidenceIsUsedForAnOrdinaryDevelopmentBuild() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2",
						Map.of(),
						Map.of(),
						new FixedGit("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true),
						CLOCK);

		assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", metadata.revision());
		assertEquals(true, metadata.sourceDirty());
		assertTrue(metadata.development());
	}

	@Test
	void sourceArchiveWithoutGitUsesExplicitUnknownDevelopmentValues() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2", Map.of(), Map.of(), new FixedGit(null, null), CLOCK);

		assertEquals("unknown", metadata.revision());
		assertEquals("2026-08-26T15:00:00Z", metadata.buildTime());
		assertTrue(metadata.development());
		assertNull(metadata.sourceDirty());
	}

	@Test
	void explicitUnknownDevelopmentEvidenceRemainsNullableAndDoesNotUseGitFallback() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2",
						Map.of(
								"sptBuildRevision", "unknown",
								"sptBuildTime", "unknown",
								"sptBuildSourceDirty", "unknown"),
						Map.of(),
						new FixedGit("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true),
						CLOCK);

		assertEquals("unknown", metadata.revision());
		assertEquals("unknown", metadata.buildTime());
		assertNull(metadata.sourceDirty());
		assertTrue(metadata.development());
	}

	@Test
	void canonicalResourceAndManifestUseTheSameNormalizedValues() {
		final var metadata = EngineBuildMetadata.resolve(
						"5.14.2", Map.of(), Map.of(), new FixedGit(null, null), CLOCK);

		assertEquals(
					Map.of(
							"schema_version", "1",
							"product", "spt-engine",
							"version", "5.14.2",
							"revision", "unknown",
							"build_time", "2026-08-26T15:00:00Z",
							"development", "true",
							"source_dirty", "unknown"),
					metadata.resourceValues());
		assertEquals(metadata.resourceValues().get("version"), metadata.manifestAttributes().get("Implementation-Version"));
		assertEquals(metadata.resourceValues().get("revision"), metadata.manifestAttributes().get("Spt-Source-Revision"));
		assertEquals(metadata.resourceValues().get("build_time"), metadata.manifestAttributes().get("Spt-Build-Time"));
		assertEquals(metadata.resourceValues().get("development"), metadata.manifestAttributes().get("Spt-Development"));
		assertEquals(metadata.resourceValues().get("source_dirty"), metadata.manifestAttributes().get("Spt-Source-Dirty"));
	}

	private record FixedGit(String revision, Boolean dirty) implements EngineBuildMetadata.GitProbe {
	}
}
