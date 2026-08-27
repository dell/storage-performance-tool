package com.dell.spt.base.buildinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.junit.jupiter.api.Test;

class EngineBuildInfoProviderTest {

	private static final String COMPLETE_RESOURCE = """
					schema_version=1
					product=spt-engine
					version=5.14.2
					revision=0123456789abcdef0123456789abcdef01234567
					build_time=2026-08-26T12:34:56Z
					development=false
					source_dirty=false
					""";

	@Test
	void loadsTheCanonicalBuildInformationOnce() {
		final var opens = new AtomicInteger();
		final var warnings = new ArrayList<String>();
		final var provider = new EngineBuildInfoProvider(source(COMPLETE_RESOURCE, null, opens), warnings::add);

		final var first = provider.snapshot();
		final var second = provider.snapshot();

		assertSame(first, second);
		assertEquals(1, opens.get());
		assertEquals(1, first.schemaVersion());
		assertEquals("spt-engine", first.product());
		assertEquals("5.14.2", first.version());
		assertEquals("0123456789abcdef0123456789abcdef01234567", first.revision());
		assertEquals("2026-08-26T12:34:56Z", first.buildTime());
		assertFalse(first.development());
		assertEquals(false, first.sourceDirty());
		assertTrue(warnings.isEmpty());
	}

	@Test
	void missingResourceFallsBackToDevelopmentIdentityAndWarnsOnce() {
		final var opens = new AtomicInteger();
		final var warnings = new ArrayList<String>();
		final var provider = new EngineBuildInfoProvider(source(null, "5.14.2", opens), warnings::add);

		final var snapshot = provider.snapshot();
		provider.snapshot();

		assertEquals("spt-engine", snapshot.product());
		assertEquals("5.14.2", snapshot.version());
		assertEquals("unknown", snapshot.revision());
		assertEquals("unknown", snapshot.buildTime());
		assertTrue(snapshot.development());
		assertNull(snapshot.sourceDirty());
		assertEquals(1, warnings.size());
		assertTrue(warnings.getFirst().contains("build information resource"));
	}

	@Test
	void malformedResourceUsesTheSameSafeFallback() {
		for (final var malformedResource : new String[]{
				COMPLETE_RESOURCE.replace("development=false", "development=maybe"),
				COMPLETE_RESOURCE.replace(
								"revision=0123456789abcdef0123456789abcdef01234567", "revision=01234567"),
				COMPLETE_RESOURCE.replace("source_dirty=false", "source_dirty=true")
		}) {
			final var warnings = new ArrayList<String>();
			final var provider = new EngineBuildInfoProvider(
							source(malformedResource, null, new AtomicInteger()), warnings::add);

			final var snapshot = provider.snapshot();

			assertEquals("unknown", snapshot.version());
			assertEquals("unknown", snapshot.revision());
			assertTrue(snapshot.development());
			assertNull(snapshot.sourceDirty());
			assertEquals(1, warnings.size());
			assertTrue(warnings.getFirst().contains("malformed"));
		}
	}

	@Test
	void projectsTheImmutableVersionAndWarnsOnceForUserOverrides() {
		final var warnings = new ArrayList<String>();
		final var provider = new EngineBuildInfoProvider(
						source(COMPLETE_RESOURCE, null, new AtomicInteger()), warnings::add);
		final var config = new BasicConfig(
						"-", Map.of("run", Map.of("version", String.class)), Map.of("run", Map.of("version", "user-value")));

		provider.projectVersion(config, true);
		provider.projectVersion(config, true);

		assertEquals("5.14.2", config.stringVal("run-version"));
		assertEquals(1, warnings.size());
		assertTrue(warnings.getFirst().contains("run.version"));
	}

	@Test
	void generatedResourceVersionMatchesTheSharedSemanticVersionContract() throws Exception {
		final var mapper = new ObjectMapper();
		for (final var fixture : semanticVersionFixtures()) {
			final var warnings = new ArrayList<String>();
			final var resource = COMPLETE_RESOURCE.replace("version=5.14.2", "version=" + fixture.version());
			final var provider = new EngineBuildInfoProvider(
							source(resource, fixture.version(), new AtomicInteger()), warnings::add);
			final var publishedVersion = mapper.readTree(EngineBuildInfoJson.serialize(provider.snapshot()))
							.get("version")
							.asText();

			if (fixture.valid()) {
				assertEquals(fixture.version(), provider.snapshot().version(), fixture.name());
				assertEquals(fixture.version(), publishedVersion, fixture.name());
				assertTrue(warnings.isEmpty(), fixture.name());
			} else {
				assertEquals(EngineBuildInfoProvider.UNKNOWN, provider.snapshot().version(), fixture.name());
				assertEquals(EngineBuildInfoProvider.UNKNOWN, publishedVersion, fixture.name());
				assertEquals(1, warnings.size(), fixture.name());
			}
		}
	}

	@Test
	void developmentFallbackVersionMatchesTheSharedSemanticVersionContract() throws Exception {
		for (final var fixture : semanticVersionFixtures()) {
			final var provider = new EngineBuildInfoProvider(
							source(null, fixture.version(), new AtomicInteger()), warning -> {});
			assertEquals(
							fixture.valid() ? fixture.version() : EngineBuildInfoProvider.UNKNOWN,
							provider.snapshot().version(),
							fixture.name());
			assertTrue(provider.snapshot().development(), fixture.name());
			assertNull(provider.snapshot().sourceDirty(), fixture.name());
		}
	}

	private static List<SemanticVersionFixture> semanticVersionFixtures() throws Exception {
		final var path = Path.of(System.getProperty("spt.test.semver-fixtures"));
		return Files.readAllLines(path).stream()
						.filter(line -> !line.startsWith("#"))
						.map(line -> line.split("\\|", -1))
						.map(parts -> new SemanticVersionFixture(parts[0].equals("valid"), parts[1], parts[2]))
						.toList();
	}

	private static EngineBuildInfoSource source(
					final String resource, final String implementationVersion, final AtomicInteger opens) {
		return new EngineBuildInfoSource() {
			@Override
			public InputStream openBuildInfoResource() {
				opens.incrementAndGet();
				return resource == null
								? null
								: new ByteArrayInputStream(resource.getBytes(StandardCharsets.UTF_8));
			}

			@Override
			public String implementationVersion() {
				return implementationVersion;
			}
		};
	}

	private record SemanticVersionFixture(boolean valid, String name, String version) {}
}
