package com.dell.spt.base.buildinfo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.CloseableThreadContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EngineBuildInfoPublicationTest {

	private static final EngineBuildInfo BUILD_INFO = new EngineBuildInfo(
					1,
					"spt-engine",
					"5.14.2",
					"0123456789abcdef0123456789abcdef01234567",
					"2026-08-26T12:34:56Z",
					true,
					null);

	@TempDir
	Path tempDir;

	@Test
	void schemaOneJsonContainsOnlyTheSafeStableFields() throws Exception {
		final var json = EngineBuildInfoJson.serialize(BUILD_INFO);
		final var root = new ObjectMapper().readTree(json);

		assertEquals(
						Set.of(
										"schema_version",
										"product",
										"version",
										"revision",
										"build_time",
										"development",
										"source_dirty"),
						Set.copyOf(root.properties().stream().map(java.util.Map.Entry::getKey).toList()));
		assertEquals(1, root.get("schema_version").asInt());
		assertEquals("spt-engine", root.get("product").asText());
		assertEquals("5.14.2", root.get("version").asText());
		assertEquals("0123456789abcdef0123456789abcdef01234567", root.get("revision").asText());
		assertEquals("2026-08-26T12:34:56Z", root.get("build_time").asText());
		assertTrue(root.get("development").asBoolean());
		assertTrue(root.get("source_dirty").isNull());
	}

	@Test
	void publisherAtomicallyReplacesTheBuildRecordWithoutLeavingTemporaryFiles() throws Exception {
		final var warnings = new ArrayList<String>();
		final var publisher = new EngineBuildInfoPublisher(BUILD_INFO, warnings::add);
		final var stepDir = tempDir.resolve("log/workload-step");
		Files.createDirectories(stepDir);
		Files.writeString(stepDir.resolve(EngineBuildInfoPublisher.FILE_NAME), "stale", StandardCharsets.UTF_8);

		publisher.publish(tempDir, "workload-step");

		assertEquals(
						EngineBuildInfoJson.serialize(BUILD_INFO),
						Files.readString(stepDir.resolve(EngineBuildInfoPublisher.FILE_NAME), StandardCharsets.UTF_8));
		try (final var entries = Files.list(stepDir)) {
			assertEquals(
							Set.of(EngineBuildInfoPublisher.FILE_NAME),
							entries.map(path -> path.getFileName().toString()).collect(Collectors.toSet()));
		}
		assertTrue(warnings.isEmpty());
	}

	@Test
	void publisherWarnsOnceAndDoesNotFailWhenArtifactCannotBeWritten() throws Exception {
		final var warnings = new ArrayList<String>();
		final var publisher = new EngineBuildInfoPublisher(BUILD_INFO, warnings::add);
		Files.writeString(tempDir.resolve("log"), "not-a-directory", StandardCharsets.UTF_8);

		assertDoesNotThrow(() -> publisher.publish(tempDir, "first-step"));
		assertDoesNotThrow(() -> publisher.publish(tempDir, "second-step"));

		assertEquals(1, warnings.size());
		assertTrue(warnings.get(0).contains(EngineBuildInfoPublisher.FILE_NAME));
		assertTrue(warnings.get(0).contains("continuing"));
		assertFalse(Files.exists(tempDir.resolve("log/first-step/engine.build.json")));
	}

	@Test
	void publisherCleansTemporaryFileWhenFinalMoveFails() throws Exception {
		final var warnings = new ArrayList<String>();
		final var publisher = new EngineBuildInfoPublisher(BUILD_INFO, warnings::add);
		final var stepDir = tempDir.resolve("log/workload-step");
		Files.createDirectories(stepDir.resolve(EngineBuildInfoPublisher.FILE_NAME));

		assertDoesNotThrow(() -> publisher.publish(tempDir, "workload-step"));

		assertEquals(1, warnings.size());
		try (final var entries = Files.list(stepDir)) {
			assertEquals(
							Set.of(EngineBuildInfoPublisher.FILE_NAME),
							entries.map(path -> path.getFileName().toString()).collect(Collectors.toSet()));
		}
		assertTrue(Files.isDirectory(stepDir.resolve(EngineBuildInfoPublisher.FILE_NAME)));
	}

	@Test
	void missingProcessLogHomeIsAlsoNonfatal() {
		final var warnings = new ArrayList<String>();
		final var publisher = new EngineBuildInfoPublisher(BUILD_INFO, warnings::add);

		assertDoesNotThrow(() -> publisher.publish(null, "workload-step"));

		assertEquals(1, warnings.size());
	}

	@Test
	void workloadPublicationUsesTheProcessLogHome() throws Exception {
		final var publisher = new EngineBuildInfoPublisher(BUILD_INFO, warning -> {});

		try (final var ignored = CloseableThreadContext.put("home_dir", tempDir.toString())) {
			publisher.publishForStep("measured-step");
		}

		assertEquals(
						EngineBuildInfoJson.serialize(BUILD_INFO),
						Files.readString(tempDir.resolve("log/measured-step/engine.build.json"), StandardCharsets.UTF_8));
	}
}
