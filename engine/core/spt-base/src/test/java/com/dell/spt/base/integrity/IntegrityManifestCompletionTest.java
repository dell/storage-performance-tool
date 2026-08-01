package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegrityManifestCompletionTest {

	@TempDir
	Path tempDir;

	@Test
	void publishesAndValidatesIdentityBoundRecord() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,\"k,1\",3,v1\r\n");
		final var completion = IntegrityManifestCompletion.create(
						manifest,
						42,
						IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
						"create-step",
						1,
						1,
						1);
		completion.publish(manifest);

		final var validated = IntegrityManifestCompletion.validate(
						manifest, 42, IntegrityInputProvenance.ENGINE_STEP, "create-step");
		assertEquals(1, validated.selectedRecordCount());
		assertEquals(Files.size(manifest), validated.manifestBytes());
	}

	@Test
	void rejectsChangedManifestAndWrongRun() throws Exception {
		final Path manifest = tempDir.resolve("verified.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,k,3,\r\n");
		IntegrityManifestCompletion.create(
						manifest,
						7,
						IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
						"read-step",
						1,
						1,
						1)
						.publish(manifest);

		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.validate(
										manifest, 8, IntegrityInputProvenance.ENGINE_STEP, "read-step"));
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,k,4,\r\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.validate(
										manifest, 7, IntegrityInputProvenance.ENGINE_STEP, "read-step"));
	}

	@Test
	void ignoresCompletionStagingFileAfterInterruptedPublication() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,k,3,\r\n");
		final Path marker = IntegrityManifestCompletion.completionPath(manifest);
		Files.writeString(
						marker.resolveSibling("." + marker.getFileName() + ".staging"),
						"{\"status\":\"complete\"}");

		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.validate(
										manifest, 7, IntegrityInputProvenance.ENGINE_STEP, "create-step"));
	}

	@Test
	void rejectsInconsistentPublicCounts() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,k,1,\r\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										manifest, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", 1, 1, 2));
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										manifest, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", -1, 0, 0));
	}

	@Test
	void validatesSharedPublicV1Fixtures() throws Exception {
		for (final String variant : new String[]{"nonempty", "empty"
		}) {
			final Path fixture = sharedCompletionFixture(variant);
			final Path manifest = fixture.resolve("verify-input.csv");
			final Path marker = fixture.resolve("verify-input.complete.json");
			assertFalse(Files.readString(marker).contains("emitted_record_count"));
			final var validated = IntegrityManifestCompletion.validate(
							manifest,
							1722369600000L,
							IntegrityInputProvenance.ENGINE_STEP,
							"mt-001-20260730.120000.000-list");
			assertEquals("empty".equals(variant) ? 0 : 1, validated.selectedRecordCount());
		}
	}

	@Test
	void requiresExactlyOneJsonDocument() throws Exception {
		final Path fixtureRoot = sharedCompletionFixture("nonempty").getParent();
		final Path sourceManifest = fixtureRoot.resolve("nonempty").resolve("verify-input.csv");
		for (final String file : new String[]{
				"trailing-garbage.json",
				"concatenated-object.json",
				"truncated.json"
		}) {
			final Path caseDir = Files.createDirectory(tempDir.resolve(file));
			final Path manifest = caseDir.resolve("verify-input.csv");
			Files.copy(sourceManifest, manifest);
			Files.copy(
							fixtureRoot.resolve("markers").resolve(file),
							IntegrityManifestCompletion.completionPath(manifest));
			assertThrows(
							IOException.class,
							() -> IntegrityManifestCompletion.validate(
											manifest,
											1722369600000L,
											IntegrityInputProvenance.ENGINE_STEP,
											"mt-001-20260730.120000.000-list"));
		}

		final Path validDir = Files.createDirectory(tempDir.resolve("valid-whitespace"));
		final Path validManifest = validDir.resolve("verify-input.csv");
		Files.copy(sourceManifest, validManifest);
		Files.copy(
						fixtureRoot.resolve("markers").resolve("valid-whitespace.json"),
						IntegrityManifestCompletion.completionPath(validManifest));
		IntegrityManifestCompletion.validate(
						validManifest,
						1722369600000L,
						IntegrityInputProvenance.ENGINE_STEP,
						"mt-001-20260730.120000.000-list");
	}

	private static Path sharedCompletionFixture(final String variant) {
		Path cursor = Path.of("").toAbsolutePath();
		while (cursor != null) {
			final Path candidate = cursor.resolve(
							Path.of("testdata", "integrity", "completion-v1", variant));
			if (Files.isDirectory(candidate)) {
				return candidate;
			}
			cursor = cursor.getParent();
		}
		throw new AssertionError("shared completion fixture not found: " + variant);
	}

	@Test
	void terminalExceptionDoesNotInheritRecoverableIllegalState() {
		org.junit.jupiter.api.Assertions.assertFalse(
						IllegalStateException.class.isAssignableFrom(IntegrityTerminalException.class));
	}
}
