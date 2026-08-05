package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegrityManifestCompletionTest {

	@TempDir
	Path tempDir;

	@Test
	void publishesAndValidatesIdentityBoundRecord() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nb,\"k,1\",3,v1\n");
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
	void publicationCleanupFailureIsSuppressedBehindPrimaryFailure() throws Exception {
		final Path manifest = tempDir.resolve("completion-failure.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nb,k,1,\n");
		final var completion = IntegrityManifestCompletion.create(
						manifest,
						42,
						IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
						"create-step",
						1,
						1,
						1);
		final IOException primary = new IOException("publication");
		final IOException cleanup = new IOException("cleanup");
		final CrashDurableFilePublisher.Operations operations = new CrashDurableFilePublisher.Operations() {
			@Override
			public void syncFile(final Path path) {}

			@Override
			public void createLinkNoReplace(final Path source, final Path target)
							throws IOException {
				throw primary;
			}

			@Override
			public void delete(final Path path) {}

			@Override
			public void syncDirectory(final Path path) {}
		};

		final IOException thrown = assertThrows(
						IOException.class,
						() -> completion.publish(
										manifest,
										operations,
										() -> {
											throw cleanup;
										}));

		assertSame(primary, thrown);
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	@Test
	void rejectsChangedManifestAndWrongRun() throws Exception {
		final Path manifest = tempDir.resolve("verified.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nb,k,3,\n");
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
		Files.writeString(manifest, "bucket,key,size,version_id\nb,k,4,\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.validate(
										manifest, 7, IntegrityInputProvenance.ENGINE_STEP, "read-step"));
	}

	@Test
	void ignoresCompletionStagingFileAfterInterruptedPublication() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nb,k,3,\n");
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
		Files.writeString(manifest, "bucket,key,size,version_id\nb,k,1,\n");
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
	void requiresStrictUniqueCanonicalIdentityOrder() throws Exception {
		final Path outOfOrder = tempDir.resolve("out-of-order.csv");
		Files.writeString(
						outOfOrder,
						"bucket,key,size,version_id\nb,z,1,\nb,a,1,\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										outOfOrder, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", 2, 2, 2));

		final Path duplicate = tempDir.resolve("duplicate.csv");
		Files.writeString(
						duplicate,
						"bucket,key,size,version_id\nb,a,1,\nb,a,1,\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										duplicate, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", 2, 2, 2));
	}

	@Test
	void canonicalOrderUsesUnicodeCodePoints() {
		assertEquals(
						-1,
						Integer.signum(IntegrityManifestOrder.compareIdentity(
										"b", "\uE000", "", "b", "\uD800\uDC00", "")));
	}

	@Test
	void rejectsMalformedUtf8() throws Exception {
		final Path manifest = tempDir.resolve("malformed-utf8.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\nb,", StandardCharsets.UTF_8);
		Files.write(manifest, new byte[]{(byte) 0xff
		}, StandardOpenOption.APPEND);
		Files.writeString(manifest, ",1,\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);

		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										manifest, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", 1, 1, 1));
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
				"truncated.json",
				"unknown-field.json"
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

	@Test
	void sharedPhysicalCanonicalCorpusHasExpectedResults() throws Exception {
		final Path fixture = sharedIntegrityFixture("canonical-v1", "cases.json");
		final List<Map<String, Object>> cases = new ObjectMapper().readValue(
						fixture.toFile(), new TypeReference<>() {});
		for (final Map<String, Object> test : cases) {
			final String name = (String) test.get("name");
			final boolean expected = (Boolean) test.get("accept");
			final Path manifest = tempDir.resolve(name + ".csv");
			Files.write(manifest, Base64.getDecoder().decode((String) test.get("base64")));
			boolean accepted = true;
			try {
				IntegrityManifestValidator.validate(manifest);
			} catch (final IOException e) {
				accepted = false;
			}
			assertEquals(expected, accepted, name);
		}
	}

	private static Path sharedCompletionFixture(final String variant) {
		return sharedIntegrityFixture("completion-v1", variant);
	}

	private static Path sharedIntegrityFixture(final String... parts) {
		Path cursor = Path.of("").toAbsolutePath();
		while (cursor != null) {
			Path candidate = cursor.resolve(Path.of("testdata", "integrity"));
			for (final String part : parts) {
				candidate = candidate.resolve(part);
			}
			if (Files.exists(candidate)) {
				return candidate;
			}
			cursor = cursor.getParent();
		}
		throw new AssertionError(
						"shared integrity fixture not found: " + String.join("/", parts));
	}

	@Test
	void terminalExceptionDoesNotInheritRecoverableIllegalState() {
		org.junit.jupiter.api.Assertions.assertFalse(
						IllegalStateException.class.isAssignableFrom(IntegrityTerminalException.class));
	}
}
