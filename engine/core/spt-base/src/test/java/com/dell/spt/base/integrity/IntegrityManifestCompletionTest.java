package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
	void rejectsInconsistentAndIndependentEmissionCounts() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,k,1,\r\n");
		assertThrows(
						IOException.class,
						() -> IntegrityManifestCompletion.create(
										manifest, 9, IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
										"list-step", 1, 0, 1, 1));
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
	void terminalExceptionDoesNotInheritRecoverableIllegalState() {
		org.junit.jupiter.api.Assertions.assertFalse(
						IllegalStateException.class.isAssignableFrom(IntegrityTerminalException.class));
	}
}
