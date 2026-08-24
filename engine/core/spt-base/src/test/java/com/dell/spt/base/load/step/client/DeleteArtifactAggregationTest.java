package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteArtifactAggregationTest {
	@TempDir
	Path temp;

	@Test
	void aggregatesEveryRequestAndTargetOnceThenPublishesCompletionLast() throws Exception {
		final Path output = temp.resolve("out");
		Files.createDirectories(output);
		final var node0 = node("node0", "r0", "t0", "t1", "accepted", "accepted");
		final var node1 = node("node1", "r1", "t2", "t3", "accepted", "failed");
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\nb,c,1,\nb,d,1,\n");
		final Path selectionCompletion = temp.resolve("verify-input.complete.json");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);

		DeleteArtifactAggregation.publish(
						"delete-step", output, List.of(node0, node1), List.of("local", "worker:1099"),
						selection, selectionCompletion);

		assertEquals(3, Files.readAllLines(output.resolve(DeleteArtifacts.REQUESTS_FILE_NAME)).size());
		assertEquals(5, Files.readAllLines(output.resolve(DeleteArtifacts.OBJECTS_FILE_NAME)).size());
		assertEquals(2, Files.readAllLines(output.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME)).size());
		assertTrue(Files.readString(output.resolve(DeleteArtifacts.REQUESTS_FILE_NAME)).contains("worker:1099"));
		assertTrue(Files.isRegularFile(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
		assertTrue(Files.isRegularFile(output.resolve(DeleteArtifacts.SELECTION_FILE_NAME)));
		assertTrue(Files.isRegularFile(output.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME)));
	}

	@Test
	void duplicateOrIncompleteEvidenceFailsClosedWithoutCompletion() throws Exception {
		final Path output = temp.resolve("failed");
		Files.createDirectories(output);
		final var node0 = node("dup0", "same", "target", "target-2", "accepted", "accepted");
		final var node1 = node("dup1", "same", "target-3", "target-4", "accepted", "accepted");
		final Path selection = temp.resolve("selection.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\nb,c,1,\nb,d,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);
		final Path marker = IntegrityManifestCompletion.completionPath(selection);

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", output, List.of(node0, node1), List.of("local", "worker"),
										selection, marker));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void malformedTraceOrDuplicateContributorIdentityFailsClosed() throws Exception {
		final var node0 = node("malformed0", "r0", "t0", "t1", "accepted", "accepted");
		final var node1 = node("malformed1", "r1", "t2", "t3", "accepted", "accepted");
		Files.writeString(
						node0.requests(),
						Files.readString(node0.requests()).replace(",local,1,2,1", ",local,-1,2,1"));
		final Path selection = temp.resolve("malformed-selection.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\nb,c,1,\nb,d,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);
		final Path marker = IntegrityManifestCompletion.completionPath(selection);

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", temp.resolve("malformed-output"), List.of(node0, node1),
										List.of("local", "worker"), selection, marker));
		Files.writeString(
						node0.requests(),
						Files.readString(node0.requests()).replace(",local,-1,2,1", ",local,1,2,1"));
		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", temp.resolve("duplicate-contributor-output"), List.of(node0, node1),
										List.of("local", "local"), selection, marker));
	}

	@Test
	void balancedContradictoryRequestAndTargetOutcomesNeverPublishCompletion() throws Exception {
		final var successful = node("conflict0", "request-0", "target-0", "target-1", "accepted", "accepted");
		final var failed = node("conflict1", "request-1", "target-2", "target-3", "failed", "failed");
		Files.writeString(
						successful.objects(),
						Files.readString(successful.objects()).replace(",request-0,", ",request-1,"));
		Files.writeString(
						failed.objects(),
						Files.readString(failed.objects()).replace(",request-1,", ",request-0,"));
		final Path selection = temp.resolve("contradictory-selection.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\nb,c,1,\nb,d,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);
		final Path output = temp.resolve("contradictory-output");

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", output, List.of(successful, failed), List.of("local", "worker"),
										selection, IntegrityManifestCompletion.completionPath(selection)));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void incompatibleTargetFailureClassificationNeverPublishesCompletion() throws Exception {
		final var source = node("classification0", "request-0", "target-0", "target-1", "accepted", "accepted");
		Files.writeString(
						source.objects(),
						Files.readString(source.objects()).replaceFirst(",accepted,none,", ",accepted,operational,"));
		final Path selection = temp.resolve("classification-selection.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 2, 2, 2).publish(selection);
		final Path output = temp.resolve("classification-output");

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", output, List.of(source), List.of("local"), selection,
										IntegrityManifestCompletion.completionPath(selection)));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void protocolFailureClassificationMustMatchTheWholeFailedRequest() throws Exception {
		final var partial = node("protocol0", "request-0", "target-0", "target-1", "accepted", "failed");
		Files.writeString(
						partial.objects(),
						Files.readString(partial.objects()).replaceFirst(",failed,operational,", ",failed,protocol,"));
		final Path partialSelection = temp.resolve("protocol-partial-selection.csv");
		Files.writeString(partialSelection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n");
		IntegrityManifestCompletion.create(
						partialSelection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 2, 2, 2).publish(partialSelection);

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", temp.resolve("protocol-partial-output"), List.of(partial), List.of("local"),
										partialSelection, IntegrityManifestCompletion.completionPath(partialSelection)));

		final var mixed = node("protocol1", "request-1", "target-2", "target-3", "failed", "failed");
		Files.writeString(
						mixed.objects(),
						Files.readString(mixed.objects()).replaceFirst(",failed,operational,", ",failed,protocol,"));
		final Path mixedSelection = temp.resolve("protocol-mixed-selection.csv");
		Files.writeString(mixedSelection, "bucket,key,size,version_id\nb,c,1,\nb,d,1,\n");
		IntegrityManifestCompletion.create(
						mixedSelection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 2, 2, 2).publish(mixedSelection);

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", temp.resolve("protocol-mixed-output"), List.of(mixed), List.of("local"),
										mixedSelection, IntegrityManifestCompletion.completionPath(mixedSelection)));
	}

	@Test
	void corruptFrozenSelectionProvenanceNeverPublishesCompletion() throws Exception {
		final var source = node("provenance0", "r0", "t0", "t1", "accepted", "accepted");
		final Path selection = temp.resolve("corrupt-selection.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nb,a,1,\nb,b,1,\n");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 2, 2, 2).publish(selection);
		final Path marker = IntegrityManifestCompletion.completionPath(selection);
		Files.writeString(marker, "{}\n");
		final Path output = temp.resolve("corrupt-provenance-output");

		assertThrows(
						IntegrityTerminalException.class,
						() -> DeleteArtifactAggregation.publish(
										"delete-step", output, List.of(source), List.of("local"), selection, marker));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void residualUsesCrossLanguageCanonicalUnicodeOrder() throws Exception {
		final String lowerCodePoint = "\uE000";
		final String higherCodePoint = "\uD800\uDC00";
		final Path selection = temp.resolve("selection-unicode.csv");
		Files.writeString(selection,
						"bucket,key,size,version_id\n"
										+ "b," + lowerCodePoint + ",1,\n"
										+ "b," + higherCodePoint + ",1,\n");
		IntegrityManifestCompletion.create(
						selection, 9, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 2, 2, 2).publish(selection);
		final Path selectionCompletion = IntegrityManifestCompletion.completionPath(selection);
		final Path node = temp.resolve("unicode-node");
		Files.createDirectories(node);
		final Path totals = node.resolve(DeleteArtifacts.METRICS_FILE_NAME);
		Files.writeString(totals, String.join(",", DeleteArtifacts.METRICS_HEADER) + "\n"
						+ "1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,0,0,1,0,2,2,0,2,0,0,1,2,1,0,true\n");
		final Path requests = node.resolve(DeleteArtifacts.REQUESTS_FILE_NAME);
		Files.writeString(requests, String.join(",", DeleteArtifacts.REQUESTS_HEADER) + "\n"
						+ "1,unicode-request,unicode-batch,2,failed,local,1,2,1\n");
		final Path objects = node.resolve(DeleteArtifacts.OBJECTS_FILE_NAME);
		Files.writeString(objects, String.join(",", DeleteArtifacts.OBJECTS_HEADER) + "\n"
						+ "1,unicode-request,unicode-target-0,0,b," + lowerCodePoint
						+ ",1,,failed,operational,error\n"
						+ "1,unicode-request,unicode-target-1,1,b," + higherCodePoint
						+ ",1,,failed,operational,error\n");
		final Path residual = node.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME);
		Files.writeString(residual, "bucket,key,size,version_id\n"
						+ "b," + lowerCodePoint + ",1,\n"
						+ "b," + higherCodePoint + ",1,\n");
		final DeleteArtifactAggregation.NodeSource source = new DeleteArtifactAggregation.NodeSource(totals, requests, objects, residual);

		DeleteArtifactAggregation.publish(
						"delete-step", temp.resolve("unicode-output"), List.of(source), List.of("local"),
						selection, selectionCompletion);

		final List<String> rows = Files.readAllLines(
						temp.resolve("unicode-output").resolve(DeleteArtifacts.RESIDUAL_FILE_NAME));
		assertEquals("b," + lowerCodePoint + ",1,", rows.get(1));
		assertEquals("b," + higherCodePoint + ",1,", rows.get(2));
	}

	private DeleteArtifactAggregation.NodeSource node(
					String name, String request, String firstTarget, String secondTarget,
					String firstOutcome, String secondOutcome) throws Exception {
		final Path dir = temp.resolve(name);
		Files.createDirectories(dir);
		final int accepted = (firstOutcome.equals("accepted") ? 1 : 0)
						+ (secondOutcome.equals("accepted") ? 1 : 0);
		final int failed = (firstOutcome.equals("failed") ? 1 : 0)
						+ (secondOutcome.equals("failed") ? 1 : 0);
		final int full = accepted == 2 ? 1 : 0;
		final int partial = accepted > 0 && failed > 0 ? 1 : 0;
		final int requestFailed = accepted == 0 ? 1 : 0;
		final String requestOutcome = full == 1 ? "full_success" : partial == 1 ? "partial" : "failed";
		final Path totals = dir.resolve(DeleteArtifacts.METRICS_FILE_NAME);
		Files.writeString(totals, String.join(",", DeleteArtifacts.METRICS_HEADER) + "\n" +
						"1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1," +
						full + "," + partial + "," + requestFailed + ",0,2,2," + accepted + "," + failed +
						",0,0,1,2,1,0,true\n");
		final Path requests = dir.resolve(DeleteArtifacts.REQUESTS_FILE_NAME);
		Files.writeString(requests, String.join(",", DeleteArtifacts.REQUESTS_HEADER) + "\n" +
						"1," + request + "," + request + ",2," + requestOutcome + ",local,1,2,1\n");
		final String firstKey = name.endsWith("1") ? "c" : "a";
		final String secondKey = name.endsWith("1") ? "d" : "b";
		final Path objects = dir.resolve(DeleteArtifacts.OBJECTS_FILE_NAME);
		Files.writeString(objects, String.join(",", DeleteArtifacts.OBJECTS_HEADER) + "\n" +
						"1," + request + "," + firstTarget + ",0,b," + firstKey + ",1,," + firstOutcome + ","
						+ failureClassification(firstOutcome) + ",\n" +
						"1," + request + "," + secondTarget + ",1,b," + secondKey + ",1,," + secondOutcome + ","
						+ failureClassification(secondOutcome) + ",\n");
		final Path residual = dir.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME);
		Files.writeString(residual, "bucket,key,size,version_id\n" +
						(firstOutcome.equals("failed") ? "b," + firstKey + ",1,\n" : "") +
						(secondOutcome.equals("failed") ? "b," + secondKey + ",1,\n" : ""));
		return new DeleteArtifactAggregation.NodeSource(totals, requests, objects, residual);
	}

	private static String failureClassification(final String outcome) {
		return "failed".equals(outcome) ? "operational" : "none";
	}
}
