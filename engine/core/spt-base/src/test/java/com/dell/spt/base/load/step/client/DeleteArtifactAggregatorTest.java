package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.item.op.deletion.DeleteArtifactRecorder;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.storage.Credential;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteArtifactAggregatorTest {
	@TempDir
	Path temp;

	@Test
	void fetchesEachContributorOncePreservesSourcesAndPublishesCompletionLast() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final Path selection = selection();
		final Path selectionCompletion = completion();
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		write(remoteArtifacts);
		serve(remote, remoteArtifacts);

		new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"), selection, selectionCompletion)
						.close();

		assertEquals(2, Files.readAllLines(output.resolve(DeleteArtifacts.REQUESTS_FILE_NAME)).size() - 1);
		assertEquals(4, Files.readAllLines(output.resolve(DeleteArtifacts.OBJECTS_FILE_NAME)).size() - 1);
		assertEquals(2, Files.readAllLines(output.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME)).size() - 1);
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)).isBlank());
		for (final String artifact : List.of(
						DeleteArtifacts.METRICS_FILE_NAME,
						DeleteArtifacts.REQUESTS_FILE_NAME,
						DeleteArtifacts.OBJECTS_FILE_NAME,
						DeleteArtifacts.RESIDUAL_FILE_NAME)) {
			assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 0)).isBlank());
			assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 1)).isBlank());
		}
	}

	@Test
	void failedFetchKeepsCollectedSourcesAndNeverPublishesComplete() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), 0)).thenThrow(new EOFException());

		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"), selection(), completion());
		assertThrows(RuntimeException.class, aggregator::close);

		assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(
						output.resolve(DeleteArtifacts.METRICS_FILE_NAME), 0)).isBlank());
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.SELECTION_FILE_NAME)).isBlank());
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME)).isBlank());
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void partialRemoteFetchDoesNotPoisonRetry() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		write(remoteArtifacts);
		final byte[] totals = Files.readAllBytes(remoteArtifacts.totals());
		final byte[] prefix = java.util.Arrays.copyOf(totals, totals.length / 2);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), 0))
						.thenReturn(prefix)
						.thenReturn(totals);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), prefix.length))
						.thenThrow(new IOException("interrupted fetch"));
		when(remote.readFromFile(remoteArtifacts.totals().toString(), totals.length))
						.thenThrow(new EOFException());
		for (final Path path : List.of(
						remoteArtifacts.requests(), remoteArtifacts.objects(), remoteArtifacts.residual())) {
			final byte[] bytes = Files.readAllBytes(path);
			when(remote.readFromFile(path.toString(), 0)).thenReturn(bytes);
			when(remote.readFromFile(path.toString(), bytes.length)).thenThrow(new EOFException());
		}
		final Path selection = selection();
		final Path selectionCompletion = completion();

		assertThrows(RuntimeException.class, () -> new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"),
						selection, selectionCompletion).close());

		new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"),
						selection, selectionCompletion).close();
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)).isBlank());
	}

	@Test
	void realRecorderOutputAggregatesIntoAHashBoundCanonicalPublication() throws Exception {
		final String stepId = "delete-recorder-aggregate-" + System.nanoTime();
		final Path selection = temp.resolve("real-verify-input.csv");
		Files.writeString(selection, fixture(DeleteArtifacts.SELECTION_FILE_NAME));
		IntegrityManifestCompletion.create(
						selection, 777, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"spt-cli-items-stager-v1", 3, 2, 2).publish(selection);
		final Path selectionCompletion = IntegrityManifestCompletion.completionPath(selection);
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(stepId, selection, 2);
		final var targets = List.of(
						new DeleteTarget(new IntegrityManifestDataItem("bucket", "alpha", 9, null), 0),
						new DeleteTarget(new IntegrityManifestDataItem(
										"bucket", "comma,key", 7, "version-comma"), 1));
		final var operation = new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", Credential.NONE, targets));
		operation.completeDelete(DeleteTransportResult.success(targets));
		recorder.recordTerminal(operation);
		final var lifecycle = new OperationLifecycleSnapshot<>(
						0, 0, 1, 0, 1, 0, 0, List.of(), List.of());
		final var metrics = DeleteMetricsSnapshot.builder(2)
						.identity("batch", "canonical")
						.requests(1, 1, 0, 0, 0, 2)
						.objects(2, 2, 2, 0, 0, 0, 2)
						.batches(1, 2, 1, 0)
						.versions(1, 1)
						.reconciled(true)
						.build();
		try {
			recorder.finish(lifecycle, metrics);
			final Path output = temp.resolve("recorder-aggregate");
			DeleteArtifactAggregation.publish(
							stepId, output,
							List.of(new DeleteArtifactAggregation.NodeSource(
											path(Loggers.DELETE_METRICS_TOTAL, stepId),
											path(Loggers.DELETE_REQUESTS, stepId),
											path(Loggers.DELETE_OBJECTS, stepId),
											path(Loggers.DELETE_RESIDUAL, stepId))),
							List.of("local"), selection, selectionCompletion);

			for (final String artifact : List.of(
							DeleteArtifacts.METRICS_FILE_NAME,
							DeleteArtifacts.REQUESTS_FILE_NAME,
							DeleteArtifacts.OBJECTS_FILE_NAME,
							DeleteArtifacts.RESIDUAL_FILE_NAME,
							DeleteArtifacts.SELECTION_FILE_NAME)) {
				assertEquals(fixture(artifact), Files.readString(output.resolve(artifact)));
			}
			assertEquals(
							fixture(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME).strip(),
							Files.readString(output.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME)).strip());
			assertTrue(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME))
							.contains("\"status\" : \"complete\""));
		} finally {
			recorder.close();
			for (final var logger : List.of(
							Loggers.DELETE_METRICS_TOTAL, Loggers.DELETE_REQUESTS,
							Loggers.DELETE_OBJECTS, Loggers.DELETE_RESIDUAL)) {
				Files.deleteIfExists(path(logger, stepId));
			}
		}
	}

	private static String fixture(final String name) throws IOException {
		try (InputStream input = DeleteArtifactAggregatorTest.class.getResourceAsStream(
						"/delete-artifacts-v1/" + name)) {
			if (input == null) {
				throw new IOException("missing shared DELETE artifact fixture " + name);
			}
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	@Test
	void rejectsNoncanonicalSelectionIndexBeforePublishingCompletion() throws Exception {
		final NodeArtifacts first = nodeArtifacts(temp.resolve("first"), 0, "local");
		final NodeArtifacts second = nodeArtifacts(temp.resolve("second"), 1, "worker-a");
		write(first);
		write(second);
		Files.writeString(
						first.objects(),
						first.objectsText().replace(",0,a,a,", ",+0,a,a,"));
		final Path output = temp.resolve("invalid-index");

		assertThrows(RuntimeException.class, () -> DeleteArtifactAggregation.publish(
						"step", output,
						List.of(
										new DeleteArtifactAggregation.NodeSource(
														first.totals(), first.requests(), first.objects(), first.residual()),
										new DeleteArtifactAggregation.NodeSource(
														second.totals(), second.requests(), second.objects(), second.residual())),
						List.of("local", "worker-a"), selection(), completion()));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void rejectsImpossiblePartialBatchShapeBeforePublishingCompletion() throws Exception {
		final NodeArtifacts first = nodeArtifacts(temp.resolve("first"), 0, "local");
		final NodeArtifacts second = nodeArtifacts(temp.resolve("second"), 1, "worker-a");
		write(first);
		write(second);
		Files.writeString(first.totals(), first.totalsText().replace(",1,0,true\n", ",0,1,true\n"));
		final Path output = temp.resolve("invalid-batch-shape");

		assertThrows(RuntimeException.class, () -> DeleteArtifactAggregation.publish(
						"step", output,
						List.of(
										new DeleteArtifactAggregation.NodeSource(
														first.totals(), first.requests(), first.objects(), first.residual()),
										new DeleteArtifactAggregation.NodeSource(
														second.totals(), second.requests(), second.objects(), second.residual())),
						List.of("local", "worker-a"), selection(), completion()));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	private Path selection() throws Exception {
		final Path path = temp.resolve("verify-input.csv");
		Files.writeString(path, "bucket,key,size,version_id\na,a,1,\nb,b,1,\nc,c,1,\nd,d,1,\n");
		return path;
	}

	private Path completion() throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", 4, 4, 4).publish(selection);
		return IntegrityManifestCompletion.completionPath(selection);
	}

	private static void register(final FileManager manager, final NodeArtifacts artifacts) throws Exception {
		when(manager.logFileName(Loggers.DELETE_METRICS_TOTAL.getName(), "step")).thenReturn(artifacts.totals().toString());
		when(manager.logFileName(Loggers.DELETE_REQUESTS.getName(), "step")).thenReturn(artifacts.requests().toString());
		when(manager.logFileName(Loggers.DELETE_OBJECTS.getName(), "step")).thenReturn(artifacts.objects().toString());
		when(manager.logFileName(Loggers.DELETE_RESIDUAL.getName(), "step")).thenReturn(artifacts.residual().toString());
	}

	private static NodeArtifacts nodeArtifacts(final Path dir, final int index, final String node) {
		return new NodeArtifacts(
						dir.resolve(DeleteArtifacts.METRICS_FILE_NAME),
						dir.resolve(DeleteArtifacts.REQUESTS_FILE_NAME),
						dir.resolve(DeleteArtifacts.OBJECTS_FILE_NAME),
						dir.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME), index, node);
	}

	private static void write(final NodeArtifacts artifacts) throws Exception {
		Files.createDirectories(artifacts.totals().getParent());
		Files.writeString(artifacts.totals(), artifacts.totalsText());
		Files.writeString(artifacts.requests(), artifacts.requestsText());
		Files.writeString(artifacts.objects(), artifacts.objectsText());
		Files.writeString(artifacts.residual(), artifacts.residualText());
	}

	private static void serve(final FileManager manager, final NodeArtifacts artifacts) throws Exception {
		for (final Path path : List.of(artifacts.totals(), artifacts.requests(), artifacts.objects(), artifacts.residual())) {
			final byte[] bytes = Files.readAllBytes(path);
			when(manager.readFromFile(path.toString(), 0)).thenReturn(bytes);
			when(manager.readFromFile(path.toString(), bytes.length)).thenThrow(new EOFException());
		}
	}

	private static Path path(final org.apache.logging.log4j.Logger logger, final String stepId)
					throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private record NodeArtifacts(
			Path totals, Path requests, Path objects, Path residual, int index, String node) {
		String requestId() { return "request-" + index; }
		String acceptedId() { return "target-" + index + "-a"; }
		String failedId() { return "target-" + index + "-b"; }
		String firstKey() { return index == 0 ? "a" : "c"; }
		String secondKey() { return index == 0 ? "b" : "d"; }
		String totalsText() {
			return String.join(",", DeleteArtifacts.METRICS_HEADER) + "\n"
					+ "1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,0,1,0,0,2,2,1,1,0,0,1,2,1,0,true\n";
		}
		String requestsText() {
			return String.join(",", DeleteArtifacts.REQUESTS_HEADER) + "\n"
					+ "1," + requestId() + "," + requestId() + ",2,partial," + node + ",1,2,1\n";
		}
		String objectsText() {
			return String.join(",", DeleteArtifacts.OBJECTS_HEADER) + "\n"
					+ "1," + requestId() + "," + acceptedId() + "," + (index * 2) + "," + firstKey() + "," + firstKey() + ",1,,accepted,none,\n"
					+ "1," + requestId() + "," + failedId() + "," + (index * 2 + 1) + "," + secondKey() + "," + secondKey() + ",1,,failed,operational,error\n";
		}
		String residualText() {
			return "bucket,key,size,version_id\n" + secondKey() + "," + secondKey() + ",1,\n";
		}
	}
}
