package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_BATCH;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_SINGLE;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_OBJECT_UNIT;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_REQUEST_UNIT;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;

import com.dell.spt.base.integrity.FailurePreservingCleanup;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityManifestOrder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/** Validates and atomically publishes exact multi-node DELETE evidence with fixed working memory. */
final class DeleteArtifactAggregation {
	record NodeSource(Path totals, Path requests, Path objects, Path residual, Path verification) {
		NodeSource(final Path totals, final Path requests, final Path objects, final Path residual) {
			this(totals, requests, objects, residual, null);
		}
	}

	private static final class CsvCursor implements AutoCloseable {
		private final CSVParser parser;
		private final Iterator<CSVRecord> records;
		private List<String> current;

		private CsvCursor(final Path path, final List<String> expectedHeader) throws IOException {
			parser = CSVFormat.RFC4180.parse(Files.newBufferedReader(path, StandardCharsets.UTF_8));
			records = parser.iterator();
			try {
				if (!records.hasNext() || !expectedHeader.equals(records.next().toList())) {
					throw new IOException("DELETE artifact has a noncanonical header: " + path);
				}
			} catch (final RuntimeException | IOException e) {
				try {
					parser.close();
				} catch (final IOException closeFailure) {
					e.addSuppressed(closeFailure);
				}
				throw e;
			}
		}

		private boolean advance() throws IOException {
			if (!records.hasNext()) {
				current = null;
				return false;
			}
			current = records.next().toList();
			return true;
		}

		@Override
		public void close() throws IOException {
			parser.close();
		}
	}

	private record ValidatedNode(Path normalizedRequests) {}

	private static final int TOTAL_FIRST_COUNTER = 7;
	private static final int TOTAL_LAST_COUNTER = 21;
	private static final int REQUEST_OUTCOME_INDEX = 4;
	private static final int OBJECT_OUTCOME_INDEX = 8;
	private static final ObjectMapper JSON = new ObjectMapper()
					.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
	private static final Comparator<List<String>> REQUEST_ORDER = Comparator.comparing(row -> row.get(1));
	private static final Comparator<List<String>> TARGET_ID_ORDER = Comparator.comparing(row -> row.get(2));
	private static final Comparator<List<String>> OBJECT_REQUEST_ORDER = Comparator
					.comparing((List<String> row) -> row.get(1))
					.thenComparing(row -> row.get(2));
	private static final Comparator<List<String>> OBJECT_MANIFEST_ORDER = (left, right) -> compareManifest(
					left, 4, 5, 6, 7, right, 4, 5, 6, 7);
	private static final Comparator<List<String>> VERIFICATION_TARGET_ORDER = Comparator.comparing(row -> row.get(1));
	private static final Comparator<List<String>> VERIFICATION_MANIFEST_ORDER = (left, right) -> compareManifest(
					left, 3, 4, 5, 6, right, 3, 4, 5, 6);
	private static final Comparator<List<String>> MANIFEST_ORDER = (left, right) -> compareManifest(
					left, 0, 1, 2, 3, right, 0, 1, 2, 3);

	private DeleteArtifactAggregation() {}

	static void retainSelection(
					final String stepId,
					final Path outputDirectory,
					final Path selection,
					final Path selectionCompletion) {
		try {
			Files.createDirectories(outputDirectory);
			final IntegrityManifestCompletion provenance = validateSelectionProvenance(
							selection, selectionCompletion);
			final Path stagedSelection = Files.createTempFile(
							outputDirectory, "." + DeleteArtifacts.SELECTION_FILE_NAME + ".", ".staging");
			final Path stagedCompletion = Files.createTempFile(
							outputDirectory, "." + DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME + ".", ".staging");
			FailurePreservingCleanup.always(() -> {
				Files.copy(selection, stagedSelection, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				JSON.writerWithDefaultPrettyPrinter().writeValue(
								stagedCompletion.toFile(), retainedSelectionProvenance(provenance, selection));
				publishRetrySafe(
								stagedSelection, outputDirectory.resolve(DeleteArtifacts.SELECTION_FILE_NAME));
				publishRetrySafe(
								stagedCompletion,
								outputDirectory.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME));
				return null;
			}, () -> {
				Files.deleteIfExists(stagedSelection);
				Files.deleteIfExists(stagedCompletion);
			});
		} catch (final Exception e) {
			throw terminal(stepId, "failed to retain DELETE frozen selection evidence", e);
		}
	}

	static void publish(
					final String stepId,
					final Path outputDirectory,
					final List<NodeSource> sources,
					final List<String> nodeIds,
					final Path selection,
					final Path selectionCompletion) {
		try {
			publishChecked(stepId, outputDirectory, sources, nodeIds, selection, selectionCompletion);
		} catch (final IntegrityTerminalException e) {
			throw e;
		} catch (final Exception e) {
			throw terminal(stepId, "failed to aggregate DELETE artifacts", e);
		}
	}

	private static void publishChecked(
					final String stepId,
					final Path outputDirectory,
					final List<NodeSource> sources,
					final List<String> nodeIds,
					final Path selection,
					final Path selectionCompletion)
					throws IOException {
		if (sources.isEmpty() || sources.size() != nodeIds.size()) {
			throw terminal(stepId, "DELETE artifact contributor identity is incomplete", null);
		}
		final Set<String> uniqueNodeIds = new HashSet<>(nodeIds);
		if (uniqueNodeIds.size() != nodeIds.size() || nodeIds.stream().anyMatch(String::isBlank)) {
			throw terminal(stepId, "DELETE artifact contributor identity is invalid or duplicated", null);
		}
		Files.createDirectories(outputDirectory);
		final Path tempDirectory = Files.createTempDirectory(outputDirectory, ".delete-aggregate-");
		FailurePreservingCleanup.always(
						() -> publishInTempDirectory(
										stepId, outputDirectory, sources, nodeIds, selection, selectionCompletion,
										tempDirectory),
						() -> deleteTempTree(tempDirectory));
	}

	private static Void publishInTempDirectory(
					final String stepId,
					final Path outputDirectory,
					final List<NodeSource> sources,
					final List<String> nodeIds,
					final Path selection,
					final Path selectionCompletion,
					final Path tempDirectory)
					throws IOException {
		final IntegrityManifestCompletion selectionProvenance = validateSelectionProvenance(
						selection, selectionCompletion);
		final List<String> identity = new ArrayList<>();
		final long[] aggregateCounters = new long[TOTAL_LAST_COUNTER - TOTAL_FIRST_COUNTER + 1];
		final List<Path> normalizedRequests = new ArrayList<>(sources.size());
		final List<Path> objectSources = new ArrayList<>(sources.size());
		final List<Path> residualSources = new ArrayList<>(sources.size());
		final List<Path> verificationSources = new ArrayList<>(sources.size());
		final boolean verificationEvidence = sources.stream().allMatch(source -> source.verification() != null);
		if (!verificationEvidence && sources.stream().anyMatch(source -> source.verification() != null)) {
			throw terminal(stepId, "DELETE verification evidence contributor set is incomplete", null);
		}
		for (int nodeIndex = 0; nodeIndex < sources.size(); nodeIndex++) {
			final NodeSource source = sources.get(nodeIndex);
			final List<String> totals = readSingleTotals(source.totals());
			validateTotalsIdentity(stepId, totals, identity);
			if (!"true".equals(totals.get(22))) {
				throw terminal(stepId, "DELETE contributor terminal evidence is incomplete", null);
			}
			for (int index = TOTAL_FIRST_COUNTER; index <= TOTAL_LAST_COUNTER; index++) {
				aggregateCounters[index - TOTAL_FIRST_COUNTER] = Math.addExact(
								aggregateCounters[index - TOTAL_FIRST_COUNTER], nonnegative(totals.get(index)));
			}
			final ValidatedNode validated = validateNode(
							stepId, source, totals, nodeIds.get(nodeIndex), tempDirectory, nodeIndex);
			normalizedRequests.add(validated.normalizedRequests());
			objectSources.add(source.objects());
			residualSources.add(source.residual());
			if (verificationEvidence) {
				verificationSources.add(source.verification());
			}
		}

		final Map<String, Path> staging = createStaging(outputDirectory, verificationEvidence);
		FailurePreservingCleanup.always(() -> {
			final long requestRows = DeleteCsvExternalSorter.sort(
							normalizedRequests, DeleteArtifacts.REQUESTS_HEADER, REQUEST_ORDER,
							staging.get(DeleteArtifacts.REQUESTS_FILE_NAME), tempDirectory, "requests");
			final long objectRows = DeleteCsvExternalSorter.sort(
							objectSources, DeleteArtifacts.OBJECTS_HEADER, TARGET_ID_ORDER,
							staging.get(DeleteArtifacts.OBJECTS_FILE_NAME), tempDirectory, "objects-target");
			final long residualRows = DeleteCsvExternalSorter.sort(
							residualSources, IntegrityManifestItemInput.HEADER, MANIFEST_ORDER,
							staging.get(DeleteArtifacts.RESIDUAL_FILE_NAME), tempDirectory, "residual");
			final long verificationRows = verificationEvidence
							? DeleteCsvExternalSorter.sort(
											verificationSources, DeleteArtifacts.VERIFICATION_HEADER,
											VERIFICATION_TARGET_ORDER,
											staging.get(DeleteArtifacts.VERIFICATION_FILE_NAME), tempDirectory,
											"verification-target")
							: 0;
			final Path objectsByManifest = Files.createTempFile(tempDirectory, "objects-manifest-", ".csv");
			DeleteCsvExternalSorter.sort(
							objectSources, DeleteArtifacts.OBJECTS_HEADER, OBJECT_MANIFEST_ORDER,
							objectsByManifest, tempDirectory, "objects-manifest");
			final Path objectsByRequest = Files.createTempFile(tempDirectory, "objects-request-", ".csv");
			DeleteCsvExternalSorter.sort(
							objectSources, DeleteArtifacts.OBJECTS_HEADER, OBJECT_REQUEST_ORDER,
							objectsByRequest, tempDirectory, "objects-request");
			final Path verificationByManifest = verificationEvidence
							? Files.createTempFile(tempDirectory, "verification-manifest-", ".csv")
							: null;
			if (verificationEvidence) {
				DeleteCsvExternalSorter.sort(
								verificationSources, DeleteArtifacts.VERIFICATION_HEADER,
								VERIFICATION_MANIFEST_ORDER, verificationByManifest, tempDirectory,
								"verification-manifest");
			}

			validateUnique(
							stepId, staging.get(DeleteArtifacts.REQUESTS_FILE_NAME),
							DeleteArtifacts.REQUESTS_HEADER, 1, "request");
			validateUnique(
							stepId, staging.get(DeleteArtifacts.OBJECTS_FILE_NAME),
							DeleteArtifacts.OBJECTS_HEADER, 2, "target");
			validateSelectionCoverage(
							stepId, selection, objectsByManifest,
							aggregateCounters[12 - TOTAL_FIRST_COUNTER], objectRows);
			validateRequestLinks(
							stepId, staging.get(DeleteArtifacts.REQUESTS_FILE_NAME), objectsByRequest);
			validateResidual(
							stepId, objectsByManifest, staging.get(DeleteArtifacts.RESIDUAL_FILE_NAME));
			if (verificationEvidence) {
				validateUnique(
								stepId, staging.get(DeleteArtifacts.VERIFICATION_FILE_NAME),
								DeleteArtifacts.VERIFICATION_HEADER, 1, "verification target");
				validateVerification(
								stepId, staging.get(DeleteArtifacts.OBJECTS_FILE_NAME),
								staging.get(DeleteArtifacts.VERIFICATION_FILE_NAME),
								verificationByManifest, staging.get(DeleteArtifacts.RESIDUAL_FILE_NAME),
								objectRows, verificationRows);
			}

			final List<String> aggregateTotals = new ArrayList<>(identity);
			for (final long counter : aggregateCounters) {
				aggregateTotals.add(Long.toString(counter));
			}
			aggregateTotals.add(Boolean.TRUE.toString());
			writeCsv(
							staging.get(DeleteArtifacts.METRICS_FILE_NAME), DeleteArtifacts.METRICS_HEADER,
							List.of(aggregateTotals));
			Files.copy(
							selection, staging.get(DeleteArtifacts.SELECTION_FILE_NAME),
							java.nio.file.StandardCopyOption.REPLACE_EXISTING);
			JSON.writerWithDefaultPrettyPrinter().writeValue(
							staging.get(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME).toFile(),
							retainedSelectionProvenance(selectionProvenance, selection));
			writeCompletion(
							staging, identity, nodeIds, requestRows, objectRows, residualRows,
							verificationEvidence, verificationRows);
			publishStaging(staging, outputDirectory);
			return null;
		}, () -> {
			for (final Path path : staging.values()) {
				Files.deleteIfExists(path);
			}
		});
		return null;
	}

	private static ValidatedNode validateNode(
					final String stepId,
					final NodeSource source,
					final List<String> totals,
					final String nodeId,
					final Path tempDirectory,
					final int nodeIndex)
					throws IOException {
		final long[] requestOutcomes = new long[4];
		long requestRows = 0;
		long tracedTargets = 0;
		final long configuredBatchSize = nonnegative(totals.get(5));
		final Path normalizedRequests = Files.createTempFile(
						tempDirectory, "requests-node-" + nodeIndex + "-", ".csv");
		try (Reader reader = Files.newBufferedReader(source.requests(), StandardCharsets.UTF_8);
						CSVParser parser = CSVFormat.RFC4180.parse(reader);
						CSVPrinter normalized = new CSVPrinter(
										Files.newBufferedWriter(
														normalizedRequests, StandardCharsets.UTF_8,
														StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
										IntegrityCsvFormat.RFC4180_LF)) {
			final Iterator<CSVRecord> records = parser.iterator();
			if (!records.hasNext() || !DeleteArtifacts.REQUESTS_HEADER.equals(records.next().toList())) {
				throw new IOException("DELETE request trace has a noncanonical header");
			}
			normalized.printRecord(DeleteArtifacts.REQUESTS_HEADER);
			while (records.hasNext()) {
				final List<String> row = records.next().toList();
				validateRequestRow(stepId, row);
				if (nonnegative(row.get(3)) > configuredBatchSize) {
					throw terminal(stepId, "DELETE request exceeds the configured batch size", null);
				}
				requestRows = Math.addExact(requestRows, 1);
				tracedTargets = Math.addExact(tracedTargets, nonnegative(row.get(3)));
				final int outcome = requestOutcome(row.get(REQUEST_OUTCOME_INDEX));
				requestOutcomes[outcome] = Math.addExact(requestOutcomes[outcome], 1);
				final List<String> rebound = new ArrayList<>(row);
				rebound.set(5, nodeId);
				normalized.printRecord(rebound);
			}
		}

		final long[] objectOutcomes = new long[4];
		long objectRows = 0;
		try (CsvCursor objects = new CsvCursor(source.objects(), DeleteArtifacts.OBJECTS_HEADER)) {
			while (objects.advance()) {
				validateObjectRow(stepId, objects.current);
				objectRows = Math.addExact(objectRows, 1);
				final int outcome = objectOutcome(objects.current.get(OBJECT_OUTCOME_INDEX));
				objectOutcomes[outcome] = Math.addExact(objectOutcomes[outcome], 1);
			}
		}
		long residualRows = 0;
		try (CsvCursor residual = new CsvCursor(source.residual(), IntegrityManifestItemInput.HEADER)) {
			while (residual.advance()) {
				validateManifestRow(residual.current);
				residualRows = Math.addExact(residualRows, 1);
			}
		}
		validateNodeCounters(
						stepId, totals, requestRows, tracedTargets, requestOutcomes,
						objectRows, objectOutcomes, residualRows);
		return new ValidatedNode(normalizedRequests);
	}

	private static void validateNodeCounters(
					final String stepId,
					final List<String> totals,
					final long requestRows,
					final long tracedTargets,
					final long[] requestOutcomes,
					final long objectRows,
					final long[] objectOutcomes,
					final long residualRows) {
		final long attemptedRequests = nonnegative(totals.get(7));
		final long selected = nonnegative(totals.get(12));
		final long attempted = nonnegative(totals.get(13));
		final long accepted = nonnegative(totals.get(14));
		final long failed = nonnegative(totals.get(15));
		final long unattempted = nonnegative(totals.get(16));
		final long unresolved = nonnegative(totals.get(17));
		final long actualRequests = nonnegative(totals.get(18));
		final long actualObjects = nonnegative(totals.get(19));
		final long fullBatches = nonnegative(totals.get(20));
		final long partialBatches = nonnegative(totals.get(21));
		if (requestRows != attemptedRequests
						|| objectRows != selected
						|| residualRows > selected
						|| requestOutcomes[0] != nonnegative(totals.get(8))
						|| requestOutcomes[1] != nonnegative(totals.get(9))
						|| requestOutcomes[2] != nonnegative(totals.get(10))
						|| requestOutcomes[3] != nonnegative(totals.get(11))
						|| objectOutcomes[0] != accepted
						|| objectOutcomes[1] != failed
						|| objectOutcomes[2] != unattempted
						|| objectOutcomes[3] != unresolved
						|| attemptedRequests != addExact(requestOutcomes)
						|| selected != Math.addExact(attempted, unattempted)
						|| attempted != addExact(accepted, failed, unresolved)
						|| actualRequests != attemptedRequests
						|| actualObjects != attempted
						|| tracedTargets != attempted
						|| Math.addExact(fullBatches, partialBatches) != actualRequests
						|| !batchShapeReconciles(
										nonnegative(totals.get(5)), actualObjects, fullBatches, partialBatches)) {
			throw terminal(stepId, "DELETE contributor counters do not reconcile", null);
		}
	}

	private static boolean batchShapeReconciles(
					final long configuredSize,
					final long actualObjects,
					final long fullBatches,
					final long partialBatches) {
		final long fullObjects = Math.multiplyExact(fullBatches, configuredSize);
		if (configuredSize == 1) {
			return partialBatches == 0 && actualObjects == fullObjects;
		}
		final long minimumObjects = Math.addExact(fullObjects, partialBatches);
		final long maximumObjects = Math.addExact(
						fullObjects, Math.multiplyExact(partialBatches, configuredSize - 1));
		return actualObjects >= minimumObjects && actualObjects <= maximumObjects;
	}

	private static void validateSelectionCoverage(
					final String stepId,
					final Path selection,
					final Path objectsByManifest,
					final long selectedCounter,
					final long objectRows)
					throws IOException {
		long rows = 0;
		try (CsvCursor selected = new CsvCursor(selection, IntegrityManifestItemInput.HEADER);
						CsvCursor objects = new CsvCursor(objectsByManifest, DeleteArtifacts.OBJECTS_HEADER)) {
			boolean hasSelected = selected.advance();
			boolean hasObject = objects.advance();
			while (hasSelected && hasObject) {
				validateManifestRow(selected.current);
				if (compareManifest(
								selected.current, 0, 1, 2, 3,
								objects.current, 4, 5, 6, 7) != 0) {
					throw terminal(
									stepId,
									"DELETE target reconciliation does not exactly cover the frozen selection",
									null);
				}
				rows = Math.addExact(rows, 1);
				hasSelected = selected.advance();
				hasObject = objects.advance();
			}
			if (hasSelected || hasObject || rows != selectedCounter || rows != objectRows) {
				throw terminal(stepId, "DELETE frozen selection count does not match terminal evidence", null);
			}
		}
	}

	private static void validateRequestLinks(
					final String stepId, final Path requestsPath, final Path objectsByRequest)
					throws IOException {
		try (CsvCursor requests = new CsvCursor(requestsPath, DeleteArtifacts.REQUESTS_HEADER);
						CsvCursor objects = new CsvCursor(objectsByRequest, DeleteArtifacts.OBJECTS_HEADER)) {
			boolean hasObject = objects.advance();
			while (hasObject && objects.current.get(1).isEmpty()) {
				if (!DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED.equals(objects.current.get(8))) {
					throw terminal(stepId, "DELETE attempted target has no request link", null);
				}
				hasObject = objects.advance();
			}
			while (requests.advance()) {
				final String requestId = requests.current.get(1);
				long linked = 0;
				final long[] linkedOutcomes = new long[4];
				final long[] linkedFailureClassifications = new long[2];
				while (hasObject && objects.current.get(1).compareTo(requestId) < 0) {
					throw terminal(stepId, "DELETE target reconciliation has a missing request link", null);
				}
				while (hasObject && objects.current.get(1).equals(requestId)) {
					if (DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED.equals(objects.current.get(8))) {
						throw terminal(stepId, "DELETE unattempted target cannot claim an API invocation", null);
					}
					linked = Math.addExact(linked, 1);
					final int outcome = objectOutcome(objects.current.get(8));
					linkedOutcomes[outcome] = Math.addExact(linkedOutcomes[outcome], 1);
					if (DeleteArtifacts.FAILURE_CLASSIFICATION_OPERATIONAL.equals(objects.current.get(9))) {
						linkedFailureClassifications[0] = Math.addExact(linkedFailureClassifications[0], 1);
					} else if (DeleteArtifacts.FAILURE_CLASSIFICATION_PROTOCOL.equals(objects.current.get(9))) {
						linkedFailureClassifications[1] = Math.addExact(linkedFailureClassifications[1], 1);
					}
					hasObject = objects.advance();
				}
				if (linked != nonnegative(requests.current.get(3))) {
					throw terminal(stepId, "DELETE request target count does not match reconciliation rows", null);
				}
				validateRequestOutcomeComposition(
								stepId, requests.current.get(4), linkedOutcomes,
								linkedFailureClassifications, linked);
			}
			if (hasObject) {
				throw terminal(stepId, "DELETE target reconciliation has a missing request link", null);
			}
		}
	}

	private static void validateRequestOutcomeComposition(
					final String stepId,
					final String requestOutcome,
					final long[] targets,
					final long[] failureClassifications,
					final long linked) {
		final long accepted = targets[0];
		final long failed = targets[1];
		final long unattempted = targets[2];
		final long unresolved = targets[3];
		final long operational = failureClassifications[0];
		final long protocol = failureClassifications[1];
		final boolean compatible = switch (requestOutcome) {
		case DeleteArtifacts.REQUEST_OUTCOME_FULL_SUCCESS -> accepted == linked && operational == 0 && protocol == 0;
		case DeleteArtifacts.REQUEST_OUTCOME_PARTIAL -> accepted > 0 && failed > 0
						&& Math.addExact(accepted, failed) == linked
						&& operational == failed && protocol == 0;
		case DeleteArtifacts.REQUEST_OUTCOME_FAILED -> failed == linked
						&& ((operational == linked && protocol == 0)
										|| (protocol == linked && operational == 0));
		case DeleteArtifacts.REQUEST_OUTCOME_UNRESOLVED -> unresolved == linked && operational == 0 && protocol == 0;
		default -> false;
		};
		if (!compatible || unattempted != 0) {
			throw terminal(stepId, "DELETE request outcome contradicts its target reconciliation", null);
		}
	}

	private static void validateResidual(
					final String stepId, final Path objectsByManifest, final Path residualPath)
					throws IOException {
		try (CsvCursor objects = new CsvCursor(objectsByManifest, DeleteArtifacts.OBJECTS_HEADER);
						CsvCursor residual = new CsvCursor(residualPath, IntegrityManifestItemInput.HEADER)) {
			boolean hasResidual = residual.advance();
			while (objects.advance() && hasResidual) {
				final int comparison = compareManifest(
								objects.current, 4, 5, 6, 7,
								residual.current, 0, 1, 2, 3);
				if (comparison > 0) {
					throw terminal(stepId, "DELETE residual contains an identity outside selection", null);
				}
				if (comparison == 0) {
					hasResidual = residual.advance();
				}
			}
			if (hasResidual) {
				throw terminal(stepId, "DELETE residual contains an identity outside selection", null);
			}
		}
	}

	private static void validateVerification(
					final String stepId,
					final Path objectsByTarget,
					final Path verificationByTarget,
					final Path verificationByManifest,
					final Path residualPath,
					final long objectRows,
					final long verificationRows)
					throws IOException {
		if (objectRows != verificationRows) {
			throw terminal(stepId, "DELETE verification does not cover every selected identity", null);
		}
		try (CsvCursor objects = new CsvCursor(objectsByTarget, DeleteArtifacts.OBJECTS_HEADER);
						CsvCursor verification = new CsvCursor(
										verificationByTarget, DeleteArtifacts.VERIFICATION_HEADER)) {
			while (objects.advance()) {
				if (!verification.advance()) {
					throw terminal(stepId, "DELETE verification target evidence is incomplete", null);
				}
				validateVerificationRow(stepId, objects.current, verification.current);
			}
			if (verification.advance()) {
				throw terminal(stepId, "DELETE verification target evidence exceeds selection", null);
			}
		}
		try (CsvCursor verification = new CsvCursor(
						verificationByManifest, DeleteArtifacts.VERIFICATION_HEADER);
						CsvCursor residual = new CsvCursor(residualPath, IntegrityManifestItemInput.HEADER)) {
			boolean hasResidual = residual.advance();
			while (verification.advance()) {
				final boolean expectedResidual = canonicalBoolean(
								stepId, verification.current.get(14), "verification residual");
				if (!expectedResidual) {
					continue;
				}
				if (!hasResidual || compareManifest(
								verification.current, 3, 4, 5, 6,
								residual.current, 0, 1, 2, 3) != 0) {
					throw terminal(stepId, "DELETE residual disagrees with verification evidence", null);
				}
				hasResidual = residual.advance();
			}
			if (hasResidual) {
				throw terminal(stepId, "DELETE residual exceeds verification evidence", null);
			}
		}
	}

	private static void validateVerificationRow(
					final String stepId, final List<String> object, final List<String> verification) {
		if (verification.size() != DeleteArtifacts.VERIFICATION_HEADER.size()
						|| !DeleteArtifacts.SCHEMA_VERSION.equals(verification.get(0))
						|| !object.get(2).equals(verification.get(1))
						|| !object.get(3).equals(verification.get(2))
						|| !object.subList(4, 8).equals(verification.subList(3, 7))
						|| !object.get(8).equals(verification.get(7))) {
			throw terminal(stepId, "DELETE verification identity or operational outcome disagrees", null);
		}
		final boolean preEnabled = canonicalBoolean(
						stepId, verification.get(8), "pre-verification enablement");
		final String prePresence = verification.get(9);
		final boolean postEnabled = canonicalBoolean(
						stepId, verification.get(10), "post-verification enablement");
		final String postPresence = verification.get(11);
		if (!verificationPresence(prePresence) || !verificationPresence(postPresence)) {
			throw terminal(stepId, "DELETE verification contains an invalid presence classification", null);
		}
		if (preEnabled == DeleteArtifacts.VERIFICATION_PRESENCE_DISABLED.equals(prePresence)
						|| postEnabled == DeleteArtifacts.VERIFICATION_PRESENCE_DISABLED.equals(postPresence)) {
			throw terminal(stepId, "DELETE verification enablement and presence disagree", null);
		}
		final String outcome = verification.get(7);
		final boolean correctness = canonicalBoolean(
						stepId, verification.get(12), "verification correctness");
		final boolean inconclusive = canonicalBoolean(
						stepId, verification.get(13), "verification inconclusive");
		final boolean residual = canonicalBoolean(
						stepId, verification.get(14), "verification residual");
		final boolean expectedCorrectness = DeleteArtifacts.TARGET_OUTCOME_ACCEPTED.equals(outcome)
						&& postEnabled
						&& !DeleteArtifacts.VERIFICATION_PRESENCE_UNATTEMPTED.equals(postPresence)
						&& !DeleteArtifacts.VERIFICATION_PRESENCE_ABSENT.equals(postPresence);
		final boolean expectedInconclusive = !DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED.equals(outcome)
						&& DeleteArtifacts.VERIFICATION_PRESENCE_UNRESOLVED.equals(postPresence);
		final boolean expectedResidual = postEnabled
						&& !DeleteArtifacts.VERIFICATION_PRESENCE_UNATTEMPTED.equals(postPresence)
										? !DeleteArtifacts.VERIFICATION_PRESENCE_ABSENT.equals(postPresence)
										: !DeleteArtifacts.TARGET_OUTCOME_ACCEPTED.equals(outcome);
		if (correctness != expectedCorrectness || inconclusive != expectedInconclusive
						|| residual != expectedResidual) {
			throw terminal(stepId, "DELETE verification classifications do not reconcile", null);
		}
	}

	private static boolean verificationPresence(final String value) {
		return DeleteArtifacts.isVerificationPresence(value);
	}

	private static boolean canonicalBoolean(
					final String stepId, final String value, final String field) {
		if (!"true".equals(value) && !"false".equals(value)) {
			throw terminal(stepId, "DELETE " + field + " is not canonical", null);
		}
		return Boolean.parseBoolean(value);
	}

	private static void validateUnique(
					final String stepId,
					final Path path,
					final List<String> header,
					final int identityIndex,
					final String identityName)
					throws IOException {
		try (CsvCursor rows = new CsvCursor(path, header)) {
			String prior = null;
			while (rows.advance()) {
				final String current = rows.current.get(identityIndex);
				if (current.equals(prior)) {
					throw terminal(
									stepId,
									"DELETE artifact contains a duplicate " + identityName + " identity",
									null);
				}
				prior = current;
			}
		}
	}

	private static Map<String, Path> createStaging(
					final Path outputDirectory, final boolean verificationEvidence) throws IOException {
		final Map<String, Path> staging = new LinkedHashMap<>();
		try {
			for (final String name : artifactNames(verificationEvidence, true)) {
				staging.put(name, Files.createTempFile(outputDirectory, "." + name + ".", ".staging"));
			}
			return staging;
		} catch (final IOException e) {
			for (final Path path : staging.values()) {
				try {
					Files.deleteIfExists(path);
				} catch (final IOException cleanupFailure) {
					e.addSuppressed(cleanupFailure);
				}
			}
			throw e;
		}
	}

	private static void writeCompletion(
					final Map<String, Path> staging,
					final List<String> identity,
					final List<String> nodeIds,
					final long requestRows,
					final long objectRows,
					final long residualRows,
					final boolean verificationEvidence,
					final long verificationRows)
					throws IOException {
		final Map<String, Object> completion = new LinkedHashMap<>();
		completion.put("version", verificationEvidence ? 2 : 1);
		completion.put("status", "complete");
		completion.put("schema_version", DeleteArtifacts.SCHEMA_VERSION);
		completion.put("mode", identity.get(4));
		completion.put("configured_batch_size", Integer.parseInt(identity.get(5)));
		completion.put("selection_order", identity.get(6));
		completion.put("contributors", List.copyOf(nodeIds));
		completion.put("request_rows", requestRows);
		completion.put("target_rows", objectRows);
		completion.put("residual_rows", residualRows);
		if (verificationEvidence) {
			completion.put("verification_rows", verificationRows);
		}
		final Map<String, String> sha256 = new LinkedHashMap<>();
		for (final String name : artifactNames(verificationEvidence, false)) {
			sha256.put(name, sha256(staging.get(name)));
		}
		completion.put("sha256", sha256);
		JSON.writerWithDefaultPrettyPrinter().writeValue(
						staging.get(DeleteArtifacts.COMPLETION_FILE_NAME).toFile(), completion);
	}

	private static void publishStaging(
					final Map<String, Path> staging, final Path outputDirectory) throws IOException {
		final boolean verificationEvidence = staging.containsKey(DeleteArtifacts.VERIFICATION_FILE_NAME);
		for (final String name : artifactNames(verificationEvidence, false)) {
			publishRetrySafe(staging.get(name), outputDirectory.resolve(name));
		}
		publishRetrySafe(
						staging.get(DeleteArtifacts.COMPLETION_FILE_NAME),
						outputDirectory.resolve(DeleteArtifacts.COMPLETION_FILE_NAME));
	}

	private static List<String> artifactNames(
					final boolean verificationEvidence, final boolean includeCompletion) {
		final List<String> names = new ArrayList<>(List.of(
						DeleteArtifacts.METRICS_FILE_NAME,
						DeleteArtifacts.REQUESTS_FILE_NAME,
						DeleteArtifacts.OBJECTS_FILE_NAME,
						DeleteArtifacts.RESIDUAL_FILE_NAME,
						DeleteArtifacts.SELECTION_FILE_NAME,
						DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME));
		if (verificationEvidence) {
			names.add(DeleteArtifacts.VERIFICATION_FILE_NAME);
		}
		if (includeCompletion) {
			names.add(DeleteArtifacts.COMPLETION_FILE_NAME);
		}
		return names;
	}

	private static IntegrityManifestCompletion validateSelectionProvenance(
					final Path selection, final Path selectionCompletion) throws IOException {
		if (!IntegrityManifestCompletion.completionPath(selection).toAbsolutePath().normalize()
						.equals(selectionCompletion.toAbsolutePath().normalize())) {
			throw new IOException("DELETE frozen selection completion path is incompatible");
		}
		final IntegrityManifestCompletion record = JSON.readValue(
						selectionCompletion.toFile(), IntegrityManifestCompletion.class);
		final IntegrityInputProvenance provenance = switch (record.producerKind()) {
		case IntegrityManifestCompletion.PRODUCER_ENGINE_STEP -> IntegrityInputProvenance.ENGINE_STEP;
		case IntegrityManifestCompletion.PRODUCER_CLI_STAGER -> IntegrityInputProvenance.CLI_STAGER;
		default -> throw new IOException("DELETE frozen selection producer is unsupported");
		};
		return IntegrityManifestCompletion.validate(
						selection, record.runId(), provenance, record.producerId());
	}

	private static IntegrityManifestCompletion retainedSelectionProvenance(
					final IntegrityManifestCompletion selectionProvenance, final Path selection)
					throws IOException {
		return new IntegrityManifestCompletion(
						selectionProvenance.version(),
						selectionProvenance.status(),
						selectionProvenance.runId(),
						selectionProvenance.producerKind(),
						selectionProvenance.producerId(),
						DeleteArtifacts.SELECTION_FILE_NAME,
						selectionProvenance.sourceRecordCount(),
						selectionProvenance.uniqueRecordCount(),
						selectionProvenance.selectedRecordCount(),
						selectionProvenance.excludedDeleteMarkerCount(),
						Files.size(selection),
						sha256(selection));
	}

	private static List<String> readSingleTotals(final Path path) throws IOException {
		try (CsvCursor totals = new CsvCursor(path, DeleteArtifacts.METRICS_HEADER)) {
			if (!totals.advance()) {
				throw new IOException("DELETE totals is missing its terminal row");
			}
			final List<String> row = List.copyOf(totals.current);
			if (totals.advance()) {
				throw new IOException("DELETE totals must contain exactly one row per contributor");
			}
			return row;
		}
	}

	private static void validateTotalsIdentity(
					final String stepId, final List<String> totals, final List<String> identity) {
		if (totals.size() != DeleteArtifacts.METRICS_HEADER.size()
						|| !DeleteArtifacts.SCHEMA_VERSION.equals(totals.get(0))
						|| !DELETE_REQUEST_UNIT.equals(totals.get(1))
						|| !DELETE_OBJECT_UNIT.equals(totals.get(2))
						|| !DELETE_REQUEST_UNIT.equals(totals.get(3))) {
			throw terminal(stepId, "DELETE totals schema or units are incompatible", null);
		}
		final long batchSize = nonnegative(totals.get(5));
		if (!(DELETE_IDENTITY_MODE_SINGLE.equals(totals.get(4))
						|| DELETE_IDENTITY_MODE_BATCH.equals(totals.get(4)))
						|| batchSize == 0 || batchSize > DeleteRequest.MAX_TARGET_COUNT
						|| (DELETE_IDENTITY_MODE_SINGLE.equals(totals.get(4)) != (batchSize == 1))
						|| !DELETE_SELECTION_ORDER_CANONICAL.equals(totals.get(6))) {
			throw terminal(stepId, "DELETE totals merge identity is incompatible", null);
		}
		final List<String> candidate = totals.subList(0, TOTAL_FIRST_COUNTER);
		if (identity.isEmpty()) {
			identity.addAll(candidate);
		} else if (!identity.equals(candidate)) {
			throw terminal(stepId, "DELETE result identities cannot be merged", null);
		}
	}

	private static void validateRequestRow(final String stepId, final List<String> row) {
		if (row.size() != DeleteArtifacts.REQUESTS_HEADER.size()
						|| !DeleteArtifacts.SCHEMA_VERSION.equals(row.get(0))
						|| row.get(1).isEmpty() || row.get(2).isEmpty() || row.get(5).isEmpty()
						|| requestOutcome(row.get(4)) < 0
						|| nonnegative(row.get(3)) == 0) {
			throw terminal(stepId, "DELETE request trace row is invalid", null);
		}
		for (int index = 6; index <= 8; index++) {
			nonnegative(row.get(index));
		}
	}

	private static void validateObjectRow(final String stepId, final List<String> row) {
		final String outcome = row.size() > OBJECT_OUTCOME_INDEX ? row.get(OBJECT_OUTCOME_INDEX) : "";
		final String classification = row.size() > 9 ? row.get(9) : "";
		final boolean failedOutcome = DeleteArtifacts.TARGET_OUTCOME_FAILED.equals(outcome);
		final boolean noneClassification = DeleteArtifacts.FAILURE_CLASSIFICATION_NONE.equals(classification);
		final boolean classificationCompatible = DeleteArtifacts.isFailureClassification(classification)
						&& (failedOutcome ? !noneClassification : noneClassification);
		if (row.size() != DeleteArtifacts.OBJECTS_HEADER.size()
						|| !DeleteArtifacts.SCHEMA_VERSION.equals(row.get(0))
						|| row.get(2).isEmpty() || row.get(4).isEmpty() || row.get(5).isEmpty()
						|| objectOutcome(outcome) < 0
						|| !classificationCompatible) {
			throw terminal(stepId, "DELETE target reconciliation row is invalid", null);
		}
		selectionIndex(row.get(3));
		nonnegative(row.get(6));
	}

	private static void validateManifestRow(final List<String> row) throws IOException {
		if (row.size() != IntegrityManifestItemInput.HEADER.size()
						|| row.get(0).isEmpty() || row.get(1).isEmpty()) {
			throw new IOException("DELETE recovery manifest row is invalid");
		}
		nonnegative(row.get(2));
	}

	private static int requestOutcome(final String outcome) {
		return DeleteArtifacts.requestOutcomeIndex(outcome);
	}

	private static int objectOutcome(final String outcome) {
		return DeleteArtifacts.targetOutcomeIndex(outcome);
	}

	private static int compareManifest(
					final List<String> left,
					final int leftBucket,
					final int leftKey,
					final int leftSize,
					final int leftVersion,
					final List<String> right,
					final int rightBucket,
					final int rightKey,
					final int rightSize,
					final int rightVersion) {
		final int identity = IntegrityManifestOrder.compareIdentity(
						left.get(leftBucket), left.get(leftKey), left.get(leftVersion),
						right.get(rightBucket), right.get(rightKey), right.get(rightVersion));
		return identity != 0
						? identity
						: Long.compare(nonnegative(left.get(leftSize)), nonnegative(right.get(rightSize)));
	}

	private static long selectionIndex(final String value) {
		try {
			final long parsed = Long.parseLong(value);
			if (parsed < -1 || !Long.toString(parsed).equals(value)) {
				throw new NumberFormatException("below compatibility sentinel or noncanonical");
			}
			return parsed;
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException("DELETE target selection index is invalid: " + value, e);
		}
	}

	private static long nonnegative(final String value) {
		try {
			final long parsed = Long.parseLong(value);
			if (parsed < 0 || !Long.toString(parsed).equals(value)) {
				throw new NumberFormatException("negative or noncanonical");
			}
			return parsed;
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException("DELETE artifact counter must be nonnegative: " + value, e);
		}
	}

	private static long addExact(final long... values) {
		long sum = 0;
		for (final long value : values) {
			sum = Math.addExact(sum, value);
		}
		return sum;
	}

	private static void writeCsv(
					final Path path, final List<String> header, final List<List<String>> rows)
					throws IOException {
		try (CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(
										path, StandardCharsets.UTF_8,
										StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
						IntegrityCsvFormat.RFC4180_LF)) {
			printer.printRecord(header);
			for (final List<String> row : rows) {
				printer.printRecord(row);
			}
		}
	}

	private static void publishRetrySafe(final Path staging, final Path target) throws IOException {
		if (Files.exists(target)) {
			if (!sha256(staging).equals(sha256(target))) {
				throw new IOException("conflicting DELETE artifact already exists: " + target);
			}
			Files.delete(staging);
			return;
		}
		IntegrityManifestCompletion.atomicMove(staging, target);
	}

	private static String sha256(final Path path) throws IOException {
		try {
			final MessageDigest digest = MessageDigest.getInstance("SHA-256");
			try (InputStream input = new DigestInputStream(Files.newInputStream(path), digest)) {
				input.transferTo(java.io.OutputStream.nullOutputStream());
			}
			return HexFormat.of().formatHex(digest.digest());
		} catch (final NoSuchAlgorithmException impossible) {
			throw new AssertionError(impossible);
		}
	}

	private static void deleteTempTree(final Path directory) throws IOException {
		IOException failure = null;
		try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
			for (final Path entry : entries) {
				try {
					if (Files.isDirectory(entry)) {
						deleteTempTree(entry);
					} else {
						Files.deleteIfExists(entry);
					}
				} catch (final IOException e) {
					if (failure == null) {
						failure = e;
					} else {
						failure.addSuppressed(e);
					}
				}
			}
		}
		try {
			Files.deleteIfExists(directory);
		} catch (final IOException e) {
			if (failure == null) {
				failure = e;
			} else {
				failure.addSuppressed(e);
			}
		}
		if (failure != null) {
			throw failure;
		}
	}

	private static IntegrityTerminalException terminal(
					final String stepId, final String message, final Throwable cause) {
		return new IntegrityTerminalException(Category.AGGREGATION, stepId, message, cause);
	}
}
