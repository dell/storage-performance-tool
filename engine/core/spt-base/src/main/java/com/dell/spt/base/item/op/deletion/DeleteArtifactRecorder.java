package com.dell.spt.base.item.op.deletion;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_OBJECT_UNIT;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_REQUEST_UNIT;

import com.dell.spt.base.integrity.FailurePreservingCleanup;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;

/** Emits lossless per-node DELETE evidence and the conservative unverified residual inventory. */
public final class DeleteArtifactRecorder implements AutoCloseable {
	@FunctionalInterface
	interface WriteHook {
		void beforeWrite(DeleteRequestOperation operation) throws Exception;
	}

	@FunctionalInterface
	interface StagingFileFactory {
		Path create(Path directory, String prefix, String suffix) throws IOException;
	}

	private static final byte STATUS_UNSEEN = 0;
	private static final byte STATUS_ACCEPTED = 1;
	private static final byte STATUS_FAILED = 2;
	private static final byte STATUS_UNRESOLVED = 3;
	private static final int DEFAULT_QUEUE_CAPACITY = 1_024;
	private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
	private static final long WRITER_POLL_MILLIS = 25;
	private static final String LOCAL_NODE = "local";

	private final String stepId;
	private final Path selectionManifest;
	private final long selectedCount;
	private final Path totalsPath;
	private final Path requestsPath;
	private final Path objectsPath;
	private final Path residualPath;
	private final Path verificationPath;
	private final Path rawRequestsPath;
	private final Path rawObjectsPath;
	private final ArrayBlockingQueue<DeleteRequestOperation> terminalQueue;
	private final WriteHook writeHook;
	private final long closeTimeoutMillis;
	private final AtomicBoolean closing = new AtomicBoolean();
	private final AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
	private final Thread writerThread;
	private boolean finished;

	public DeleteArtifactRecorder(
					final String stepId, final Path selectionManifest, final long selectedCount) {
		this(
						stepId, selectionManifest, selectedCount, DEFAULT_QUEUE_CAPACITY,
						DEFAULT_CLOSE_TIMEOUT_MILLIS, ignored -> {});
	}

	DeleteArtifactRecorder(
					final String stepId,
					final Path selectionManifest,
					final long selectedCount,
					final int queueCapacity,
					final long closeTimeoutMillis,
					final WriteHook writeHook) {
		this(
						stepId, selectionManifest, selectedCount, queueCapacity, closeTimeoutMillis,
						writeHook, Files::createTempFile);
	}

	DeleteArtifactRecorder(
					final String stepId,
					final Path selectionManifest,
					final long selectedCount,
					final int queueCapacity,
					final long closeTimeoutMillis,
					final WriteHook writeHook,
					final StagingFileFactory stagingFileFactory) {
		this.stepId = requireText(stepId, "DELETE step id");
		this.selectionManifest = selectionManifest.toAbsolutePath().normalize();
		if (selectedCount < 0) {
			throw new IllegalArgumentException("DELETE selected count must be nonnegative");
		}
		if (queueCapacity <= 0 || closeTimeoutMillis <= 0) {
			throw new IllegalArgumentException("DELETE artifact writer bounds must be positive");
		}
		this.selectedCount = selectedCount;
		this.terminalQueue = new ArrayBlockingQueue<>(queueCapacity);
		this.closeTimeoutMillis = closeTimeoutMillis;
		this.writeHook = writeHook;
		Path initializedRawRequests = null;
		Path initializedRawObjects = null;
		final Path initializedTotals;
		final Path initializedRequests;
		final Path initializedObjects;
		final Path initializedResidual;
		final Path initializedVerification;
		try {
			initializedTotals = loggerPath(Loggers.DELETE_METRICS_TOTAL);
			initializedRequests = loggerPath(Loggers.DELETE_REQUESTS);
			initializedObjects = loggerPath(Loggers.DELETE_OBJECTS);
			initializedResidual = loggerPath(Loggers.DELETE_RESIDUAL);
			initializedVerification = loggerPath(Loggers.DELETE_VERIFICATION);
			final Path directory = initializedTotals.toAbsolutePath().getParent();
			Files.createDirectories(directory);
			initializedRawRequests = stagingFileFactory.create(
							directory, ".delete.requests.", ".recording");
			initializeArtifact(initializedRawRequests, DeleteArtifacts.REQUESTS_HEADER);
			initializedRawObjects = stagingFileFactory.create(
							directory, ".delete.objects.", ".recording");
			initializeArtifact(initializedRawObjects, DeleteArtifacts.OBJECTS_HEADER);
		} catch (final IOException e) {
			deleteQuietly(initializedRawRequests);
			deleteQuietly(initializedRawObjects);
			throw new IllegalStateException("Failed to initialize DELETE artifact staging", e);
		}
		totalsPath = initializedTotals;
		requestsPath = initializedRequests;
		objectsPath = initializedObjects;
		residualPath = initializedResidual;
		verificationPath = initializedVerification;
		rawRequestsPath = initializedRawRequests;
		rawObjectsPath = initializedRawObjects;
		writerThread = new Thread(this::writeTerminals, "spt-delete-artifacts-" + stepId);
		writerThread.setDaemon(true);
		writerThread.start();
	}

	/**
	 * Enqueues one immutable terminal request without blocking or throwing inside lifecycle ownership.
	 * A full/failed writer records a fail-closed condition surfaced by {@link #finish}.
	 */
	public void recordTerminal(final DeleteRequestOperation operation) {
		if (operation == null || closing.get() || asynchronousFailure.get() != null
						|| !terminalQueue.offer(operation)) {
			asynchronousFailure.compareAndSet(
							null, new IllegalStateException("DELETE artifact writer could not accept terminal evidence"));
		}
	}

	/** Completes and retry-safely publishes node evidence after admission closure and bounded drain. */
	public synchronized void finish(
					final OperationLifecycleSnapshot<?> lifecycle, final DeleteMetricsSnapshot metrics) {
		finish(lifecycle, metrics, null, null);
	}

	/** Completes evidence, refining residual identities when post-verification is available. */
	public synchronized void finish(
					final OperationLifecycleSnapshot<?> lifecycle,
					final DeleteMetricsSnapshot metrics,
					final DeleteVerificationReport postVerification) {
		finish(lifecycle, metrics, null, postVerification);
	}

	/** Completes evidence with selection-indexed validation and verification observations. */
	public synchronized void finish(
					final OperationLifecycleSnapshot<?> lifecycle,
					final DeleteMetricsSnapshot metrics,
					final DeleteVerificationReport preValidation,
					final DeleteVerificationReport postVerification) {
		if (finished) {
			return;
		}
		closing.set(true);
		awaitWriter();
		Path finalRequests = null;
		Path finalObjects = null;
		Path finalResidual = null;
		Path finalVerification = null;
		Path finalTotals = null;
		try {
			final Path directory = totalsPath.toAbsolutePath().getParent();
			finalRequests = Files.createTempFile(directory, ".delete.requests.", ".final");
			finalObjects = Files.createTempFile(directory, ".delete.objects.", ".final");
			finalResidual = Files.createTempFile(directory, ".items.", ".final");
			finalVerification = Files.createTempFile(directory, ".delete.verification.", ".final");
			finalTotals = Files.createTempFile(directory, ".delete.metrics.total.", ".final");
			Files.copy(rawRequestsPath, finalRequests, StandardCopyOption.REPLACE_EXISTING);
			Files.copy(rawObjectsPath, finalObjects, StandardCopyOption.REPLACE_EXISTING);
			appendUnresolved(lifecycle, finalRequests, finalObjects);
			writeResidualUnattemptedAndVerificationRows(
							finalObjects, finalResidual, finalVerification,
							metrics != null && metrics.verification().preValidationEnabled(),
							metrics != null && metrics.verification().postVerificationEnabled(),
							metrics != null && metrics.verification().postVerificationSkipped(),
							preValidation, postVerification);
			initializeArtifact(finalTotals, DeleteArtifacts.METRICS_HEADER);
			if (metrics != null) {
				append(finalTotals, metricsRow(metrics));
			}

			publishRetrySafe(finalRequests, requestsPath);
			publishRetrySafe(finalObjects, objectsPath);
			publishRetrySafe(finalResidual, residualPath);
			publishRetrySafe(finalVerification, verificationPath);
			final Throwable failure = asynchronousFailure.get();
			if (failure != null) {
				throw new IllegalStateException("DELETE artifact recording is incomplete", failure);
			}
			if (metrics == null) {
				throw new IllegalStateException("DELETE artifact totals are unavailable");
			}
			publishRetrySafe(finalTotals, totalsPath);
			finished = true;
			deletePrivateStaging();
		} catch (final IOException e) {
			throw new IllegalStateException("Failed to persist DELETE recovery evidence", e);
		} finally {
			deleteQuietly(finalRequests);
			deleteQuietly(finalObjects);
			deleteQuietly(finalResidual);
			deleteQuietly(finalVerification);
			deleteQuietly(finalTotals);
		}
	}

	private void writeTerminals() {
		try (CSVPrinter requests = appendPrinter(rawRequestsPath);
						CSVPrinter objects = appendPrinter(rawObjectsPath)) {
			while (!closing.get() || !terminalQueue.isEmpty()) {
				final DeleteRequestOperation operation = terminalQueue.poll(
								WRITER_POLL_MILLIS, TimeUnit.MILLISECONDS);
				if (operation == null) {
					continue;
				}
				writeHook.beforeWrite(operation);
				writeTerminal(operation, requests, objects);
			}
		} catch (final Throwable failure) {
			asynchronousFailure.compareAndSet(null, failure);
		}
	}

	private void awaitWriter() {
		try {
			writerThread.join(closeTimeoutMillis);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			asynchronousFailure.compareAndSet(null, e);
		}
		if (writerThread.isAlive()) {
			writerThread.interrupt();
			asynchronousFailure.compareAndSet(
							null, new IllegalStateException("DELETE artifact writer exceeded its bounded close time"));
		}
	}

	private static void writeTerminal(
					final DeleteRequestOperation operation,
					final CSVPrinter requests,
					final CSVPrinter objects)
					throws IOException {
		final DeleteRequestResult result = operation.deleteResult();
		if (result == null) {
			throw new IOException("DELETE terminal artifact requires reconciliation");
		}
		printRequest(requests, operation, DeleteArtifacts.requestOutcome(result.outcome()));
		final String requestId = DeleteArtifacts.requestId(operation.deleteRequest());
		for (final DeleteTargetResult target : result.targetResults()) {
			printTarget(
							objects, requestId, target.target(), target.outcome(),
							target.failureClassification(), target.errorMessage());
		}
	}

	private static void appendUnresolved(
					final OperationLifecycleSnapshot<?> lifecycle,
					final Path requestsPath,
					final Path objectsPath)
					throws IOException {
		try (CSVPrinter requests = appendPrinter(requestsPath);
						CSVPrinter objects = appendPrinter(objectsPath)) {
			for (final Object unresolved : lifecycle.unresolvedOperations()) {
				if (!(unresolved instanceof DeleteRequestOperation operation)) {
					throw new IOException("DELETE unresolved evidence contains a non-DELETE operation");
				}
				printRequest(
								requests, operation,
								DeleteArtifacts.REQUEST_OUTCOME_UNRESOLVED);
				final String requestId = DeleteArtifacts.requestId(operation.deleteRequest());
				for (final DeleteTarget target : operation.deleteRequest().targets()) {
					printTarget(
									objects, requestId, target, DeleteTargetOutcome.UNRESOLVED,
									DeleteFailureClassification.NONE, "");
				}
			}
		}
	}

	private void writeResidualUnattemptedAndVerificationRows(
					final Path finalObjectsPath,
					final Path finalResidualPath,
					final Path finalVerificationPath,
					final boolean preValidationEnabled,
					final boolean postVerificationEnabled,
					final boolean postVerificationSkipped,
					final DeleteVerificationReport preValidation,
					final DeleteVerificationReport postVerification) throws IOException {
		if (!Files.isRegularFile(selectionManifest)) {
			throw new IOException("DELETE frozen selection is missing: " + selectionManifest);
		}
		if (preValidationEnabled && !completeReport(preValidation, DeleteVerificationPhase.PRE_DELETE)) {
			throw new IOException("DELETE pre-validation evidence is incomplete");
		}
		final boolean postExpected = postVerificationEnabled && !postVerificationSkipped
						&& (!preValidationEnabled || preValidation.successful());
		if (postExpected && !completeReport(postVerification, DeleteVerificationPhase.POST_DELETE)) {
			throw new IOException("DELETE post-verification evidence is incomplete");
		}
		initializeArtifact(finalResidualPath, IntegrityManifestItemInput.HEADER);
		initializeArtifact(finalVerificationPath, DeleteArtifacts.VERIFICATION_HEADER);
		final Path statusPath = Files.createTempFile("spt-delete-status-", ".bin");
		try {
			try (final var status = new RandomAccessFile(statusPath.toFile(), "rw")) {
				status.setLength(selectedCount);
				readRecordedTargetStatuses(finalObjectsPath, status);
				try (CSVPrinter objects = appendPrinter(finalObjectsPath);
								CSVPrinter residual = appendPrinter(finalResidualPath);
								CSVPrinter verification = appendPrinter(finalVerificationPath);
								var statusInput = new java.io.BufferedInputStream(Files.newInputStream(statusPath));
								var prePresence = preValidation == null ? null : preValidation.cursor();
								var postPresence = postVerification == null ? null : postVerification.cursor();
								var selection = new IntegrityManifestItemInput(selectionManifest)) {
					long selectionIndex = 0;
					for (IntegrityManifestDataItem item = selection.get(); item != null; item = selection.get()) {
						if (selectionIndex >= selectedCount) {
							throw new IOException("DELETE selection contains more rows than its frozen count");
						}
						final DeleteTarget target = new DeleteTarget(item, selectionIndex);
						final int recordedOutcome = statusInput.read();
						if (recordedOutcome < 0) {
							throw new IOException("DELETE operational outcome storage ended early");
						}
						final byte outcome = (byte) recordedOutcome;
						if (outcome == STATUS_UNSEEN) {
							printTarget(
											objects, "", target, DeleteTargetOutcome.UNATTEMPTED,
											DeleteFailureClassification.NONE, "");
						}
						final DeleteVerificationProbe.Presence observedPostPresence = postPresence == null
										? null
										: postPresence.next();
						final boolean retainResidual = observedPostPresence == null
										? outcome != STATUS_ACCEPTED
										: observedPostPresence != DeleteVerificationProbe.Presence.ABSENT;
						if (retainResidual) {
							residual.printRecord(
											target.bucket(), target.key(), target.size(),
											target.versionId() == null ? "" : target.versionId());
						}
						final DeleteVerificationProbe.Presence observedPrePresence = prePresence == null
										? null
										: prePresence.next();
						final boolean correctnessFailure = outcome == STATUS_ACCEPTED
										&& observedPostPresence != null
										&& observedPostPresence != DeleteVerificationProbe.Presence.ABSENT;
						final boolean inconclusive = outcome != STATUS_UNSEEN
										&& observedPostPresence == DeleteVerificationProbe.Presence.UNRESOLVED;
						verification.printRecord(
										DeleteArtifacts.SCHEMA_VERSION,
										DeleteArtifacts.targetId(target),
										target.selectionIndex(),
										target.bucket(),
										target.key(),
										target.size(),
										target.versionId() == null ? "" : target.versionId(),
										operationalOutcome(outcome),
										preValidationEnabled,
										presence(preValidationEnabled, observedPrePresence),
										postVerificationEnabled,
										presence(postVerificationEnabled, observedPostPresence),
										correctnessFailure,
										inconclusive,
										retainResidual);
						selectionIndex++;
					}
					if (selectionIndex != selectedCount) {
						throw new IOException(
										"DELETE selection row count " + selectionIndex
														+ " does not match frozen count " + selectedCount);
					}
				}
			}
		} finally {
			Files.deleteIfExists(statusPath);
		}
	}

	private boolean completeReport(
					final DeleteVerificationReport report, final DeleteVerificationPhase phase) {
		return report != null && report.completePass() && report.phase() == phase
						&& report.selected() == selectedCount;
	}

	private static String operationalOutcome(final byte status) {
		return switch (status) {
		case STATUS_ACCEPTED -> DeleteArtifacts.TARGET_OUTCOME_ACCEPTED;
		case STATUS_FAILED -> DeleteArtifacts.TARGET_OUTCOME_FAILED;
		case STATUS_UNRESOLVED -> DeleteArtifacts.TARGET_OUTCOME_UNRESOLVED;
		default -> DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED;
		};
	}

	private static String presence(
					final boolean enabled, final DeleteVerificationProbe.Presence presence) {
		if (!enabled) {
			return DeleteArtifacts.VERIFICATION_PRESENCE_DISABLED;
		}
		return presence == null
						? DeleteArtifacts.VERIFICATION_PRESENCE_UNATTEMPTED
						: DeleteArtifacts.verificationPresence(presence);
	}

	private void readRecordedTargetStatuses(
					final Path path, final RandomAccessFile status)
					throws IOException {
		try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
						CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final var records = parser.iterator();
			if (!records.hasNext() || !DeleteArtifacts.OBJECTS_HEADER.equals(records.next().toList())) {
				throw new IOException("DELETE target reconciliation has a noncanonical header");
			}
			while (records.hasNext()) {
				final var row = records.next();
				if (row.size() != DeleteArtifacts.OBJECTS_HEADER.size()
								|| !DeleteArtifacts.SCHEMA_VERSION.equals(row.get(0))) {
					throw new IOException("DELETE target reconciliation contains an invalid row");
				}
				final byte outcome = status(row.get(8));
				long index;
				try {
					index = Long.parseLong(row.get(3));
				} catch (final NumberFormatException e) {
					throw new IOException("DELETE target reconciliation has an invalid target index", e);
				}
				if (index < 0) {
					index = compatibilitySelectionIndex(row.toList());
				}
				if (index >= selectedCount) {
					throw new IOException("DELETE target reconciliation index is outside the frozen selection");
				}
				status.seek(index);
				if (status.readByte() != STATUS_UNSEEN) {
					throw new IOException("DELETE target reconciliation contains a duplicate selection index");
				}
				status.seek(index);
				status.writeByte(outcome);
			}
		}
	}

	private long compatibilitySelectionIndex(final List<String> row) throws IOException {
		final long size;
		try {
			size = Long.parseLong(row.get(6));
		} catch (final NumberFormatException e) {
			throw new IOException("DELETE target reconciliation has an invalid object size", e);
		}
		long index = 0;
		try (var selection = new IntegrityManifestItemInput(selectionManifest)) {
			for (IntegrityManifestDataItem item = selection.get(); item != null; item = selection.get()) {
				final String version = item.versionId() == null ? "" : item.versionId();
				if (item.bucket().equals(row.get(4)) && item.name().equals(row.get(5))
								&& item.size() == size && version.equals(row.get(7))
								&& DeleteArtifacts.targetId(new DeleteTarget(item, index)).equals(row.get(2))) {
					return index;
				}
				index++;
			}
		}
		throw new IOException("DELETE target evidence contains an identity outside the frozen selection");
	}

	private static byte status(final String value) throws IOException {
		return switch (value) {
		case DeleteArtifacts.TARGET_OUTCOME_ACCEPTED -> STATUS_ACCEPTED;
		case DeleteArtifacts.TARGET_OUTCOME_FAILED -> STATUS_FAILED;
		case DeleteArtifacts.TARGET_OUTCOME_UNRESOLVED -> STATUS_UNRESOLVED;
		case DeleteArtifacts.TARGET_OUTCOME_UNATTEMPTED -> STATUS_UNSEEN;
		default -> throw new IOException("DELETE target reconciliation has an unknown outcome: " + value);
		};
	}

	private static void printRequest(
					final CSVPrinter printer,
					final DeleteRequestOperation operation,
					final String outcome)
					throws IOException {
		final String requestId = DeleteArtifacts.requestId(operation.deleteRequest());
		printer.printRecord(
						DeleteArtifacts.SCHEMA_VERSION,
						requestId,
						requestId,
						operation.deleteRequest().targets().size(),
						outcome,
						LOCAL_NODE,
						Math.max(0, operation.reqTimeStart()),
						requestDuration(operation),
						Math.max(0, operation.transportRequestLatency()));
	}

	private static void printTarget(
					final CSVPrinter printer,
					final String requestId,
					final DeleteTarget target,
					final DeleteTargetOutcome outcome,
					final DeleteFailureClassification classification,
					final String error)
					throws IOException {
		printer.printRecord(
						DeleteArtifacts.SCHEMA_VERSION,
						requestId,
						DeleteArtifacts.targetId(target),
						target.selectionIndex(),
						target.bucket(),
						target.key(),
						target.size(),
						target.versionId() == null ? "" : target.versionId(),
						DeleteArtifacts.targetOutcome(outcome),
						DeleteArtifacts.failureClassification(classification),
						error == null ? "" : error);
	}

	private static long requestDuration(final DeleteRequestOperation operation) {
		final long start = operation.reqTimeStart();
		final long done = operation.respTimeDone();
		return start > 0 && done > start ? done - start : 0;
	}

	private static String metricsRow(final DeleteMetricsSnapshot metrics) {
		return DeleteArtifacts.csvLine(
						DeleteArtifacts.SCHEMA_VERSION,
						DELETE_REQUEST_UNIT,
						DELETE_OBJECT_UNIT,
						DELETE_REQUEST_UNIT,
						metrics.mode(),
						metrics.configuredBatchSize(),
						metrics.selectionOrder(),
						metrics.requestAttempted(),
						metrics.requestFullSuccess(),
						metrics.requestPartial(),
						metrics.requestFailed(),
						metrics.requestUnresolved(),
						metrics.objectSelected(),
						metrics.objectAttempted(),
						metrics.objectAccepted(),
						metrics.objectFailed(),
						metrics.objectUnattempted(),
						metrics.objectUnresolved(),
						metrics.actualRequestCount(),
						metrics.actualObjectCount(),
						metrics.fullBatchCount(),
						metrics.partialBatchCount(),
						metrics.reconciled());
	}

	private Path loggerPath(final org.apache.logging.log4j.Logger logger) throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private static void initializeArtifact(final Path path, final List<String> header) throws IOException {
		try (CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(
										path, StandardCharsets.UTF_8,
										StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
						IntegrityCsvFormat.RFC4180_LF)) {
			printer.printRecord(header);
		}
	}

	private static CSVPrinter appendPrinter(final Path path) throws IOException {
		return new CSVPrinter(
						Files.newBufferedWriter(
										path, StandardCharsets.UTF_8,
										StandardOpenOption.APPEND, StandardOpenOption.WRITE),
						IntegrityCsvFormat.RFC4180_LF);
	}

	private static void append(final Path path, final String line) throws IOException {
		Files.writeString(
						path, line + "\n", StandardCharsets.UTF_8,
						StandardOpenOption.APPEND, StandardOpenOption.WRITE);
	}

	private static void publishRetrySafe(final Path source, final Path target) throws IOException {
		if (Files.exists(target)) {
			if (Files.mismatch(source, target) != -1) {
				throw new IOException("conflicting DELETE node artifact already exists: " + target);
			}
			return;
		}
		final Path staging = Files.createTempFile(
						target.toAbsolutePath().getParent(), "." + target.getFileName() + ".", ".publishing");
		FailurePreservingCleanup.always(() -> {
			Files.copy(source, staging, StandardCopyOption.REPLACE_EXISTING);
			com.dell.spt.base.integrity.IntegrityManifestCompletion.atomicMove(staging, target);
			return null;
		}, () -> Files.deleteIfExists(staging));
	}

	private void deletePrivateStaging() {
		deleteQuietly(rawRequestsPath);
		deleteQuietly(rawObjectsPath);
	}

	private static void deleteQuietly(final Path path) {
		if (path == null) {
			return;
		}
		try {
			Files.deleteIfExists(path);
		} catch (final IOException ignored) {
			// Preserve the primary result; private staging is safe for later process cleanup.
		}
	}

	private static String requireText(final String value, final String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be empty");
		}
		return value;
	}

	@Override
	public void close() {
		closing.set(true);
		if (!finished) {
			awaitWriter();
			deletePrivateStaging();
		}
	}
}
