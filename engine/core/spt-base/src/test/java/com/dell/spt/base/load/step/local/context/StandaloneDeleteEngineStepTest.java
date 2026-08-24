package com.dell.spt.base.load.step.local.context;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_PERCENTAGE;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.item.op.deletion.DeleteTransportTargetResult;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StandaloneDeleteEngineStepTest {
	private static final int MULTI_INPUT_CONTEXT_COUNT = 10;

	@Test
	void preValidationForwardsProbeInterruptionWithoutStartingDelete(
					@TempDir final Path temp) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nbucket,current,1,\n");
		final var config = config(1, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 1L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=1"));
		config.val("load-op-delete-preValidation", true);
		config.val("load-op-delete-verificationTimeoutMillis", 1_000L);
		final InterruptedException expected = new InterruptedException("external verification interrupt");
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		driver.presence = ignored -> {
			throwUnchecked(expected);
			return null;
		};
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-interrupted-pre",
						generator(config, driver, new ManifestInput(1)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"),
						(ignoredId, ignoredSelection, ignoredCount) -> null);
		try {
			step.holdObjectFailureBudgetAdmission();
			step.start();
			final InterruptedException actual = assertThrows(
							InterruptedException.class,
							step::validateDeleteInventoryBeforeAdmission);
			assertSame(expected, actual);
			assertEquals(1, driver.presenceCalls.get());
			assertEquals(0, driver.scheduled.get(), "interrupted pre-validation started timed DELETE");
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void postVerificationForwardsProbeInterruptionWithoutClassifyingInventory(
					@TempDir final Path temp) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nbucket,current,1,\n");
		final var config = config(1, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 1L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=1"));
		config.val("load-op-delete-postVerification", true);
		config.val("load-op-delete-verificationTimeoutMillis", 1_000L);
		final InterruptedException expected = new InterruptedException("external verification interrupt");
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		driver.presence = ignored -> {
			throwUnchecked(expected);
			return null;
		};
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-interrupted-post",
						generator(config, driver, new ManifestInput(1)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"),
						(ignoredId, ignoredSelection, ignoredCount) -> null);
		try {
			step.holdObjectFailureBudgetAdmission();
			step.start();
			final InterruptedException actual = assertThrows(
							InterruptedException.class,
							step::verifyDeleteInventoryAfterDrain);
			assertSame(expected, actual);
			assertEquals(1, driver.presenceCalls.get());
			assertEquals(0, driver.scheduled.get(), "interrupted post-verification scheduled DELETE work");
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void strictPreValidationRetainsClassificationAndTimingAndSkipsPostAfterFailure(
					@TempDir final Path temp) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection,
						"bucket,key,size,version_id\n"
										+ "bucket,current,1,\n"
										+ "bucket,exact,1,version-1\n");
		final var config = config(2, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 2L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 1L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=2"));
		config.val("load-op-delete-preValidation", true);
		config.val("load-op-delete-postVerification", true);
		config.val("load-op-delete-verificationTimeoutMillis", 1L);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		driver.presence = target -> "exact".equals(target.key())
						? DeleteVerificationProbe.Presence.ABSENT
						: DeleteVerificationProbe.Presence.PRESENT;
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-strict-pre",
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"),
						(ignoredId, ignoredSelection, ignoredCount) -> null);
		try {
			step.holdObjectFailureBudgetAdmission();
			step.start();
			assertThrows(
							IntegrityTerminalException.class,
							step::validateDeleteInventoryBeforeAdmission);
			metrics.refreshLastSnapshot(true);
			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(1, delete.verification().preValidationFailures());
			assertTrue(delete.verification().preValidationComplete());
			assertFalse(delete.verification().postVerificationComplete());
			assertTrue(delete.verification().postVerificationSkipped());
			assertTrue(delete.preValidationNanos() >= 0);
			assertEquals(-1, delete.scheduledDeleteNanos());
			assertEquals(-1, delete.postVerificationNanos());
			assertEquals(2, driver.presenceCalls.get());
			assertEquals(0, driver.scheduled.get());
		} finally {
			step.close();
			assertDoesNotThrow(step::validateTerminalState,
							"closed strict-pre evidence must retain its finalized completeness state");
			metrics.close();
		}
		assertEquals(2, driver.presenceCalls.get(), "failed pre-validation must not run post-verification");
	}

	@Test
	void distributedStrictPreValidationAbortSkipsPostOnLocallyPassingSlice(
					@TempDir final Path temp) throws Exception {
		final String stepId = "standalone-delete-distributed-strict-pre-abort-" + System.nanoTime();
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection,
						"bucket,key,size,version_id\n"
										+ "bucket,current,1,\n"
										+ "bucket,exact,1,version-1\n");
		final var config = config(2, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 2L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 1L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=2"));
		config.val("load-op-delete-preValidation", true);
		config.val("load-op-delete-postVerification", true);
		config.val("load-op-delete-verificationTimeoutMillis", 1L);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		driver.presence = ignored -> DeleteVerificationProbe.Presence.PRESENT;
		final var metrics = metrics();
		final List<Path> artifactPaths = List.of(
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_METRICS_TOTAL.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_REQUESTS.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_OBJECTS.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_RESIDUAL.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_VERIFICATION.getName(), stepId)));
		for (final Path artifact : artifactPaths) {
			Files.deleteIfExists(artifact);
		}
		final var step = new LoadStepContextImpl<>(
						stepId,
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"));
		try {
			step.holdObjectFailureBudgetAdmission();
			step.start();
			step.validateDeleteInventoryBeforeAdmission();
			step.skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure();
		} finally {
			step.close();
			metrics.close();
		}
		final var delete = metrics.lastSnapshot().deleteMetrics();
		assertEquals(0, delete.verification().preValidationFailures());
		assertTrue(delete.verification().preValidationComplete());
		assertFalse(delete.verification().postVerificationComplete());
		assertTrue(delete.verification().postVerificationSkipped());
		assertEquals(-1, delete.postVerificationNanos());
		assertEquals(2, driver.presenceCalls.get(),
						"a locally passing slice must honor the distributed strict-pre abort");
		assertDoesNotThrow(step::validateTerminalState);
		assertEquals(3, Files.readAllLines(artifactPaths.get(2)).size());
		assertEquals(3, Files.readAllLines(artifactPaths.get(3)).size());
		assertTrue(Files.readString(artifactPaths.get(4)).contains(",unattempted,true,present,true,unattempted,"));
		for (final Path artifact : artifactPaths) {
			Files.deleteIfExists(artifact);
		}
	}

	@Test
	void postVerificationJoinsEveryOperationalOutcomeAndPublishesPhaseTiming(
					@TempDir final Path temp) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection,
						"bucket,key,size,version_id\n"
										+ "bucket,key-0,0,\n"
										+ "bucket,key-1,1,version-1\n"
										+ "bucket,key-2,2,\n");
		final var config = config(3, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 3L);
		config.val("load-op-delete-selectedCurrentKey", 2L);
		config.val("load-op-delete-selectedExactVersion", 1L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=3"));
		config.val("load-op-delete-postVerification", true);
		config.val("load-op-delete-verificationTimeoutMillis", 1L);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		driver.presence = target -> switch (target.key()) {
		case "key-0" -> DeleteVerificationProbe.Presence.ABSENT;
		case "key-1" -> DeleteVerificationProbe.Presence.PRESENT;
		default -> DeleteVerificationProbe.Presence.UNRESOLVED;
		};
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-post-verify",
						generator(config, driver, new ManifestInput(3)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"),
						(ignoredId, ignoredSelection, ignoredCount) -> null);
		try {
			step.start();
			awaitDone(step);
			step.stop();
			metrics.refreshLastSnapshot(true);
			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(1, delete.verification().acceptedAbsent());
			assertEquals(1, delete.verification().acceptedPresent());
			assertEquals(1, delete.verification().acceptedUnresolved());
			assertEquals(2, delete.verification().correctnessFailures());
			assertEquals(1, delete.verification().inconclusiveFailures());
			assertEquals(DELETE_FAILURE_OUTCOME_FAILED, delete.failureOutcome());
			assertTrue(delete.postVerificationNanos() >= 0);
			final var verificationFailure = assertThrows(
							IntegrityTerminalException.class, step::validateTerminalState);
			assertTrue(verificationFailure.getMessage().contains("verification"));
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void constructorDoesNotPublishCallbacksBeforeArtifactRecorderSucceeds(
					@TempDir final Path temp) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nbucket,key,1,\n");
		final var config = config(1, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 1L);
		config.val("load-op-delete-selectedCurrentKey", 1L);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=1"));
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var metrics = metrics();
		final AtomicBoolean recorderAttempted = new AtomicBoolean();
		try {
			assertThrows(
							IllegalStateException.class,
							() -> new LoadStepContextImpl<>(
											"standalone-delete-rejected-recorder",
											generator(config, driver, new ManifestInput(1)),
											driver,
											metrics,
											null,
											config.configVal("load"),
											false,
											com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
											null,
											config.configVal("item"),
											(ignoredId, ignoredSelection, ignoredCount) -> {
												recorderAttempted.set(true);
												throw new IllegalStateException("injected recorder initialization failure");
											}));
			assertTrue(recorderAttempted.get());
			assertNull(driver.resultOutput, "rejected context escaped to the storage driver");
			assertFalse(
							metrics.metadata().containsKey(
											com.dell.spt.base.metrics.MetricsConstants.METADATA_DELETE_METRICS),
							"rejected context escaped through metrics metadata");
		} finally {
			metrics.close();
		}
	}

	@Test
	void realStepPersistsOneLogicalRequestRowEveryTargetAndOnlyResidualIdentities(
					@TempDir final Path temp) throws Exception {
		final String stepId = "standalone-delete-artifact-canary-" + System.nanoTime();
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(
						selection,
						"bucket,key,size,version_id\n"
										+ "bucket,key-0,0,\n"
										+ "bucket,key-1,1,\n"
										+ "bucket,key-2,2,\n"
										+ "bucket,key-3,3,\n");
		final var config = config(2, 10);
		config.val("item-input-file", selection.toString());
		config.val("load-op-delete-selected", 4L);
		config.val("load-op-delete-selectedCurrentKey", 4L);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=4"));
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var metrics = metrics();
		final List<Path> artifactPaths = List.of(
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_METRICS_TOTAL.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_REQUESTS.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_OBJECTS.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_RESIDUAL.getName(), stepId)),
						Path.of(FileManager.INSTANCE.logFileName(Loggers.DELETE_VERIFICATION.getName(), stepId)));
		for (final Path artifact : artifactPaths) {
			Files.deleteIfExists(artifact);
		}
		final var step = new LoadStepContextImpl<>(
						stepId,
						generator(config, driver, new ManifestInput(4)),
						driver,
						metrics,
						null,
						config.configVal("load"),
						false,
						com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.NO_OP,
						null,
						config.configVal("item"));

		try {
			step.start();
			awaitDone(step);
			step.stop();
			LogUtil.flushAll();

			assertEquals(2, Files.readAllLines(artifactPaths.get(1)).size() - 1);
			assertEquals(4, Files.readAllLines(artifactPaths.get(2)).size() - 1);
			final List<String> residual = Files.readAllLines(artifactPaths.get(3));
			assertEquals(2, residual.size());
			assertEquals("bucket,key-3,3,", residual.get(1));
			final String objects = Files.readString(artifactPaths.get(2));
			assertTrue(objects.contains(",key-0,0,,accepted,"));
			assertTrue(objects.contains(",key-3,3,,failed,"));
			assertFalse(objects.contains("start_us"), "target rows must not fabricate object timing");
			assertEquals(5, Files.readAllLines(artifactPaths.get(4)).size());
			assertEquals(2, Files.readAllLines(artifactPaths.get(0)).size());
		} finally {
			step.close();
			metrics.close();
			for (final Path artifact : artifactPaths) {
				Files.deleteIfExists(artifact);
			}
		}
	}

	@Test
	void batchSizeOneExecutesOneRequestAndOnePermitPerSelectedIdentity() throws Exception {
		final var config = config(1, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-size-one", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			assertEquals(2, driver.completed.size());
			assertTrue(driver.completed.stream().allMatch(operation -> operation.latency() > 0));
			assertTrue(driver.completed.stream().allMatch(operation -> operation.duration() > 0));
			assertTrue(driver.completed.stream()
							.allMatch(operation -> operation.deleteRequest().targets().size() == 1));
			assertEquals(2, step.operationLifecycle().dispatched());
			assertEquals(2, step.operationLifecycle().terminal());
			assertEquals(2, step.deleteObjectLifecycle().selected());
			assertEquals(2, step.deleteObjectLifecycle().accepted());
			assertEquals(2, step.deleteObjectLifecycle().fullSuccessfulRequests());
			assertTrue(step.deleteObjectLifecycle().reconciled());
			assertEquals(2, metrics.lastSnapshot().successSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void nativeTransportIntervalFeedsEstablishedRequestLatencyMetric() throws Exception {
		final var config = config(1, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.NATIVE_TRANSPORT_TIMING);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-native-transport-timing",
						generator(config, driver, new ManifestInput(1)),
						driver,
						metrics,
						config.configVal("load"),
						false);
		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			assertEquals(1, metrics.lastSnapshot().latencySnapshot().count());
			assertEquals(500.0, metrics.lastSnapshot().latencySnapshot().mean());
			assertEquals(500L, driver.completed.get(0).transportRequestLatency());
			assertEquals(0L, driver.completed.get(0).requestFirstByteTime(),
							"a direct transport-clock interval must not invent an epoch timestamp");
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void realStepExecutesBatchesAndPublishesRequestAndObjectOutcomes() throws Exception {
		final var config = config(2, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(4));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-canary", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			assertEquals(2, driver.completed.size());
			assertEquals(
							List.of(List.of("key-0", "key-1"), List.of("key-2", "key-3")),
							driver.completed.stream()
											.map(op -> op.deleteRequest().targets().stream().map(target -> target.key()).toList())
											.toList());
			assertEquals(
							List.of(DeleteRequestOutcome.FULL_SUCCESS, DeleteRequestOutcome.PARTIAL),
							driver.completed.stream().map(op -> op.deleteResult().outcome()).toList());
			assertTrue(driver.completed.stream().allMatch(operation -> operation.latency() > 0));
			assertTrue(driver.completed.stream().allMatch(operation -> operation.duration() > 0));
			assertEquals(
							List.of(DeleteFailureClassification.NONE, DeleteFailureClassification.OPERATIONAL),
							driver.completed.stream().map(op -> op.deleteResult().failureClassification()).toList());

			final var objects = step.deleteObjectLifecycle();
			assertEquals(4, objects.selected());
			assertEquals(4, objects.attempted());
			assertEquals(3, objects.accepted());
			assertEquals(1, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(0, objects.unresolved());
			assertEquals(0, objects.protocolFailed());
			assertTrue(objects.reconciled());
			assertEquals(2, step.operationLifecycle().dispatched());
			assertEquals(2, step.operationLifecycle().terminal());
			assertEquals(0, metrics.lastSnapshot().byteSnapshot().count());
			assertEquals(1, metrics.lastSnapshot().successSnapshot().count());
			assertEquals(1, metrics.lastSnapshot().failsSnapshot().count());
			assertEquals(2, metrics.lastSnapshot().latencySnapshot().count());
			assertEquals(2, metrics.lastSnapshot().durationSnapshot().count());
			final double firstByteLatencyMean = driver.completed.stream()
							.mapToLong(operation -> operation.responseFirstByteTime() - operation.requestFirstByteTime())
							.average()
							.orElseThrow();
			assertEquals(firstByteLatencyMean, metrics.lastSnapshot().latencySnapshot().mean(), 0.001);
			assertTrue(driver.completed.stream()
							.allMatch(operation -> operation.requestFirstByteTime() > operation.reqTimeStart()));
			assertTrue(driver.completed.stream()
							.allMatch(operation -> operation.responseFirstByteTime() - operation.requestFirstByteTime() > operation.latency()));
			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(2, delete.requestAttempted());
			assertEquals(1, delete.requestFullSuccess());
			assertEquals(1, delete.requestPartial());
			assertEquals(0, delete.requestFailed());
			assertEquals(4, delete.objectSelected());
			assertEquals(3, delete.objectAccepted());
			assertEquals(1, delete.objectFailed());
			assertEquals(2, delete.actualRequestCount());
			assertEquals(4, delete.actualObjectCount());
			assertEquals(2, delete.fullBatchCount());
			assertEquals(0, delete.partialBatchCount());
			assertEquals(4, delete.currentKeyCount());
			assertEquals(0, delete.exactVersionCount());
			assertEquals(1, delete.buckets().size());
			assertEquals("bucket", delete.buckets().get(0).bucket());
			assertEquals("batch", delete.mode());
			assertEquals("canonical", delete.selectionOrder());
			assertEquals("fixed", delete.failurePolicyMode());
			assertTrue(delete.reconciled());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void percentageFailureBudgetPublishesTheSharedWireValue() throws Exception {
		final var config = config(1, 10);
		config.val("load-op-failureBudget-mode", DELETE_FAILURE_POLICY_MODE_PERCENTAGE);
		config.val("load-op-failureBudget-maxFailurePercent", 100.0);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-percentage-policy",
						generator(config, driver, new ManifestInput(1)),
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			assertEquals(
							DELETE_FAILURE_POLICY_MODE_PERCENTAGE,
							metrics.lastSnapshot().deleteMetrics().failurePolicyMode());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void reorderedDispatchUsesSelectionBucketMappingWithoutInflatingGlobalTotals() throws Exception {
		final int retainedBucketCount = DeleteMetricsSnapshot.MAX_BUCKET_METRICS;
		final int overflowBucketCount = 5;
		final int bucketCount = retainedBucketCount + overflowBucketCount;
		final var config = config(1, 10);
		final var selectedBuckets = new ArrayList<String>();
		for (int i = 0; i < retainedBucketCount; i++) {
			selectedBuckets.add(String.format(Locale.ROOT, "bucket-%03d=1", i));
		}
		selectedBuckets.add(DeleteMetricsSnapshot.OVERFLOW_BUCKET + '=' + overflowBucketCount);
		config.val("load-op-delete-selected", (long) bucketCount);
		config.val("load-op-delete-selectedCurrentKey", (long) bucketCount);
		config.val("load-op-delete-selectedExactVersion", 0L);
		config.val("load-op-delete-selectedBuckets", selectedBuckets);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-bounded-buckets",
						generator(config, driver, new ReorderedBucketManifestInput(bucketCount)),
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(bucketCount, delete.objectSelected());
			assertEquals(bucketCount, delete.objectAttempted());
			assertEquals(bucketCount, delete.objectAccepted());
			assertEquals(retainedBucketCount + 1, delete.buckets().size());
			assertEquals(
							delete.objectSelected(),
							delete.buckets().stream().mapToLong(bucket -> bucket.selected()).sum());
			assertEquals(
							delete.objectAttempted(),
							delete.buckets().stream().mapToLong(bucket -> bucket.attempted()).sum());
			assertEquals(
							delete.objectAccepted(),
							delete.buckets().stream().mapToLong(bucket -> bucket.accepted()).sum());
			assertEquals(
							delete.objectFailed(),
							delete.buckets().stream().mapToLong(bucket -> bucket.failed()).sum());
			final var overflow = delete.buckets().get(delete.buckets().size() - 1);
			assertEquals(DeleteMetricsSnapshot.OVERFLOW_BUCKET, overflow.bucket());
			assertEquals(overflowBucketCount, overflow.selected());
			assertEquals(overflowBucketCount, overflow.attempted());
			assertEquals(overflowBucketCount, overflow.accepted());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void monotonicWorkflowStartSpansSetupInterphaseAndDeleteWithoutEnteringRequestTiming()
					throws Exception {
		final var config = config(1, 10);
		config.val("load-op-delete-seedMillis", 125L);
		final long workflowStartedNanos = DurationTime.monotonicEpochNanos();
		config.val("load-op-delete-workflowStartedEpochNanos", workflowStartedNanos);
		Thread.sleep(200);
		final long beforeDeleteStep = DurationTime.monotonicEpochNanos();
		final long independentlyObservedSetupAndInterphase = DurationTime.elapsedNanos(
						workflowStartedNanos, beforeDeleteStep);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(1));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-phase-canary",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			awaitDone(step);
			step.stop();
			metrics.refreshLastSnapshot(true);
			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(TimeUnit.MILLISECONDS.toNanos(125), delete.seedNanos());
			assertEquals(-1, delete.discoveryNanos());
			assertEquals(-1, delete.preValidationNanos());
			assertTrue(
							delete.totalWallNanos() >= independentlyObservedSetupAndInterphase,
							"total wall must span the gap between setup and DELETE step start");
			assertTrue(
							delete.totalWallNanos() > delete.seedNanos() + delete.scheduledDeleteNanos() + delete.drainNanos(),
							"total wall must use the independently measured workflow interval");
			assertEquals(1, metrics.lastSnapshot().latencySnapshot().count());
			assertEquals(1, metrics.lastSnapshot().durationSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void futureDistributedWorkflowBoundaryFallsBackToLocalDeleteStep() throws Exception {
		final var config = config(1, 10);
		config.val(
						"load-op-delete-workflowStartedEpochNanos",
						DurationTime.monotonicEpochNanos() + TimeUnit.HOURS.toNanos(1));
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(1));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-future-clock-skew",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			awaitDone(step);
			step.stop();
			metrics.refreshLastSnapshot(true);
			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertTrue(delete.totalWallNanos() >= 0, "future skew must not produce negative wall time");
			assertTrue(
							delete.totalWallNanos() < TimeUnit.MINUTES.toNanos(1),
							"a future shared boundary must fall back to the local DELETE-step start");
			assertTrue(
							delete.totalWallNanos() >= delete.scheduledDeleteNanos(),
							"clock-skew fallback must not make total wall shorter than scheduled DELETE");
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void delayedControllerReleaseAndDrainDoNotEnterDeleteRequestOrObjectRates() throws Exception {
		final var config = config(1, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.COMPLETE_DURING_DRAIN);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-delayed-release",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);
		step.holdObjectFailureBudgetAdmission();

		try {
			final long stepStartedAt = System.nanoTime();
			step.start();
			Thread.sleep(200);
			final long releaseStartedAt = System.nanoTime();
			assertTrue(
							DurationTime.elapsedNanos(stepStartedAt, releaseStartedAt) >= TimeUnit.MILLISECONDS.toNanos(150),
							"the admission hold must be independently observable");
			step.releaseObjectFailureBudgetAdmission();
			awaitScheduled(driver, 2);
			final long exhaustionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (step.schedulingExhaustionNanos().isEmpty()
							&& System.nanoTime() < exhaustionDeadline) {
				Thread.onSpinWait();
			}
			final long schedulingExhaustedAt = step.schedulingExhaustionNanos().orElseThrow();
			final long scheduledUpperBound = DurationTime.elapsedNanos(
							releaseStartedAt, schedulingExhaustedAt);
			assertTrue(scheduledUpperBound > 0);
			Thread.sleep(200);
			final long beforeDrainRelease = System.nanoTime();
			final long independentlyObservedDrain = DurationTime.elapsedNanos(
							schedulingExhaustedAt, beforeDrainRelease);
			assertTrue(
							independentlyObservedDrain >= TimeUnit.MILLISECONDS.toNanos(150),
							"the in-flight drain must be independently observable");
			step.stop();
			metrics.refreshLastSnapshot(true);

			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertTrue(
							delete.scheduledDeleteNanos() <= scheduledUpperBound,
							"scheduled DELETE must end no later than generator exhaustion");
			assertTrue(
							delete.drainNanos() >= independentlyObservedDrain,
							"time after scheduling exhaustion belongs to drain, not the rate clock");
			assertTrue(
							delete.totalWallNanos() - delete.scheduledDeleteNanos() - delete.drainNanos() >= TimeUnit.MILLISECONDS.toNanos(150),
							"controller admission hold must enter total wall without entering the rate clock");
			final double scheduledSeconds = delete.scheduledDeleteNanos()
							/ (double) TimeUnit.SECONDS.toNanos(1);
			assertTrue(scheduledSeconds > 0);
			assertEquals(
							delete.requestAttempted() / scheduledSeconds,
							delete.requestsPerSecond(),
							0.000_001);
			assertEquals(
							delete.objectAttempted() / scheduledSeconds,
							delete.objectsPerSecond(),
							0.000_001);
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void scheduledDeleteClockAcceptsNegativeNanoTimeValues() throws Exception {
		final var config = config(1, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-negative-clock",
						generator(config, driver, new ManifestInput(1)),
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			atomicBooleanField(step, "startedOnce").set(true);
			atomicLongField(step, "deleteScheduledStartedNanos").set(-100L);
			assertEquals(90L, step.currentScheduledDeleteNanos(-10L));

			atomicBooleanField(step, "operationAdmissionClosed").set(true);
			atomicLongField(step, "deleteAdmissionClosedNanos").set(-50L);
			assertEquals(50L, step.currentScheduledDeleteNanos(10L));
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void dispatchedCancellationWithoutCompletionFailsClosedAsUnresolvedAtDrainBound() throws Exception {
		final var config = config(3, 0, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var generator = generator(config, driver, new ManifestInput(3));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-unresolved", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			step.startDurationInterval(
							durationStartNanos, durationStartNanos + TimeUnit.SECONDS.toNanos(1));
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (driver.scheduled.get() == 0 && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());
			assertEquals(1, step.operationLifecycle().dispatched());
			step.stop();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(3, objects.selected());
			assertEquals(3, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(3, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(1, step.operationLifecycle().unresolved());
			final var failure = assertThrows(
							com.dell.spt.base.integrity.IntegrityTerminalException.class,
							step::validateTerminalState);
			assertTrue(failure.getMessage().contains("unresolved"));
			assertTrue(step.deletePhaseTiming().scheduledNanos() > 0);
			assertTrue(step.deletePhaseTiming().drainNanos() >= 0);
			assertTrue(step.deletePhaseTiming().drainNanos() < TimeUnit.SECONDS.toNanos(1));
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationDeadlineDrainsDispatchedCompletionIntoMeasuredOutcomes() throws Exception {
		final var config = config(2, 2, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.COMPLETE_DURING_DRAIN);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-duration-drain",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			step.startDurationInterval(
							durationStartNanos, durationStartNanos + TimeUnit.SECONDS.toNanos(2));
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (driver.scheduled.get() == 0 && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());

			final long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
			step.closeOperationAdmissionForStepStop();
			step.recoverQueuedOperationsForStepStop();
			step.drainDispatchedOperationsForStepStop(drainDeadline);
			assertTrue(driver.awaitCompletionThread());
			metrics.refreshLastSnapshot();

			assertDoesNotThrow(step::validateTerminalState);
			assertEquals(2, step.deleteObjectLifecycle().attempted());
			assertEquals(2, step.deleteObjectLifecycle().accepted());
			assertEquals(0, step.deleteObjectLifecycle().unresolved());
			assertEquals(1, metrics.lastSnapshot().successSnapshot().count());
			assertTrue(step.deletePhaseTiming().scheduledNanos() > 0);
			assertTrue(step.deletePhaseTiming().drainNanos() > 0);
			assertTrue(System.nanoTime() < drainDeadline);
		} finally {
			driver.releaseDrainCompletion();
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationDeadlineRejectsCompletionArrivingWhileRecoveryIsStillBlocked() throws Exception {
		final var config = config(2, 0, true);
		final var driver = new DeterministicDeleteDriver(
						DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-post-deadline-recovery",
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						config.configVal("load"),
						false);
		final AtomicReference<Throwable> recoveryFailure = new AtomicReference<>();
		final Thread recoveryThread = Thread.ofPlatform().unstarted(() -> {
			try {
				step.recoverQueuedOperationsForStepStop();
			} catch (final Throwable failure) {
				recoveryFailure.set(failure);
			}
		});

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			final long scheduledDeadlineNanos = DurationTime.deadlineAfter(
							durationStartNanos, TimeUnit.MILLISECONDS.toNanos(100));
			step.startDurationInterval(durationStartNanos, scheduledDeadlineNanos);
			final long dispatchDeadlineNanos = DurationTime.deadlineAfter(
							System.nanoTime(), TimeUnit.SECONDS.toNanos(5));
			while (driver.scheduled.get() == 0
							&& !DurationTime.deadlineReached(dispatchDeadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());

			step.closeOperationAdmissionForStepStop();
			recoveryThread.start();
			assertTrue(driver.awaitRecoveryEntered());
			while (!DurationTime.deadlineReached(scheduledDeadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			driver.releaseLateCompletion();
			assertTrue(driver.awaitCompletionThread());
			metrics.refreshLastSnapshot();

			assertEquals(0, step.operationLifecycle().terminal());
			assertEquals(1, step.operationLifecycle().unresolved());
			assertEquals(2, step.deleteObjectLifecycle().unresolved());
			assertEquals(0, metrics.lastSnapshot().successSnapshot().count());
		} finally {
			driver.releaseLateCompletion();
			driver.releaseRecovery();
			recoveryThread.join(TimeUnit.SECONDS.toMillis(5));
			step.close();
			metrics.close();
		}
		assertFalse(recoveryThread.isAlive());
		assertNull(recoveryFailure.get());
	}

	@Test
	void durationAwaitInvalidatesWhenLastScheduledRequestRemainsInFlight() throws Exception {
		final var config = config(2, 1, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.COMPLETE_DURING_DRAIN);
		final var metrics = metrics();
		final var context = new LoadStepContextImpl<>(
						"standalone-delete-duration-exhausted-in-flight",
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						config.configVal("load"),
						false);
		final var step = new MultiInputDeleteStep(
						config, mock(MetricsManager.class), context);

		try {
			step.start();
			step.startDurationInterval(TimeUnit.SECONDS.toNanos(1));
			awaitScheduled(driver);

			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> step.await(250, TimeUnit.MILLISECONDS));

			assertTrue(failure.getMessage().contains("inventory slice exhausted"));
			assertEquals(1, context.operationLifecycle().dispatched());
			assertEquals(0, context.operationLifecycle().terminal());
		} finally {
			driver.releaseDrainCompletion();
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationPublicStopBoundsRealMultiInputResourcesAndReconcilesEveryQueue() throws Exception {
		final var config = config(2, 1, true);
		final int contextCount = MULTI_INPUT_CONTEXT_COUNT;
		final var phaseConcurrency = new PhaseConcurrencyProbe(contextCount);
		final List<DeterministicDeleteDriver> drivers = new ArrayList<>(contextCount);
		final List<InterruptGatedManifestInput> inputs = new ArrayList<>(contextCount);
		final var generators = new ArrayList<LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation>>(
						contextCount);
		final List<MetricsContext<AllMetricsSnapshot>> metricsContexts = new ArrayList<>(contextCount);
		final var contexts = new ArrayList<LoadStepContextImpl<IntegrityManifestDataItem, DeleteRequestOperation>>(
						contextCount);
		for (int i = 0; i < contextCount; i++) {
			final var driver = new DeterministicDeleteDriver(
							DriverMode.COMPLETE_DURING_DRAIN, phaseConcurrency);
			final var input = new InterruptGatedManifestInput(1_000_000_000, 4);
			final var generator = generator(config, driver, input);
			final var metrics = metrics();
			drivers.add(driver);
			inputs.add(input);
			generators.add(generator);
			metricsContexts.add(metrics);
			contexts.add(new LoadStepContextImpl<>(
							"standalone-delete-duration-" + i,
							generator,
							driver,
							metrics,
							config.configVal("load"),
							false));
		}
		final var step = new MultiInputDeleteStep(
						config,
						mock(MetricsManager.class),
						contexts.toArray(LoadStepContext[]::new));

		try {
			step.start();
			step.startDurationInterval(TimeUnit.SECONDS.toNanos(1));
			for (int i = 0; i < contextCount; i++) {
				assertTrue(inputs.get(i).awaitSecondRead());
				awaitScheduled(drivers.get(i), 2);
			}

			final long stopStartedNanos = System.nanoTime();
			step.stop();

			assertTrue(step.isStopped());
			assertTrue(
							System.nanoTime() - stopStartedNanos < TimeUnit.SECONDS.toNanos(1),
							"multi-input stop exceeded the one step-wide drain budget");
			assertTrue(phaseConcurrency.maximumActive() > 1);
			assertEquals(contextCount, phaseConcurrency.maximumActive());
			assertEquals(0, phaseConcurrency.active());
			for (int i = 0; i < contextCount; i++) {
				final var driver = drivers.get(i);
				final var generator = generators.get(i);
				final var context = contexts.get(i);
				assertTrue(driver.awaitCompletionThread());
				assertDoesNotThrow(context::validateTerminalState);
				assertEquals(1_000_000_000, context.deleteObjectLifecycle().selected());
				assertEquals(4, context.deleteObjectLifecycle().attempted());
				assertEquals(999_999_996, context.deleteObjectLifecycle().unattempted());
				assertEquals(0, context.deleteObjectLifecycle().unresolved());
				assertEquals(2, context.operationLifecycle().terminal());
				assertEquals(0, context.operationLifecycle().unresolved());
				assertTrue(context.deleteObjectLifecycle().reconciled());
				assertEquals(4, generator.consumedItemCount());
				assertEquals(999_999_996, generator.aggregateUnattemptedItemCount());
				assertEquals(0, inputs.get(i).recoveryReadAttempts.get());
			}
		} finally {
			drivers.forEach(DeterministicDeleteDriver::releaseDrainCompletion);
			step.close();
			metricsContexts.forEach(MetricsContext::close);
		}
	}

	@Test
	void cancellationAccountsBillionIdentityUnreadSuffixWithoutReadingOrRetainingIt()
					throws Exception {
		final var config = config(2, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var input = new InterruptGatedManifestInput(1_000_000_000, 4);
		final var generator = generator(config, driver, input);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-unread", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			assertTrue(input.awaitSecondRead(), "generator should block before consuming the unread suffix");
			assertEquals(2, driver.scheduled.get());
			step.stop();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(1_000_000_000, objects.selected());
			assertEquals(4, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(999_999_996, objects.unattempted());
			assertEquals(4, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(0, step.operationLifecycle().unattempted());
			assertEquals(2, step.operationLifecycle().unresolved());
			assertEquals(4, generator.consumedItemCount());
			assertEquals(999_999_996, generator.aggregateUnattemptedItemCount());
			assertEquals(0, input.recoveryReadAttempts.get());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void frozenVersionSelectionSurvivesUnattemptedManifestSuffix() throws Exception {
		final var config = config(2, 0);
		config.val("load-op-delete-selected", 6L);
		config.val("load-op-delete-selectedCurrentKey", 3L);
		config.val("load-op-delete-selectedExactVersion", 3L);
		config.val("load-op-delete-selectedBuckets", List.of("bucket=6"));
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var input = new InterruptGatedManifestInput(6, 4);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-frozen-versions",
						generator(config, driver, input),
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			assertTrue(input.awaitSecondRead());
			step.stop();
			metrics.refreshLastSnapshot(true);

			final var delete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(6, delete.objectSelected());
			assertEquals(2, delete.objectUnattempted());
			assertEquals(3, delete.currentKeyCount());
			assertEquals(3, delete.exactVersionCount());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void drainWinsBlockedTerminalPublicationWithoutRecordingRequestOrObjectOutcome()
					throws Exception {
		final var config = config(2, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.BLOCK_TERMINAL_PUBLICATION);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-publication-race",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			assertTrue(driver.awaitOutputAcceptance(), "result output should accept before terminal commit");
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			metrics.refreshLastSnapshot(true);
			final var liveDelete = metrics.lastSnapshot().deleteMetrics();
			assertEquals(2, liveDelete.objectAttempted());
			assertEquals(2, liveDelete.actualObjectCount());
			assertTrue(liveDelete.objectsPerSecond() > 0);
			step.stop();
			driver.releaseTerminalPublication();
			assertTrue(driver.awaitCompletionThread(), "late terminal attempt should finish");
			metrics.refreshLastSnapshot(true);

			final var objects = step.deleteObjectLifecycle();
			assertEquals(2, objects.selected());
			assertEquals(2, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(2, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(0, step.operationLifecycle().terminal());
			assertEquals(1, step.operationLifecycle().unresolved());
			assertEquals(0, metrics.lastSnapshot().successSnapshot().count());
			assertEquals(0, metrics.lastSnapshot().failsSnapshot().count());
		} finally {
			driver.releaseTerminalPublication();
			step.close();
			metrics.close();
		}
	}

	@Test
	void transportAndProtocolFailuresReconcileEverySelectedObject() throws Exception {
		assertFailedObjectReconciliation(DriverMode.TRANSPORT_FAILURE, 0);
		assertFailedObjectReconciliation(DriverMode.PROTOCOL_DEFECT, 3);
	}

	@Test
	@SuppressWarnings("unchecked")
	void queuedCancellationAccountsEveryRetainedTargetAsUnattempted() {
		final var config = config(3, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation> generator = mock(LoadGenerator.class);
		when(generator.consumedItemCount()).thenReturn(3L);
		final var targets = List.of(
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-0", 1, null)),
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-1", 1, null)),
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-2", 1, null)));
		final var operation = new com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl(
						0,
						new com.dell.spt.base.item.op.deletion.DeleteRequest(
										"bucket", com.dell.spt.base.storage.Credential.NONE, targets));
		driver.lifecycle.generatorBuffered(operation);
		driver.lifecycle.unattempted(operation);
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-cancelled",
						generator,
						driver,
						mock(MetricsContext.class),
						config.configVal("load"),
						false);

		final var objects = step.deleteObjectLifecycle();
		assertEquals(3, objects.selected());
		assertEquals(0, objects.attempted());
		assertEquals(0, objects.accepted());
		assertEquals(0, objects.failed());
		assertEquals(3, objects.unattempted());
		assertEquals(0, objects.unresolved());
		assertTrue(objects.reconciled());
	}

	private static AtomicBoolean atomicBooleanField(
					final Object target, final String name) throws ReflectiveOperationException {
		final var field = target.getClass().getDeclaredField(name);
		field.setAccessible(true);
		return (AtomicBoolean) field.get(target);
	}

	private static AtomicLong atomicLongField(
					final Object target, final String name) throws ReflectiveOperationException {
		final var field = target.getClass().getDeclaredField(name);
		field.setAccessible(true);
		return (AtomicLong) field.get(target);
	}

	private static void assertFailedObjectReconciliation(
					final DriverMode mode, final long expectedProtocolFailures) throws Exception {
		final var config = config(3, 10);
		final var driver = new DeterministicDeleteDriver(mode);
		final var generator = generator(config, driver, new ManifestInput(3));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-" + mode.name().toLowerCase(Locale.ROOT),
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);
		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot(true);

			final var objects = step.deleteObjectLifecycle();
			assertEquals(3, objects.selected());
			assertEquals(3, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(3, objects.failed());
			assertEquals(expectedProtocolFailures, objects.protocolFailed());
			assertTrue(objects.reconciled());
			assertTrue(driver.completed.stream().allMatch(operation -> operation.latency() > 0));
			assertTrue(driver.completed.stream().allMatch(operation -> operation.duration() > 0));
			assertEquals(1, metrics.lastSnapshot().failsSnapshot().count());
			assertEquals(1, metrics.lastSnapshot().latencySnapshot().count());
			assertEquals(1, metrics.lastSnapshot().durationSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	private static Config config(final int deleteBatchSize, final int waitLimit) {
		return config(deleteBatchSize, waitLimit, false);
	}

	private static Config config(
					final int deleteBatchSize, final int waitLimit, final boolean durationMode) {
		final var config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-output-file", "");
		config.val("load-batch-size", 4);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", deleteBatchSize);
		config.val("load-op-delete-duration", durationMode);
		config.val("load-step-limit-time", durationMode ? "60s" : "0s");
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("load-op-limit-count", 0L);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", waitLimit);
		return config;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation> generator(
					final Config config,
					final DeterministicDeleteDriver driver,
					final Input<IntegrityManifestDataItem> input) {
		return new LoadGeneratorBuilderImpl()
						.authConfig(config.configVal("storage-auth"))
						.itemConfig(config.configVal("item"))
						.itemFactory((ItemFactory) new DataItemFactoryImpl<>())
						.itemType(ItemType.DATA)
						.loadConfig(config.configVal("load"))
						.itemInput(input)
						.loadOperationsOutput(driver)
						.originIndex(0)
						.build();
	}

	private static MetricsContext<AllMetricsSnapshot> metrics() {
		final MetricsContext<AllMetricsSnapshot> metrics = MetricsContextImpl.builder()
						.loadStepId("standalone-delete-canary")
						.opType(OpType.DELETE)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		metrics.start();
		return metrics;
	}

	private static void awaitDone(final LoadStepContextImpl<?, ?> step) throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!step.isDone() && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(step.isDone(), "standalone DELETE step should complete");
	}

	private static void awaitScheduled(final DeterministicDeleteDriver driver) {
		awaitScheduled(driver, 1);
	}

	private static void awaitScheduled(
					final DeterministicDeleteDriver driver, final int expectedCount) {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (driver.scheduled.get() < expectedCount && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertEquals(expectedCount, driver.scheduled.get());
	}

	private static final class PhaseConcurrencyProbe {
		private final CountDownLatch firstWaveEntered;
		private final AtomicInteger active = new AtomicInteger();
		private final AtomicInteger maximumActive = new AtomicInteger();

		private PhaseConcurrencyProbe(final int firstWaveSize) {
			firstWaveEntered = new CountDownLatch(firstWaveSize);
		}

		private void observe() {
			final int currentActive = active.incrementAndGet();
			maximumActive.accumulateAndGet(currentActive, Math::max);
			firstWaveEntered.countDown();
			try {
				assertTrue(
								firstWaveEntered.await(5, TimeUnit.SECONDS),
								"real local stop contexts were serialized below their configured bound");
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			} finally {
				active.decrementAndGet();
			}
		}

		private int active() {
			return active.get();
		}

		private int maximumActive() {
			return maximumActive.get();
		}
	}

	private static final class MultiInputDeleteStep extends LoadStepLocalBase {
		private MultiInputDeleteStep(
						final Config config,
						final MetricsManager metricsManager,
						final LoadStepContext<?, ?>... contexts) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			stepContexts.addAll(List.of(contexts));
		}

		@Override
		public String getTypeName() {
			return "standalone-delete-multi-input-test";
		}

		@Override
		protected void init() {
			// The real contexts are provided directly by this lifecycle integration canary.
		}
	}

	private static final class ManifestInput implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private int next;

		private ManifestInput(final int count) {
			this.count = count;
		}

		@Override
		public IntegrityManifestDataItem get() {
			return next < count
							? new IntegrityManifestDataItem("bucket", "key-" + next, next++, null)
							: null;
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = next;
			while (next < count && next - start < limit) {
				buffer.add(new IntegrityManifestDataItem("bucket", "key-" + next, next, null));
				next++;
			}
			return next - start;
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
		}

		@Override
		public void close() {}
	}

	private static final class ReorderedBucketManifestInput
					implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private int next;

		private ReorderedBucketManifestInput(final int count) {
			this.count = count;
		}

		@Override
		public IntegrityManifestDataItem get() {
			return next < count ? item(next++) : null;
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = next;
			while (next < count && next - start < limit) {
				buffer.add(item(next++));
			}
			return next - start;
		}

		private IntegrityManifestDataItem item(final int index) {
			final int bucketIndex = count - index - 1;
			return new IntegrityManifestDataItem(
							String.format(Locale.ROOT, "bucket-%03d", bucketIndex),
							"key-" + index,
							index,
							null);
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
		}

		@Override
		public void close() {}
	}

	private static final class InterruptGatedManifestInput
					implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private final int initialReadCount;
		private final CountDownLatch secondReadStarted = new CountDownLatch(1);
		private final AtomicInteger recoveryReadAttempts = new AtomicInteger();
		private volatile boolean recovering;
		private int next;

		private InterruptGatedManifestInput(final int count, final int initialReadCount) {
			this.count = count;
			this.initialReadCount = initialReadCount;
		}

		private boolean awaitSecondRead() throws InterruptedException {
			return secondReadStarted.await(5, TimeUnit.SECONDS);
		}

		@Override
		public IntegrityManifestDataItem get() {
			throw new AssertionError();
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			if (next >= initialReadCount && !recovering) {
				secondReadStarted.countDown();
				try {
					new CountDownLatch(1).await();
				} catch (final InterruptedException e) {
					recovering = true;
					com.github.akurilov.commons.lang.Exceptions.throwUnchecked(e);
				}
			}
			if (recovering) {
				recoveryReadAttempts.incrementAndGet();
				throw new AssertionError("cancellation recovery must not read the unread manifest suffix");
			}
			final var start = next;
			while (next < count && next - start < limit) {
				buffer.add(new IntegrityManifestDataItem("bucket", "key-" + next, next, null));
				next++;
			}
			return next - start;
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
			recovering = false;
		}

		@Override
		public void close() {}
	}

	private enum DriverMode {
		DEFAULT, HOLD, TRANSPORT_FAILURE, PROTOCOL_DEFECT, NATIVE_TRANSPORT_TIMING, BLOCK_TERMINAL_PUBLICATION, COMPLETE_DURING_DRAIN, COMPLETE_AFTER_DEADLINE_DURING_RECOVERY
	}

	private static final class DeterministicDeleteDriver extends AsyncRunnableBase
					implements StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>,
					DeleteVerificationProbe {
		private final OperationLifecycleTracker<DeleteRequestOperation> lifecycle = new OperationLifecycleTracker<>();
		private final DriverMode mode;
		private final AtomicInteger scheduled = new AtomicInteger();
		private final List<DeleteRequestOperation> completed = java.util.Collections.synchronizedList(new ArrayList<>());
		private final List<Thread> completionThreads = java.util.Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch outputAccepted = new CountDownLatch(1);
		private final CountDownLatch terminalPublicationRelease = new CountDownLatch(1);
		private final CountDownLatch drainCompletionRelease = new CountDownLatch(1);
		private final CountDownLatch lateCompletionRelease = new CountDownLatch(1);
		private final CountDownLatch recoveryEntered = new CountDownLatch(1);
		private final CountDownLatch recoveryRelease = new CountDownLatch(1);
		private final PhaseConcurrencyProbe phaseConcurrency;
		private final AtomicInteger presenceCalls = new AtomicInteger();
		private volatile Function<DeleteTarget, Presence> presence = ignored -> Presence.PRESENT;
		private volatile Output<DeleteRequestOperation> resultOutput;

		private DeterministicDeleteDriver(final DriverMode mode) {
			this(mode, null);
		}

		private DeterministicDeleteDriver(
						final DriverMode mode, final PhaseConcurrencyProbe phaseConcurrency) {
			this.mode = mode;
			this.phaseConcurrency = phaseConcurrency;
		}

		@Override
		public boolean put(final DeleteRequestOperation operation) {
			lifecycle.driverQueued(operation);
			lifecycle.explicitlyDispatched(operation);
			final int requestIndex = scheduled.getAndIncrement();
			if (mode == DriverMode.HOLD) {
				return true;
			}
			if (mode == DriverMode.BLOCK_TERMINAL_PUBLICATION) {
				completionThreads.add(Thread.ofVirtual().start(() -> complete(operation, requestIndex)));
				return true;
			}
			if (mode == DriverMode.COMPLETE_DURING_DRAIN) {
				completionThreads.add(Thread.ofVirtual().start(() -> {
					awaitUninterruptibly(drainCompletionRelease);
					complete(operation, requestIndex);
				}));
				return true;
			}
			if (mode == DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY) {
				completionThreads.add(Thread.ofVirtual().start(() -> {
					awaitUninterruptibly(lateCompletionRelease);
					complete(operation, requestIndex);
				}));
				return true;
			}
			return complete(operation, requestIndex);
		}

		@Override
		public Presence presence(final DeleteTarget target) {
			presenceCalls.incrementAndGet();
			return presence.apply(target);
		}

		private boolean complete(final DeleteRequestOperation operation, final int requestIndex) {
			operation.reset();
			operation.startRequest();
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
			if (mode == DriverMode.NATIVE_TRANSPORT_TIMING) {
				operation.recordTransportRequestTiming(1_000_000L, 1_500_000L);
			} else {
				operation.markRequestFirstByteSent();
			}
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			operation.finishRequest();
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			operation.markResponseFirstByteReceived();
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			operation.finishResponse();
			if (mode == DriverMode.TRANSPORT_FAILURE) {
				operation.completeDelete(DeleteTransportResult.failure(
								com.dell.spt.base.item.op.Operation.Status.FAIL_TIMEOUT,
								"injected timeout"));
			} else if (mode == DriverMode.PROTOCOL_DEFECT) {
				operation.completeDelete(new DeleteTransportResult(
								List.of(DeleteTransportTargetResult.succeeded(
												operation.deleteRequest().targets().get(0))),
								null,
								null));
			} else if (requestIndex == 0 || operation.deleteRequest().targets().size() == 1) {
				operation.completeDelete(DeleteTransportResult.success(operation.deleteRequest().targets()));
			} else {
				final var targets = operation.deleteRequest().targets();
				operation.completeDelete(new DeleteTransportResult(
								List.of(
												DeleteTransportTargetResult.failed(targets.get(1), "injected failure"),
												DeleteTransportTargetResult.succeeded(targets.get(0))),
								null,
								null));
			}
			if (!lifecycle.completionStarted(operation)) {
				return false;
			}
			final var result = operation.result();
			completed.add(result);
			final boolean accepted = resultOutput.put(result);
			if (mode == DriverMode.BLOCK_TERMINAL_PUBLICATION) {
				outputAccepted.countDown();
				awaitUninterruptibly(terminalPublicationRelease);
			}
			if (accepted) {
				lifecycle.terminal(operation);
			}
			return accepted;
		}

		private boolean awaitOutputAcceptance() throws InterruptedException {
			return outputAccepted.await(5, TimeUnit.SECONDS);
		}

		private void releaseTerminalPublication() {
			terminalPublicationRelease.countDown();
		}

		private void releaseDrainCompletion() {
			drainCompletionRelease.countDown();
		}

		private void releaseLateCompletion() {
			lateCompletionRelease.countDown();
		}

		private boolean awaitRecoveryEntered() throws InterruptedException {
			return recoveryEntered.await(5, TimeUnit.SECONDS);
		}

		private void releaseRecovery() {
			recoveryRelease.countDown();
		}

		private boolean awaitCompletionThread() throws InterruptedException {
			for (final Thread thread : List.copyOf(completionThreads)) {
				thread.join(TimeUnit.SECONDS.toMillis(5));
				if (thread.isAlive()) {
					return false;
				}
			}
			return true;
		}

		private static void awaitUninterruptibly(final CountDownLatch latch) {
			var interrupted = false;
			while (true) {
				try {
					latch.await();
					break;
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public int put(final List<DeleteRequestOperation> operations, final int from, final int to) {
			var i = from;
			while (i < to && put(operations.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<DeleteRequestOperation> operations) {
			return put(operations, 0, operations.size());
		}

		@Override
		public void operationResultOutput(final Output<DeleteRequestOperation> output) {
			resultOutput = output;
		}

		@Override
		public List<IntegrityManifestDataItem> list(
						final ItemFactory<IntegrityManifestDataItem> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final IntegrityManifestDataItem lastPrevItem,
						final int count) {
			return List.of();
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return 4;
		}

		@Override
		public boolean supportsStandaloneDeleteRequests() {
			return true;
		}

		@Override
		public int activeOpCount() {
			return (int) lifecycle.inFlightCount();
		}

		@Override
		public long scheduledOpCount() {
			return scheduled.get();
		}

		@Override
		public long completedOpCount() {
			return completed.size();
		}

		@Override
		public boolean isIdle() {
			return lifecycle.inFlightCount() == 0;
		}

		@Override
		public OperationLifecycleTracker<DeleteRequestOperation> operationLifecycle() {
			return lifecycle;
		}

		@Override
		public List<DeleteRequestOperation> recoverQueuedOperations() {
			if (mode == DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY) {
				recoveryEntered.countDown();
				awaitUninterruptibly(recoveryRelease);
			}
			return List.of();
		}

		@Override
		public void closeAdmission() {
			if (phaseConcurrency != null) {
				phaseConcurrency.observe();
			}
			releaseDrainCompletion();
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}
	}
}
