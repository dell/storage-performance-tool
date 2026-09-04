package com.dell.spt.base.load.step.local.context;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.io.IntegrityOperationManifestOutput;
import com.dell.spt.base.item.io.ItemInfoFileOutput;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationAssembler;
import com.dell.spt.base.item.op.OperationAssemblyResult;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.ListedObject;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorderImpl;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilder;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StandaloneDeletePreparable;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.rmi.RemoteException;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.inOrder;

/* Alot of the functionality from ItemInputFactoryTest is used here since need an ItemInputFactory */
public class LoadStepContextImplTest {
	private static final int LARGE_RECOVERY_BATCH_SIZE = 400_000;
	private static final Duration LARGE_RECOVERY_TIME_BOUND = Duration.ofSeconds(2);

	@TempDir
	Path tempDir;

	private static Path TMP_DIR_PATH = null;
	private LoadGeneratorBuilder generatorBuilder = null;
	private LoadGenerator generator = null;
	Input<Item> itemInput = null;
	Config testConfig = TestConfigBuilder.config();
	private int batchSize = 4096; // default value given in the schema
	final ItemType itemType = ItemType.valueOf(testConfig.stringVal("item-type").toUpperCase(Locale.ROOT));
	final ItemFactory<? extends Item> itemFactory = ItemType.getItemFactory(itemType);
	final DummyStorageDriverMock mockDriver = DummyStorageDriverMock.create();

	@BeforeEach
	public void setUp() throws InterruptedException, IllegalConfigurationException, IOException {
		initDirectory();

		testConfig.val("item-type", "data");
		testConfig.val("item-data-ranges-concat", null);
		testConfig.val("load-op-wait-finish", true);
		testConfig.val("load-op-wait-limit", 10);
		testConfig.val("load-op-retry", true);
		itemInput = createCSVItemInput();

		generatorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(mockDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		generator = generatorBuilder.build();
	}

	@Test
	public void loadStepRunTest() throws IOException {
		final Path itemOutputPath = createItemOutputFile();
		final Output<? extends Item> itemOutput = new ItemInfoFileOutput<>(itemOutputPath);
		String id = "test";

		final LoadStepContextImpl stepCtx = new LoadStepContextImpl<>(
						id, generator, mockDriver, null, testConfig.configVal("load"), false);

		stepCtx.operationsResultsOutput(itemOutput);

		assertDoesNotThrow(() -> stepCtx.doStart());
		assertDoesNotThrow(() -> stepCtx.doShutdown());
		assertDoesNotThrow(() -> stepCtx.doClose());
		assertDoesNotThrow(() -> stepCtx.doStop());
		Assertions.assertTrue(stepCtx.isDone());
	}

	@Test
	void ordinaryContextDoesNotInvokeStandaloneDeletePreparation() throws Exception {
		testConfig.val("load-op-retry", false);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(
						StorageDriver.class,
						withSettings().extraInterfaces(StandaloneDeletePreparable.class));
		when(driver.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		doNothing().when(driver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		final var context = new LoadStepContextImpl<>(
						"ordinary-preparation-boundary",
						generator,
						driver,
						null,
						testConfig.configVal("load"),
						false);

		try {
			context.start();
			verify(driver).start();
			verify((StandaloneDeletePreparable) driver, never()).prepareStandaloneDelete();
			verify(generator).openAdmission();
			verify(generator).start();
		} finally {
			context.close();
		}
	}

	@Test
	void durationContextHoldsSchedulingUntilTheCommonIntervalIsArmed() throws Exception {
		testConfig.val("load-op-type", "delete");
		testConfig.val("load-op-delete-standalone", true);
		testConfig.val("load-op-delete-duration", true);
		testConfig.val("load-step-limit-time", "1s");
		testConfig.val("load-op-retry", false);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		when(driverMock.supportsStandaloneDeleteRequests()).thenReturn(true);
		when(driverMock.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		doNothing().when(driverMock).operationResultOutput(any());
		final MetricsContext metrics = buildMetricsCtx("duration-start-barrier");
		final var context = new LoadStepContextImpl<>(
						"duration-start-barrier",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);

		try {
			context.start();
			verify(driverMock).start();
			verify(generatorMock).holdAdmission();
			verify(generatorMock, never()).start();
			assertEquals(0, context.deletePhaseTiming().scheduledNanos());

			final long intervalStartNanos = System.nanoTime();
			final long intervalDeadlineNanos = intervalStartNanos + TimeUnit.MILLISECONDS.toNanos(20);
			context.startDurationInterval(intervalStartNanos, intervalDeadlineNanos);

			verify(generatorMock).openAdmissionUntil(intervalDeadlineNanos);
			verify(generatorMock).start();
			Thread.sleep(60);
			context.closeOperationAdmissionForStepStop();
			assertTrue(context.deletePhaseTiming().scheduledNanos() >= TimeUnit.MILLISECONDS.toNanos(10));
			assertTrue(
							context.deletePhaseTiming().scheduledNanos() < TimeUnit.MILLISECONDS.toNanos(40),
							"late admission closure was incorrectly reported as scheduled workload time");
		} finally {
			context.close();
			metrics.close();
		}
	}

	@Test
	void durationContextNeverReopensAdmissionAfterItsAbsoluteDeadline() throws Exception {
		testConfig.val("load-op-type", "delete");
		testConfig.val("load-op-delete-standalone", true);
		testConfig.val("load-op-delete-duration", true);
		testConfig.val("load-step-limit-time", "1s");
		testConfig.val("load-op-retry", false);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		when(driverMock.supportsStandaloneDeleteRequests()).thenReturn(true);
		when(driverMock.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		doNothing().when(driverMock).operationResultOutput(any());
		final MetricsContext metrics = buildMetricsCtx("duration-expired-start");
		final var context = new LoadStepContextImpl<>(
						"duration-expired-start",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);

		try {
			context.start();
			final long deadlineNanos = System.nanoTime();
			final long startNanos = deadlineNanos - TimeUnit.MILLISECONDS.toNanos(1);

			final var failure = assertThrows(
							IllegalStateException.class,
							() -> context.startDurationInterval(startNanos, deadlineNanos));

			assertTrue(failure.getMessage().contains("deadline"));
			verify(generatorMock, never()).openAdmissionUntil(anyLong());
			verify(generatorMock, never()).start();
		} finally {
			context.close();
			metrics.close();
		}
	}

	@Test
	public void shutdownClosesAdmissionBeforeRecoveringAndMarksOnlyDispatchedWorkUnresolved()
					throws Exception {
		testConfig.val("load-op-retry", false);
		testConfig.val("load-op-wait-finish", true);
		testConfig.val("load-op-wait-limit", 0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		final var lifecycle = new OperationLifecycleTracker<Operation<DataItem>>();
		when(driver.operationLifecycle()).thenReturn(lifecycle);
		doNothing().when(driver).operationResultOutput(any());
		final var generatorBuffered = baseDataOp("generator-buffered", 1);
		final var driverQueued = baseDataOp("driver-queued", 1);
		final var dispatched = baseDataOp("dispatched", 1);
		lifecycle.generatorBuffered(generatorBuffered);
		lifecycle.generatorBuffered(driverQueued);
		lifecycle.driverQueued(driverQueued);
		lifecycle.generatorBuffered(dispatched);
		lifecycle.driverQueued(dispatched);
		lifecycle.dispatched(dispatched);
		when(generator.recoverBufferedOperations()).thenReturn(List.of(generatorBuffered));
		when(driver.recoverQueuedOperations()).thenReturn(List.of(driverQueued));
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		final var context = new LoadStepContextImpl<>(
						"lossless-stop", generator, driver, metrics, testConfig.configVal("load"), false);

		context.doShutdown();

		final var ordered = inOrder(generator, driver);
		ordered.verify(driver).closeAdmission();
		ordered.verify(generator).closeAdmission();
		ordered.verify(generator).recoverBufferedOperations();
		ordered.verify(driver).recoverQueuedOperations();
		ordered.verify(driver).shutdown();
		final var snapshot = context.operationLifecycle();
		assertEquals(2, snapshot.unattempted());
		assertEquals(1, snapshot.unresolved());
		assertEquals(OperationLifecycleState.UNATTEMPTED, generatorBuffered.lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, driverQueued.lifecycle().state());
		assertEquals(OperationLifecycleState.UNRESOLVED, dispatched.lifecycle().state());
	}

	@Test
	public void shutdownDrainsActualDispatchWithinConfiguredBound() throws Exception {
		testConfig.val("load-op-retry", false);
		testConfig.val("load-op-wait-finish", true);
		testConfig.val("load-op-wait-limit", 1);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		final var lifecycle = new OperationLifecycleTracker<Operation<DataItem>>();
		when(driver.operationLifecycle()).thenReturn(lifecycle);
		doNothing().when(driver).operationResultOutput(any());
		final var dispatched = baseDataOp("drained", 1);
		lifecycle.generatorBuffered(dispatched);
		lifecycle.driverQueued(dispatched);
		lifecycle.dispatched(dispatched);
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		final var context = new LoadStepContextImpl<>(
						"bounded-drain", generator, driver, metrics, testConfig.configVal("load"), false);
		final var completion = Thread.ofVirtual().start(() -> {
			try {
				Thread.sleep(50);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			lifecycle.completionStarted(dispatched);
			lifecycle.terminal(dispatched);
		});

		final long started = System.nanoTime();
		context.doShutdown();
		final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		completion.join();

		assertTrue(elapsedMillis < 1000, "drain should finish when the dispatched request completes");
		assertEquals(1, context.operationLifecycle().terminal());
		assertEquals(0, context.operationLifecycle().unresolved());
		assertEquals(OperationLifecycleState.TERMINAL, dispatched.lifecycle().state());
	}

	@Test
	public void putSingleOperationSuccessAndMetricsOutput() throws Exception {
		// enable recycle to populate latestSuccOpResultByItem map
		testConfig.val("load-op-recycle-mode", true);
		final MetricsContext metrics = buildMetricsCtx("putSingle");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"step-put-single", generator, mockDriver, metrics, testConfig.configVal("load"), false);

		// capture timing metrics to verify put(Operation) path hits outputTimingMetrics
		final CollectingOpOutput<DataItem> metricsOut = new CollectingOpOutput<>();
		stepCtx.operationsMetricsOutput(metricsOut);

		// SUCC operation with non-zero timing + bytes
		final DataOperation<DataItem> op = newSuccDataOp("item-1", 1024);
		assertTrue(stepCtx.put(op));
		// timing metrics should receive this op
		assertEquals(1, metricsOut.received.size());
	}

	@Test
	public void metadataManifestOutputFailureIsTerminalWhileOrdinaryOutputRemainsCompatible()
					throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		final MetricsContext metrics = buildMetricsCtx("manifestOutput");
		final Output<Operation<DataItem>> failingOutput = new ThrowingIoOpOutput<>();

		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> metadataDriver = mock(StorageDriver.class);
		when(metadataDriver.metadataIntegrityEnabled()).thenReturn(true);
		final var metadataContext = new LoadStepContextImpl<>(
						"metadata-output", generatorMock, metadataDriver, metrics,
						testConfig.configVal("load"), false);
		metadataContext.operationsResultsOutput(failingOutput);
		final var terminal = assertThrows(
						IntegrityTerminalException.class,
						() -> metadataContext.put(newSuccDataOp("metadata-item", 8)));
		assertEquals(IntegrityTerminalException.Category.PUBLICATION, terminal.category());

		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> ordinaryDriver = mock(StorageDriver.class);
		final var ordinaryContext = new LoadStepContextImpl<>(
						"ordinary-output", generatorMock, ordinaryDriver, metrics,
						testConfig.configVal("load"), false);
		ordinaryContext.operationsResultsOutput(failingOutput);
		assertDoesNotThrow(() -> ordinaryContext.put(newSuccDataOp("ordinary-item", 8)));
	}

	@Test
	public void putListRangeProcessesSubsetAndEmitsOmit() throws Exception {
		// enable recycle to exercise both branches and reuse generator.recycle calls
		testConfig.val("load-op-recycle-mode", true);
		final MetricsContext metrics = buildMetricsCtx("putRange");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"step-put-range", generator, mockDriver, metrics, testConfig.configVal("load"), false);

		final CollectingOpOutput<DataItem> resultsOut = new CollectingOpOutput<>();
		stepCtx.operationsResultsOutput(resultsOut);

		final List<Operation<DataItem>> ops = new ArrayList<>();
		// 0: SUCC
		ops.add(newSuccDataOp("item-s1", 256));
		// 1: OMIT should be forwarded to results output immediately
		final DataOperation<DataItem> omit = baseDataOp("item-omit", 128);
		omit.status(Operation.Status.OMIT);
		ops.add(omit);
		// 2: PENDING should recycle and not emit to results output
		final DataOperation<DataItem> pending = baseDataOp("item-pending", 64);
		pending.status(Operation.Status.PENDING);
		ops.add(pending);

		final int processed = stepCtx.put(ops, 0, ops.size());
		assertEquals(ops.size(), processed);
		// Only the OMIT op should be emitted to results output immediately
		assertEquals(1, resultsOut.received.size());
		assertEquals(omit, resultsOut.received.get(0));
	}

	/** Builds a mock {@link LoadGenerator} pre-stubbed to satisfy the constructor's retry validation. */
	@SuppressWarnings("unchecked")
	private static LoadGenerator<DataItem, Operation<DataItem>> mockRetryCapableGenerator() {
		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mock(LoadGenerator.class);
		when(mockGenerator.supportsRetry()).thenReturn(true);
		doNothing().when(mockGenerator).recycle(any());
		doNothing().when(mockGenerator).retry(any());
		// Mockito does not honor LoadGenerator#isNothingPendingRetry's own (true) default
		// interface method for a plain mock - an unstubbed boolean-returning method
		// returns false, which would make every test that reaches doShutdown()/doStop()
		// pay awaitRetryQueueDrained()'s full bounded wait for no reason. Tests
		// specifically about that wait behavior override this stub themselves.
		when(mockGenerator.isNothingPendingRetry()).thenReturn(true);
		return mockGenerator;
	}

	/**
	 * A scheduler that runs the retry task synchronously, ignoring the requested delay, so
	 * tests don't wait on real backoff timers and aren't subject to timing flakiness under
	 * CI load. {@link LoadStepContextImpl#retryBackoffMillis(int)} is covered separately
	 * (see {@link #retryBackoffMillisStaysWithinConfiguredBounds()}).
	 */
	private static LoadStepContextImpl.RetryScheduler synchronousRetryScheduler() {
		return (delayMillis, task) -> {
			task.run();
			return java.util.concurrent.CompletableFuture.completedFuture(null);
		};
	}

	@Test
	public void putSuccessfulOperationClearsOpRetryCountThroughLoadStepContextPutNotJustDirectly() {
		// Test gap: OperationResultCopyTest proves OperationImpl.resetOpRetryCount() itself
		// zeroes the field, but not that LoadStepContextImpl.put() actually calls it on the
		// SUCC path. Uses a real op (not a mock) that already has a nonzero count, exactly
		// as it would after failing at least once before eventually succeeding.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-recycle-mode", false);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("succ-clears-retry-count");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"succ-clears-retry-count-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);

		final DataOperation<DataItem> op = newSuccDataOp("item-succ-after-retry", 64);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		assertEquals(2, op.opRetryCount());

		assertTrue(stepCtx.put((Operation<DataItem>) op));
		assertEquals(0, op.opRetryCount(), "LoadStepContextImpl.put() must clear opRetryCount on a SUCC result");
	}

	@Test
	public void batchPutSuccessfulOperationClearsOpRetryCount() {
		// Test gap: the single-op SUCC path's opRetryCount reset (test above) is not
		// automatically proof the batch put(List, from, to) path does the same - it's a
		// separate branch in production code (see put(List, int, int)'s own SUCC handling).
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-recycle-mode", false);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("batch-succ-clears-retry-count");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"batch-succ-clears-retry-count-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);

		final DataOperation<DataItem> op = newSuccDataOp("item-batch-succ-after-retry", 64);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		assertEquals(2, op.opRetryCount());
		final List<Operation<DataItem>> batch = new ArrayList<>();
		batch.add((Operation<DataItem>) op);

		assertEquals(1, stepCtx.put(batch, 0, 1));
		assertEquals(0, op.opRetryCount(), "batch put() must clear opRetryCount on a SUCC result");
	}

	@Test
	public void pendingResultRoutesThroughRetryForNonRecycleModeRetryEnabledWorkload() {
		// P2b (round 4): LoadGeneratorImpl only drains recycleQueue when *its own*
		// recycleFlag is set (true recycle-mode) - for a non-recycle-mode workload, a
		// PENDING result recycled via generator.recycle() would be silently stranded
		// forever while counterResults advances as if it had actually resolved. With
		// load-op-retry enabled, PENDING routes through the dedicated, always-drained
		// retry path instead, and does not count as resolved until it actually is.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-recycle-mode", false);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("pending-routes-to-retry");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"pending-routes-to-retry-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);

		final DataOperation<DataItem> op = baseDataOp("item-pending-retry-route", 64);
		op.status(Operation.Status.PENDING);
		assertTrue(stepCtx.put((Operation<DataItem>) op));

		verify(mockGenerator, times(1)).retry(any());
		verify(mockGenerator, never()).recycle(any());
	}

	@Test
	public void pendingResultStillUsesRecycleForTrueRecycleModeWorkloadEvenWithRetryEnabled() {
		// Contrast with the test above: a true recycle-mode workload's recycle queue *is*
		// drained by the generator regardless of load-op-retry, so there's no stranding
		// risk there - existing recycle() + immediate counterResults behavior (duration-
		// based read-loop workloads rely on PENDING cycling back around) is unchanged.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-recycle-mode", true);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("pending-recycle-mode-unchanged");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"pending-recycle-mode-unchanged-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);

		final DataOperation<DataItem> op = baseDataOp("item-pending-recycle-mode", 64);
		op.status(Operation.Status.PENDING);
		assertTrue(stepCtx.put((Operation<DataItem>) op));

		verify(mockGenerator, times(1)).recycle(any());
		verify(mockGenerator, never()).retry(any());
	}

	@Test
	public void putFailedOperationWithRetryEnabledRecyclesUntilLimitThenCountsAsFailed() {
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 2);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"retry-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		final DataOperation<DataItem> op = baseDataOp("item-retry", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);

		// Attempt 1: 0 < retryLimit(2) -> retried synchronously, not yet counted as failed.
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, times(1)).retry(any());
		verify(metrics, never()).markFail();
		assertEquals(1, op.opRetryCount());

		// Attempt 2: 1 < retryLimit(2) -> retried again, still not failed.
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, times(2)).retry(any());
		verify(metrics, never()).markFail();
		assertEquals(2, op.opRetryCount());

		// Attempt 3: 2 >= retryLimit(2) -> limit reached, counted as failed, not retried again.
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, times(2)).retry(any());
		verify(metrics, times(1)).markFail();
		assertEquals(2, op.opRetryCount());
	}

	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.EnumSource(value = Operation.Status.class, names = {"FAIL_IO", "FAIL_TIMEOUT", "FAIL_UNKNOWN", "RESP_FAIL_UNKNOWN", "RESP_FAIL_SVC"
	})
	public void everyRetryableStatusIsActuallyRetriedWithinLimit(final Operation.Status status) {
		// Test gap: the positive retry-path test only exercised RESP_FAIL_SVC. Parameterize
		// over the full retryable set isRetryableStatus() defines, so a future edit to that
		// set can't silently narrow it without a test noticing.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"retryable-status-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		final DataOperation<DataItem> op = baseDataOp("item-" + status, 64);
		op.status(status);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, times(1)).retry(any());
		verify(metrics, never()).markFail();
		assertEquals(1, op.opRetryCount(), status + " should have been retried");
	}

	@Test
	public void isDoneStaysFalseWhileARetryIsStillInBackoff() throws Exception {
		// Test gap: nothing directly proved isDone() waits for a scheduled-but-not-yet-fired
		// retry, as opposed to happening to work only because the synchronous test scheduler
		// resolves everything immediately. Uses a real generator + a controllable scheduler
		// that captures the task without running it, so the retry is genuinely still
		// "in backoff" from isDone()'s perspective when checked.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final var flakyDriver = new FlakyThenSucceedsDriver(1, Operation.Status.RESP_FAIL_SVC);
		final var stepCtx = buildRealRetryStepCtx(1, flakyDriver, "isdone-during-backoff");
		final java.util.concurrent.atomic.AtomicReference<Runnable> capturedTask = new java.util.concurrent.atomic.AtomicReference<>();
		stepCtx.setRetryScheduler((delayMillis, task) -> {
			capturedTask.set(task);
			return new CompletableFuture<Void>();
		});

		stepCtx.start();
		try {
			final long deadline = System.currentTimeMillis() + 5_000;
			while (capturedTask.get() == null && System.currentTimeMillis() < deadline) {
				Thread.sleep(5);
			}
			assertTrue(capturedTask.get() != null, "the one op should have failed and scheduled a retry by now");
			// The retry is scheduled but deliberately not yet run - isDone() must not
			// consider the workload complete while it's still sitting in backoff.
			assertFalse(stepCtx.isDone(), "isDone() must stay false while a retry is still in its backoff delay");

			// Running the captured task only enqueues the redispatch into the generator's
			// own retry queue (LoadGenerator#retry) - draining it to the (real, if
			// synchronous here) driver and having that driver's result flow back through
			// put() all still happens asynchronously on the generator's own work loop, so
			// isDone() must be polled rather than asserted immediately afterward.
			capturedTask.get().run();
			final long resolvedDeadline = System.currentTimeMillis() + 5_000;
			while (!stepCtx.isDone() && System.currentTimeMillis() < resolvedDeadline) {
				Thread.sleep(5);
			}
			assertTrue(stepCtx.isDone(), "isDone() should become true once the (successful) retry resolves");
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void putFailedOperationWithNonRetryableStatusCountsAsFailedImmediatelyEvenWithinLimit() {
		// Finding: retry should not apply to permanent failures (auth, not-found, client
		// error, corruption, out-of-space) even when load-op-retry is on and the retry
		// budget hasn't been exhausted - matches minio-go/aws-sdk-cpp's own retry policies.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"non-retryable-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		for (final var status : new Operation.Status[]{
				Operation.Status.RESP_FAIL_AUTH, Operation.Status.RESP_FAIL_NOT_FOUND,
				Operation.Status.RESP_FAIL_CLIENT, Operation.Status.RESP_FAIL_CORRUPT,
				Operation.Status.RESP_FAIL_SPACE
		}) {
			final DataOperation<DataItem> op = baseDataOp("item-" + status, 64);
			op.status(status);
			assertTrue(stepCtx.put((Operation<DataItem>) op), status.toString());
			assertEquals(0, op.opRetryCount(), status + ": should never have been retried");
		}
		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(4)).markFail();
		verify(metrics, times(1)).markCorrupt();
	}

	@Test
	public void putFailedOperationWhenOperationDoesNotSupportRetryTrackingFailsImmediately() {
		// Finding: an Operation implementation that doesn't override opRetryCount()/
		// incrementOpRetryCount() (supportsOpRetryTracking() == false) must not be retried
		// forever by accident - it should fail fast, with a clear log, same as retry=false.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"untracked-op-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		@SuppressWarnings("unchecked")
		final DataOperation<DataItem> op = mock(DataOperation.class);
		when(op.status()).thenReturn(Operation.Status.RESP_FAIL_SVC);
		when(op.type()).thenReturn(OpType.CREATE);
		when(op.supportsOpRetryTracking()).thenReturn(false);
		// opRetryCount()/incrementOpRetryCount() left unstubbed -> default no-op behavior

		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(1)).markFail();
	}

	@Test
	public void putFailedOperationWithRetryDisabledCountsAsFailedImmediately() {
		testConfig.val("load-op-retry", false);

		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mock(LoadGenerator.class);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"no-retry-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);

		final DataOperation<DataItem> op = baseDataOp("item-no-retry", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);

		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(1)).markFail();
		assertEquals(0, op.opRetryCount());
	}

	@Test
	public void constructorRejectsRetryAgainstAGeneratorThatDoesNotSupportIt() {
		// Finding: MixedLoadGenerator.recycle() is a deliberate no-op (mixed mode handles
		// its own per-op-type recycling), so enabling load-op-retry against it would
		// silently drop failed operations - neither retried nor counted as failed. Fail
		// fast at construction instead.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mock(LoadGenerator.class);
		when(mockGenerator.supportsRetry()).thenReturn(false);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);

		Assertions.assertThrows(
						IllegalConfigurationException.class,
						() -> new LoadStepContextImpl<>(
										"unsupported-retry-step", mockGenerator, mockStorageDriver, metrics,
										testConfig.configVal("load"), false));
	}

	@Test
	public void constructorRejectsNegativeRetryLimit() {
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", -1);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);

		Assertions.assertThrows(
						IllegalConfigurationException.class,
						() -> new LoadStepContextImpl<>(
										"negative-retry-limit-step", mockGenerator, mockStorageDriver, metrics,
										testConfig.configVal("load"), false));
	}

	@Test
	public void constructorAcceptsZeroRetryLimitAsRetryDisabledInPractice() {
		// 0 is documented as "disables retry even though load-op-retry is true", not an
		// error - only negative values are rejected.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 0);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = assertDoesNotThrow(
						() -> new LoadStepContextImpl<>(
										"zero-retry-limit-step", mockGenerator, mockStorageDriver, metrics,
										testConfig.configVal("load"), false));
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		final DataOperation<DataItem> op = baseDataOp("item-zero-limit", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(1)).markFail();
	}

	@Test
	public void scheduledRetryFallsBackToFailureWhenGeneratorAlreadyStopped() throws Exception {
		// Finding #2/#6: the generator can self-stop on its own (e.g. it hit its configured
		// load-op-limit-count by dispatching every op at least once, even though several
		// then failed and are still awaiting retry) while a retry is sitting in its backoff
		// delay. Recycling into a generator that will never poll its queue again would
		// leave the operation stuck forever instead of resolving; the scheduled task must
		// notice and fail it immediately instead.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"gen-stopped-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.start();

		// Capture the scheduled task instead of running it immediately, so the generator's
		// self-stop can be simulated *after* scheduling but *before* the task actually runs
		// - exactly the race window findings #2/#6 describe.
		final java.util.concurrent.atomic.AtomicReference<Runnable> capturedTask = new java.util.concurrent.atomic.AtomicReference<>();
		stepCtx.setRetryScheduler((delayMillis, task) -> {
			capturedTask.set(task);
			return new CompletableFuture<Void>(); // never-completing handle; not run yet
		});

		final DataOperation<DataItem> op = baseDataOp("item-gen-stopped", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		assertEquals(1, op.opRetryCount());
		verify(mockGenerator, never()).retry(any()); // not yet - task hasn't run

		// Simulate the generator having stopped on its own in the meantime, then let the
		// (previously scheduled, still-pending) retry task actually run.
		when(mockGenerator.isStopped()).thenReturn(true);
		capturedTask.get().run();

		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(1)).markFail();
	}

	@Test
	public void cancelledPendingRetryOnStopResolvesToFailureNotSilentlyLost() throws Exception {
		// Finding #6: a step stopping/shutting down (e.g. duration expired) while a retry
		// is sitting in its backoff delay must resolve that operation to a definite outcome
		// immediately - not leave a stray timer that fires later into a torn-down driver, and
		// not silently drop it uncounted either.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);
		testConfig.val("load-op-wait-finish", false);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"cancel-pending-retry-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);
		stepCtx.start();

		// A scheduler whose "future" is a real, never-completing CompletableFuture - matching
		// what the real delayed executor would return for a retry still in its backoff. It
		// is deliberately never run and never completed by this test: the only way it can
		// resolve is via doStop()'s cancelPendingRetries() cancelling it, so if that logic is
		// missing or broken, markFail() below would never be invoked and the test fails clean
		// (no recycle() call is possible either, since a successful cancel() guarantees, per
		// the Future contract, that the task body - the only thing that would call it - never runs).
		stepCtx.setRetryScheduler((delayMillis, task) -> new CompletableFuture<Void>());

		final DataOperation<DataItem> op = baseDataOp("item-cancel-me", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		assertEquals(1, op.opRetryCount());
		verify(metrics, never()).markFail();

		stepCtx.doStop();

		verify(mockGenerator, never()).retry(any());
		verify(metrics, times(1)).markFail();
	}

	@Test
	public void cancelledRetryTaskDoesNotDoubleResolveIfItRunsAnyway() throws Exception {
		// P1b (round 4): CompletableFuture#cancel does not reliably mean "the task body
		// will never run" - for a future produced by runAsync(), a successful cancel(false)
		// can still race with a task that has already started running on its own executor
		// thread (cancelling the future does not interrupt or otherwise stop that thread).
		// The old cancelPendingRetries() treated a successful cancel() as sufficient proof
		// it was safe to resolve the operation itself; if the task then ran anyway, it
		// would resolve the *same* operation a second time (e.g. markFail() twice, or a
		// markFail() racing a retry() that could go on to succeed). Verifies the CAS-based
		// RetryHandle actually prevents this: even if the task runs after
		// cancelPendingRetries() already resolved it, the task's own tryClaim() must lose
		// and it must do nothing further.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);
		testConfig.val("load-op-wait-finish", false);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"cas-double-resolve-step", mockGenerator, mockStorageDriver, metrics,
						testConfig.configVal("load"), false);
		stepCtx.start();

		final java.util.concurrent.atomic.AtomicReference<Runnable> capturedTask = new java.util.concurrent.atomic.AtomicReference<>();
		stepCtx.setRetryScheduler((delayMillis, task) -> {
			capturedTask.set(task);
			// A never-completing future, same as the sibling test above: cancel(false)'s
			// return value doesn't drive the outcome here anymore (the CAS does), but it
			// still matters that this is a realistic "still pending" future.
			return new CompletableFuture<Void>();
		});

		final DataOperation<DataItem> op = baseDataOp("item-cas-double-resolve", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));
		assertEquals(1, op.opRetryCount());

		// Resolve it via the cancellation path first.
		stepCtx.doStop();
		verify(metrics, times(1)).markFail();
		verify(mockGenerator, never()).retry(any());

		// Now simulate the race this finding describes: the task runs anyway, despite
		// having already been "cancelled".
		capturedTask.get().run();

		// Must not have been resolved a second time.
		verify(metrics, times(1)).markFail();
		verify(mockGenerator, never()).retry(any());
	}

	@Test
	public void publicStopClosesAdmissionBeforeWaitingForInFlightRetryTask() throws Exception {
		// Force the exact ordering race: a retry task has already passed its shutdown check
		// and is paused immediately before retry(). Stop must close both admission gates
		// immediately, then may wait for this already-running task to settle.
		//
		// Necessary but not sufficient on its own: this only proves generator.retry() gets
		// *called* before stop() proceeds - it does not prove the (here, mocked, so
		// inherently uncheckable) generator actually drains what that call enqueues before
		// being stopped. shutdownDuringRetryDrainDoesNotStrandTheOperation() below proves
		// that stronger, complete property against a *real* LoadGeneratorImpl.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"public-stop-race-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.start();

		// The retry task's first act (per scheduleRetry()) is to remove itself from
		// pendingRetries, then check generator.isStopped() before deciding to call
		// retry(). Hooking isStopped() lets this test pin the task exactly at "already
		// past cancellation, about to call retry()" for as long as needed, deterministically
		// simulating the race instead of hoping real thread timing lines up.
		final java.util.concurrent.CountDownLatch taskReachedIsStoppedCheck = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.CountDownLatch releaseTask = new java.util.concurrent.CountDownLatch(1);
		when(mockGenerator.isStopped()).thenAnswer(invocation -> {
			taskReachedIsStoppedCheck.countDown();
			releaseTask.await(5, java.util.concurrent.TimeUnit.SECONDS);
			return false; // generator not stopped yet, from the task's point of view
		});

		// Run the scheduled task on a real background thread so it can genuinely race
		// against the test thread's call to stop() below, rather than running inline.
		stepCtx.setRetryScheduler((delayMillis, task) -> {
			final Thread taskThread = new Thread(task, "retry-task");
			taskThread.setDaemon(true);
			taskThread.start();
			return new CompletableFuture<Void>();
		});

		final DataOperation<DataItem> op = baseDataOp("item-public-stop-race", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<DataItem>) op));

		assertTrue(
						taskReachedIsStoppedCheck.await(5, java.util.concurrent.TimeUnit.SECONDS),
						"retry task should have started and reached its isStopped() check by now");

		// Call public stop() on its own thread. It blocks on the claimed retry task, but the
		// admission boundary must already be closed during that wait.
		final Thread stopperThread = new Thread(stepCtx::stop, "stopper");
		stopperThread.start();
		verify(mockGenerator, timeout(1_000)).closeAdmission();
		verify(mockStorageDriver, timeout(1_000)).closeAdmission();

		// Release the task: the mock still records the already-committed retry() call. A real
		// generator rejects it atomically as unattempted, covered by the generator gate test.
		releaseTask.countDown();
		stopperThread.join(5_000);
		assertFalse(stopperThread.isAlive(), "public stop() should have completed");
		verify(mockGenerator, timeout(1_000).times(1)).retry(any());
		verify(mockGenerator, timeout(1_000)).stop();
	}

	@Test
	public void batchAndSingleOpRetryPathsBothCallOnRetryNotOnRequeue() throws Exception {
		// Finding #11: the single-op and batch put() retry branches used different list-
		// shard callbacks (onRetry(shard, status) vs the less-informative onRequeue(shard))
		// for what represents the exact same "this op failed and is being retried" event.
		// Both now go through the shared handleFailedOp(), so both must call onRetry().
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 5);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("batch-retry-shard-callback");
		final ListShardMetricsRecorder mockRecorder = mock(ListShardMetricsRecorder.class);

		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"batch-retry-shard-step",
						(LoadGenerator<Item, Operation<Item>>) (LoadGenerator<?, ?>) mockGenerator,
						(StorageDriver<Item, Operation<Item>>) (StorageDriver<?, ?>) mockStorageDriver,
						metrics,
						testConfig.configVal("load"),
						false,
						mockRecorder);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		final ListShard shardSingle = new ListShard("single/", null, null, null);
		final ListOperationImpl<PathItemImpl> singleOp = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("single/"), null);
		singleOp.shard(shardSingle);
		singleOp.status(Operation.Status.RESP_FAIL_SVC);
		assertTrue(stepCtx.put((Operation<Item>) (Operation<?>) singleOp));
		verify(mockRecorder, times(1)).onRetry(shardSingle, Operation.Status.RESP_FAIL_SVC);
		verify(mockRecorder, never()).onRequeue(shardSingle);

		final ListShard shardBatch = new ListShard("batch/", null, null, null);
		final ListOperationImpl<PathItemImpl> batchOp = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("batch/"), null);
		batchOp.shard(shardBatch);
		batchOp.status(Operation.Status.RESP_FAIL_SVC);
		final List<Operation<Item>> batch = new ArrayList<>();
		batch.add((Operation<Item>) (Operation<?>) batchOp);
		assertEquals(1, stepCtx.put(batch, 0, 1));
		verify(mockRecorder, times(1)).onRetry(shardBatch, Operation.Status.RESP_FAIL_SVC);
		verify(mockRecorder, never()).onRequeue(shardBatch);
	}

	@Test
	public void batchPutRetriesUntilLimitThenCountsAsFailedSameAsSingleOp() throws Exception {
		// Finding #11: the batch retry branch itself was untested (only success/omit/pending
		// were covered). Mirrors putFailedOperationWithRetryEnabledRecyclesUntilLimitThenCountsAsFailed
		// but through put(List, from, to).
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 2);

		final LoadGenerator<DataItem, Operation<DataItem>> mockGenerator = mockRetryCapableGenerator();
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> mockStorageDriver = mock(StorageDriver.class);
		doNothing().when(mockStorageDriver).operationResultOutput(any());
		@SuppressWarnings("unchecked")
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		doNothing().when(metrics).markFail();

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"batch-retry-step", mockGenerator, mockStorageDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		final DataOperation<DataItem> op = baseDataOp("item-batch-retry", 64);
		op.status(Operation.Status.RESP_FAIL_SVC);
		final List<Operation<DataItem>> batch = new ArrayList<>();
		batch.add((Operation<DataItem>) op);

		assertEquals(1, stepCtx.put(batch, 0, 1));
		verify(mockGenerator, times(1)).retry(any());
		verify(metrics, never()).markFail();
		assertEquals(1, op.opRetryCount());

		op.status(Operation.Status.RESP_FAIL_SVC);
		assertEquals(1, stepCtx.put(batch, 0, 1));
		verify(mockGenerator, times(2)).retry(any());
		verify(metrics, never()).markFail();
		assertEquals(2, op.opRetryCount());

		op.status(Operation.Status.RESP_FAIL_SVC);
		assertEquals(1, stepCtx.put(batch, 0, 1));
		verify(mockGenerator, times(2)).retry(any());
		verify(metrics, times(1)).markFail();
		assertEquals(2, op.opRetryCount());
	}

	@Test
	public void retryBackoffMillisStaysWithinConfiguredBounds() {
		// Attempt 1 (base, unshifted): bounded by the 200ms base itself.
		for (int i = 0; i < 50; i++) {
			final long delay = LoadStepContextImpl.retryBackoffMillis(1);
			assertTrue(delay >= 0 && delay <= 200, "attempt 1 delay out of bounds: " + delay);
		}
		// A later attempt whose uncapped exponential value would exceed the 1s cap must still
		// be bounded by it (200ms * 2^4 = 3200ms > 1000ms cap).
		for (int i = 0; i < 50; i++) {
			final long delay = LoadStepContextImpl.retryBackoffMillis(5);
			assertTrue(delay >= 0 && delay <= 1000, "attempt 5 delay exceeded the cap: " + delay);
		}
		// A large attempt number must not overflow or go negative -- still capped at 1s.
		final long farDelay = LoadStepContextImpl.retryBackoffMillis(1000);
		assertTrue(farDelay >= 0 && farDelay <= 1000, "large attempt delay out of bounds: " + farDelay);
	}

	@Test
	public void doStopFlushesLatestResultsHandlesBackpressureAndPoisons() throws Exception {
		// configure recycle so that successful ops are retained for final flush
		testConfig.val("load-op-recycle-mode", true);
		final MetricsContext metrics = buildMetricsCtx("doStop");

		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"step-stop", generator, mockDriver, metrics, testConfig.configVal("load"), false);

		// set outputs: resultsOut is flaky to force backpressure path; metricsOut just collects
		final FlakyOpOutput<DataItem> resultsOut = new FlakyOpOutput<>(1);
		final CollectingOpOutput<DataItem> metricsOut = new CollectingOpOutput<>();
		stepCtx.operationsResultsOutput(resultsOut);
		stepCtx.operationsMetricsOutput(metricsOut);

		// produce a successful op which with recycle=true will be kept in latestSuccOpResultByItem
		final DataOperation<DataItem> op = newSuccDataOp("item-final", 777);
		assertTrue(stepCtx.put(op));

		// invoke stop; should
		// - wait (briefly) for activeOps (mock returns 0, so no long wait)
		// - flush latestSuccOpResultByItem into resultsOut, handling initial backpressure (false)
		// - poison both outputs
		assertDoesNotThrow(stepCtx::doStop);

		// ensure the op eventually made it through flaky output and poison was sent
		assertEquals(1, resultsOut.received.size());
		assertEquals(op, resultsOut.received.get(0));
		assertTrue(resultsOut.poisoned.get());
		assertTrue(metricsOut.poisoned.get());
	}

	@Test
	public void doStartFailsFastOnDriverRemoteException() throws Exception {
		// unrelated to retry; avoid the constructor's generator.supportsRetry() validation
		// tripping on this bare (unstubbed) mock
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		when(driverMock.start()).thenThrow(new RemoteException("driver-start"));
		final MetricsContext metrics = buildMetricsCtx("driverStartRemote");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> ctx = new LoadStepContextImpl<>(
						"ctx-driver-start",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);
		final IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class, ctx::doStart);
		Assertions.assertTrue(thrown.getCause() instanceof RemoteException);
	}

	@Test
	public void stoppedContextRejectsSameInstanceRestartInsteadOfStartingWithoutItsGenerator() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		final MetricsContext metrics = buildMetricsCtx("singleRunContext");
		final var ctx = new LoadStepContextImpl<>(
						"ctx-single-run", generatorMock, driverMock, metrics,
						testConfig.configVal("load"), false);

		ctx.start();
		ctx.stop();
		final var failure = assertThrows(IllegalStateException.class, ctx::start);

		assertTrue(failure.getMessage().contains("cannot be restarted"));
		assertTrue(ctx.isStopped(), "a rejected restart must preserve the stopped state");
		verify(generatorMock, times(1)).start();
		verify(driverMock, times(1)).start();
	}

	@Test
	public void doShutdownLogsAndContinuesOnDriverRemoteException() throws Exception {
		// unrelated to retry; avoid the constructor's generator.supportsRetry() validation
		// tripping on this bare (unstubbed) mock
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		// doShutdown() calls awaitRetryQueueDrained(), which polls this - an unstubbed
		// mock returns false (Mockito doesn't honor the interface's own true default),
		// which would otherwise make this pay that method's full bounded wait for nothing.
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		doThrow(new RemoteException("shutdown")).when(driverMock).shutdown();
		final MetricsContext metrics = buildMetricsCtx("shutdownRemote");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> ctx = new LoadStepContextImpl<>(
						"ctx-shutdown",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);
		assertDoesNotThrow(ctx::doShutdown);
	}

	@Test
	public void queuedRecoveryAttemptsBothSourcesAndRetriesOnlyTheFailedSource() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		final OperationLifecycleTracker<Operation<DataItem>> lifecycle = new OperationLifecycleTracker<>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		final MetricsContext metrics = buildMetricsCtx("independentQueuedRecovery");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> ctx = new LoadStepContextImpl<>(
						"ctx-independent-recovery",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);
		final Operation<DataItem> generatorBuffered = new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("generator-buffered", 0, 1),
						null, "bucket", null, List.of(), 0);
		final Operation<DataItem> driverQueued = new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("driver-queued", 0, 1),
						null, "bucket", null, List.of(), 0);
		lifecycle.generatorBuffered(generatorBuffered);
		lifecycle.generatorBuffered(driverQueued);
		lifecycle.driverQueued(driverQueued);
		when(generatorMock.recoverBufferedOperations())
						.thenThrow(new IllegalStateException("generator recovery failed"))
						.thenReturn(List.of(generatorBuffered));
		when(driverMock.recoverQueuedOperations()).thenReturn(List.of(driverQueued));

		final var failure = assertThrows(
						IllegalStateException.class, ctx::recoverQueuedOperationsForStepStop);
		assertTrue(failure.getMessage().contains("generator recovery failed"));
		verify(driverMock).recoverQueuedOperations();
		assertEquals(OperationLifecycleState.UNATTEMPTED, driverQueued.lifecycle().state());

		assertDoesNotThrow(ctx::recoverQueuedOperationsForStepStop);
		verify(generatorMock, times(2)).recoverBufferedOperations();
		verify(driverMock, times(1)).recoverQueuedOperations();
		assertEquals(OperationLifecycleState.UNATTEMPTED, generatorBuffered.lifecycle().state());
	}

	@Test
	public void queuedRecoveryRetainsDrainedBatchWhenLedgerUpdateFails() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		final OperationLifecycleTracker<Operation<DataItem>> lifecycle = mock(OperationLifecycleTracker.class);
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		final MetricsContext metrics = buildMetricsCtx("retainedQueuedRecovery");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> ctx = new LoadStepContextImpl<>(
						"ctx-retained-recovery",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);
		final Operation<DataItem> generatorBuffered = baseDataOp("generator-buffered", 1);
		final Operation<DataItem> driverQueued = baseDataOp("driver-queued", 1);
		when(generatorMock.recoverBufferedOperations())
						.thenReturn(List.of(generatorBuffered), List.of());
		when(driverMock.recoverQueuedOperations()).thenReturn(List.of(driverQueued));
		when(lifecycle.unattempted(generatorBuffered))
						.thenThrow(new IllegalStateException("generator ledger update failed"))
						.thenReturn(true);
		when(lifecycle.unattempted(driverQueued)).thenReturn(true);

		final var failure = assertThrows(
						IllegalStateException.class, ctx::recoverQueuedOperationsForStepStop);
		assertTrue(failure.getMessage().contains("generator ledger update failed"));
		verify(driverMock).recoverQueuedOperations();
		verify(lifecycle).unattempted(driverQueued);

		assertDoesNotThrow(ctx::recoverQueuedOperationsForStepStop);
		verify(generatorMock, times(1)).recoverBufferedOperations();
		verify(driverMock, times(1)).recoverQueuedOperations();
		verify(lifecycle, times(2)).unattempted(generatorBuffered);
		verify(lifecycle, times(1)).unattempted(driverQueued);
	}

	@Test
	void queuedRecoveryRetriesFailedTransitionsInOriginalOrder() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		final OperationLifecycleTracker<Operation<DataItem>> lifecycle = mock(OperationLifecycleTracker.class);
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		final Operation<DataItem> firstFailure = baseDataOp("first-failure", 1);
		final Operation<DataItem> success = baseDataOp("success", 1);
		final Operation<DataItem> secondFailure = baseDataOp("second-failure", 1);
		when(driverMock.recoverQueuedOperations())
						.thenReturn(List.of(firstFailure, success, secondFailure));
		final var attempts = new ArrayList<String>();
		final var firstAttempts = new AtomicInteger();
		final var secondAttempts = new AtomicInteger();
		doAnswer(invocation -> {
			final Operation<DataItem> operation = invocation.getArgument(0);
			if (operation == firstFailure) {
				attempts.add("first-failure");
				if (firstAttempts.getAndIncrement() == 0) {
					throw new IllegalStateException("first transition failed");
				}
			} else if (operation == secondFailure) {
				attempts.add("second-failure");
				if (secondAttempts.getAndIncrement() == 0) {
					throw new IllegalStateException("second transition failed");
				}
			} else {
				attempts.add("success");
			}
			return true;
		}).when(lifecycle).unattempted(any());
		final MetricsContext metrics = buildMetricsCtx("orderedQueuedRecovery");
		final var ctx = new LoadStepContextImpl<>(
						"ctx-ordered-queued-recovery",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);

		final var failure = assertThrows(
						IllegalStateException.class, ctx::recoverQueuedOperationsForStepStop);
		assertEquals("first transition failed", failure.getMessage());
		assertEquals(1, failure.getSuppressed().length);
		assertEquals("second transition failed", failure.getSuppressed()[0].getMessage());
		assertEquals(List.of("first-failure", "success", "second-failure"), attempts);

		attempts.clear();
		assertDoesNotThrow(ctx::recoverQueuedOperationsForStepStop);
		assertEquals(List.of("first-failure", "second-failure"), attempts);
	}

	@Test
	void largeQueuedRecoveryCompletesWithinBound() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		when(driverMock.operationLifecycle()).thenReturn(OperationLifecycleTracker.disabled());
		final Operation<DataItem> queued = baseDataOp("large-recovery-batch", 1);
		when(driverMock.recoverQueuedOperations())
						.thenReturn(Collections.nCopies(LARGE_RECOVERY_BATCH_SIZE, queued));
		final MetricsContext metrics = buildMetricsCtx("largeQueuedRecovery");
		final var ctx = new LoadStepContextImpl<>(
						"ctx-large-queued-recovery",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);

		assertTimeout(LARGE_RECOVERY_TIME_BOUND, ctx::recoverQueuedOperationsForStepStop);
	}

	@Test
	void queuedRecoveryTreatsANonThrowingFalseTransitionAsRecovered() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		final OperationLifecycleTracker<Operation<DataItem>> lifecycle = spy(new OperationLifecycleTracker<>());
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		final Operation<DataItem> alreadyRecovered = baseDataOp("already-recovered", 1);
		lifecycle.generatorBuffered(alreadyRecovered);
		lifecycle.driverQueued(alreadyRecovered);
		lifecycle.unattempted(alreadyRecovered);
		clearInvocations(lifecycle);
		when(driverMock.recoverQueuedOperations())
						.thenReturn(List.of(alreadyRecovered))
						.thenThrow(new IllegalStateException("recovery source invoked more than once"));
		final MetricsContext metrics = buildMetricsCtx("falseTransitionRecovery");
		final var ctx = new LoadStepContextImpl<>(
						"ctx-false-transition-recovery",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);

		assertDoesNotThrow(ctx::recoverQueuedOperationsForStepStop);
		assertDoesNotThrow(ctx::recoverQueuedOperationsForStepStop);
		verify(lifecycle, times(1)).unattempted(alreadyRecovered);
	}

	@Test
	public void queuedRecoveryCannotCrossAdmissionCloseStillInProgress() throws Exception {
		testConfig.val("load-op-retry", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
		when(generatorMock.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver<DataItem, Operation<DataItem>> driverMock = mock(StorageDriver.class);
		doNothing().when(driverMock).operationResultOutput(any());
		when(driverMock.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		final CountDownLatch driverCloseEntered = new CountDownLatch(1);
		final CountDownLatch releaseDriverClose = new CountDownLatch(1);
		doAnswer(invocation -> {
			driverCloseEntered.countDown();
			while (true) {
				try {
					releaseDriverClose.await();
					return null;
				} catch (final InterruptedException ignored) {
					// Model an extension which does not cooperate with cancellation.
				}
			}
		}).when(driverMock).closeAdmission();
		final MetricsContext metrics = buildMetricsCtx("admissionCloseBarrier");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> ctx = new LoadStepContextImpl<>(
						"ctx-admission-close-barrier",
						generatorMock,
						driverMock,
						metrics,
						testConfig.configVal("load"),
						false);
		final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
		final AtomicReference<Throwable> recoveryFailure = new AtomicReference<>();
		final Thread closeThread = Thread.ofPlatform().start(() -> {
			try {
				ctx.closeOperationAdmissionForStepStop();
			} catch (final Throwable failure) {
				closeFailure.set(failure);
			}
		});
		final Thread recoveryThread = Thread.ofPlatform().start(() -> {
			try {
				assertTrue(driverCloseEntered.await(1, TimeUnit.SECONDS));
				ctx.recoverQueuedOperationsForStepStop();
			} catch (final Throwable failure) {
				recoveryFailure.set(failure);
			}
		});
		try {
			assertTrue(driverCloseEntered.await(1, TimeUnit.SECONDS));
			Thread.sleep(100);
			verify(generatorMock, never()).recoverBufferedOperations();
			verify(driverMock, never()).recoverQueuedOperations();
		} finally {
			releaseDriverClose.countDown();
			closeThread.join(TimeUnit.SECONDS.toMillis(2));
			recoveryThread.join(TimeUnit.SECONDS.toMillis(2));
		}
		assertFalse(closeThread.isAlive());
		assertFalse(recoveryThread.isAlive());
		assertNull(closeFailure.get());
		assertNull(recoveryFailure.get());
	}

	@Test
	public void markListSuccessCountsObjectsWithoutTreatingLogicalSizeAsTransferredBytes() throws Exception {
		testConfig.val("load-op-recycle-mode", false);
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("listMetrics");
		final TrackingMetricsContext trackingCtx = new TrackingMetricsContext(metrics);
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"step-list", (LoadGenerator<Item, Operation<Item>>) generator,
						(DummyStorageDriverMock<Item, Operation<Item>>) (DummyStorageDriverMock) mockDriver,
						trackingCtx,
						testConfig.configVal("load"),
						false);

		final ListOperation<PathItemImpl> listOp = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("prefix"), null);
		listOp.status(Operation.Status.SUCC);
		listOp.objectsListed(17);
		listOp.bytesListed(3400);
		listOp.countBytesDone(3400);

		assertTrue(stepCtx.put((Operation) listOp));
		assertEquals(17L, trackingCtx.successCount.get());
		assertEquals(0L, trackingCtx.byteCount.get());
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"
	})
	void batchResultDoesNotCarryDataBytesIntoFollowingStandaloneDelete() throws Exception {
		testConfig.val("load-op-recycle-mode", false);
		final TrackingMetricsContext trackingCtx = new TrackingMetricsContext(buildMetricsCtx("delete-batch-zero-bytes"));
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"delete-batch-zero-bytes",
						(LoadGenerator<Item, Operation<Item>>) generator,
						(DummyStorageDriverMock<Item, Operation<Item>>) (DummyStorageDriverMock) mockDriver,
						trackingCtx,
						testConfig.configVal("load"),
						false);

		final DataOperation<DataItem> dataResult = newSuccDataOp("positive-byte-result", 73);
		final var target = new DeleteTarget(
						new IntegrityManifestDataItem("bucket", "delete-target", 41, "version-1"));
		final var deleteResult = new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", Credential.NONE, List.of(target)));
		deleteResult.completeDelete(
						com.dell.spt.base.item.op.deletion.DeleteTransportResult.success(List.of(target)));

		assertEquals(
						2,
						stepCtx.put(
										List.of(
														(Operation<Item>) (Operation<?>) dataResult,
														(Operation<Item>) (Operation<?>) deleteResult),
										0,
										2));
		assertEquals(73L, trackingCtx.byteCount.get());
		assertEquals(2L, trackingCtx.successCount.get());
	}

	@Test
	public void markListSuccessRecordsTimeToFirstByte() throws Exception {
		testConfig.val("load-op-recycle-mode", false);
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("listTtfb");
		final TrackingMetricsContext trackingCtx = new TrackingMetricsContext(metrics);
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"step-list-ttfb", (LoadGenerator<Item, Operation<Item>>) generator,
						(DummyStorageDriverMock<Item, Operation<Item>>) (DummyStorageDriverMock) mockDriver,
						trackingCtx,
						testConfig.configVal("load"),
						false);

		@SuppressWarnings("unchecked")
		final ListOperation<PathItemImpl> listOp = mock(ListOperation.class);
		when(listOp.status()).thenReturn(Operation.Status.SUCC);
		when(listOp.type()).thenReturn(OpType.LIST);
		when(listOp.duration()).thenReturn(200L);
		when(listOp.latency()).thenReturn(25L);
		when(listOp.reqTimeDone()).thenReturn(1_000L);
		when(listOp.respDataTimeStart()).thenReturn(1_123L);
		when(listOp.objectsListed()).thenReturn(3);
		when(listOp.bytesListed()).thenReturn(123L);
		when(listOp.options()).thenReturn(ListOptions.DEFAULT);

		assertTrue(stepCtx.put((Operation) listOp));
		assertEquals(3L, trackingCtx.successCount.get());
		assertEquals(0L, trackingCtx.byteCount.get());
		assertEquals(123L, trackingCtx.arrayTtfb.get());
	}

	@Test
	void readTtfbEqualToDurationIsRecorded() throws Exception {
		testConfig.val("load-op-recycle-mode", false);
		final TrackingMetricsContext trackingCtx = new TrackingMetricsContext(buildMetricsCtx("read-ttfb-equal"));
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"step-read-ttfb-equal", generator, mockDriver, trackingCtx, testConfig.configVal("load"), false);
		final DataItem item = new DataItemImpl("fast-read", 0, 1);
		@SuppressWarnings("unchecked")
		final DataOperation<DataItem> op = mock(DataOperation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.type()).thenReturn(OpType.READ);
		when(op.item()).thenReturn(item);
		when(op.duration()).thenReturn(100L);
		when(op.latency()).thenReturn(25L);
		when(op.dataLatency()).thenReturn(100L);
		when(op.countBytesDone()).thenReturn(1L);

		assertTrue(stepCtx.put((Operation<DataItem>) op));

		assertEquals(100L, trackingCtx.singleTtfb.get());
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"
	})
	void metadataListPublishesThreePagesExactlyOnceWithMatchingEmissionCount() throws Exception {
		final Config listConfig = TestConfigBuilder.config();
		listConfig.val("item-type", "path");
		listConfig.val("load-op-type", "list");
		listConfig.val("load-op-recycle-mode", true);
		listConfig.val("load-op-recycle-content-update", false);

		final LoadGenerator<Item, Operation<Item>> listGenerator = mock(LoadGenerator.class);
		final StorageDriver<Item, Operation<Item>> listDriver = mock(StorageDriver.class);
		when(listDriver.metadataIntegrityEnabled()).thenReturn(true);
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"metadata-list-three-pages",
						listGenerator,
						listDriver,
						buildMetricsCtx("metadata-list-three-pages"),
						listConfig.configVal("load"),
						false);
		final Path manifest = tempDir.resolve("verify-input.csv");
		final IntegrityOperationManifestOutput<Operation<Item>> output = new IntegrityOperationManifestOutput<>(manifest, "/bucket", OpType.LIST);
		stepCtx.operationsResultsOutput(output);
		final PathItemImpl seed = new PathItemImpl("prefix/");
		final ListOperationImpl<PathItemImpl> page = new ListOperationImpl<>(
						0, OpType.LIST, seed, null);
		page.status(Operation.Status.SUCC);
		final Operation<Item> result = (Operation<Item>) (Operation<?>) page;

		page.listedObjects(List.of(new ListedObject("prefix/a", 1, "a-v1")));
		page.objectsListed(1);
		page.truncated(true);
		page.continuationToken("page-2");
		assertTrue(stepCtx.put(result));

		page.listedObjects(List.of(new ListedObject("prefix/b", 2, "b-v1")));
		page.objectsListed(1);
		page.truncated(true);
		page.continuationToken("page-3");
		assertTrue(stepCtx.put(result));

		page.listedObjects(List.of(new ListedObject("prefix/c", 3, "c-v1")));
		page.objectsListed(1);
		page.truncated(false);
		page.continuationToken(null);
		assertTrue(stepCtx.put(result));
		output.close();

		assertEquals(
						List.of(
										"bucket,key,size,version_id",
										"bucket,prefix/a,1,a-v1",
										"bucket,prefix/b,2,b-v1",
										"bucket,prefix/c,3,c-v1"),
						Files.readAllLines(manifest));
		assertEquals(
						"3",
						Files.readString(com.dell.spt.base.integrity.IntegrityManifestCompletion
										.emissionCountPath(manifest)).trim());
		verify(listGenerator, times(2)).recycle(result);
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"
	})
	void ordinaryRecycledListRetainsOnlyIntermediatePageAndEmitsFinalPageOnce() throws Exception {
		final Config listConfig = TestConfigBuilder.config();
		listConfig.val("item-type", "path");
		listConfig.val("load-op-type", "list");
		listConfig.val("load-op-recycle-mode", true);
		listConfig.val("load-op-recycle-content-update", false);

		final LoadGenerator<Item, Operation<Item>> listGenerator = mock(LoadGenerator.class);
		final StorageDriver<Item, Operation<Item>> listDriver = mock(StorageDriver.class);
		when(listDriver.metadataIntegrityEnabled()).thenReturn(false);
		final Output<Operation<Item>> resultsOutput = mock(Output.class);
		when(resultsOutput.put(any(Operation.class))).thenReturn(true);
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"ordinary-list-pages",
						listGenerator,
						listDriver,
						buildMetricsCtx("ordinary-list-pages"),
						listConfig.configVal("load"),
						false);
		stepCtx.operationsResultsOutput(resultsOutput);
		final PathItemImpl seed = new PathItemImpl("prefix/");
		final ListOperationImpl<PathItemImpl> page = new ListOperationImpl<>(
						0, OpType.LIST, seed, null);
		page.status(Operation.Status.SUCC);
		page.objectsListed(1);
		page.truncated(true);
		final Operation<Item> result = (Operation<Item>) (Operation<?>) page;

		assertTrue(stepCtx.put(result));
		verify(resultsOutput, never()).put(result);
		final var retainedField = LoadStepContextImpl.class.getDeclaredField(
						"latestSuccOpResultByItem");
		retainedField.setAccessible(true);
		assertEquals(1, ((Map<?, ?>) retainedField.get(stepCtx)).size());

		page.truncated(false);
		assertTrue(stepCtx.put(result));
		verify(resultsOutput, times(1)).put(result);
		assertTrue(((Map<?, ?>) retainedField.get(stepCtx)).isEmpty());
		verify(listGenerator, times(1)).recycle(result);
	}

	@Test
	void metadataListPublishesTruncatedPageBeforeSingleResultRecycle() throws Exception {
		assertMetadataListPagePublished(false);
	}

	@Test
	void metadataListPublishesTruncatedPageBeforeBatchResultRecycle() throws Exception {
		assertMetadataListPagePublished(true);
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private void assertMetadataListPagePublished(final boolean batched) throws Exception {
		final Config listConfig = TestConfigBuilder.config();
		listConfig.val("item-type", "path");
		listConfig.val("load-op-type", "list");
		listConfig.val("load-op-recycle-mode", true);
		listConfig.val("load-op-recycle-content-update", false);

		final LoadGenerator<Item, Operation<Item>> listGenerator = mock(LoadGenerator.class);
		final StorageDriver<Item, Operation<Item>> listDriver = mock(StorageDriver.class);
		when(listDriver.metadataIntegrityEnabled()).thenReturn(true);
		final Output<Operation<Item>> resultsOutput = mock(Output.class);
		when(resultsOutput.put(any(Operation.class))).thenReturn(true);
		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"metadata-list-page",
						listGenerator,
						listDriver,
						buildMetricsCtx("metadata-list-page"),
						listConfig.configVal("load"),
						false);
		stepCtx.operationsResultsOutput(resultsOutput);

		final ListOperationImpl<PathItemImpl> page = new ListOperationImpl<>(
						0, OpType.LIST, new PathItemImpl("prefix/"), null);
		page.status(Operation.Status.SUCC);
		page.objectsListed(1000);
		page.truncated(true);
		page.continuationToken("next-page");
		final Operation<Item> result = (Operation<Item>) (Operation<?>) page;

		if (batched) {
			assertEquals(1, stepCtx.put(List.of(result), 0, 1));
		} else {
			assertTrue(stepCtx.put(result));
		}

		verify(resultsOutput).put(result);
		verify(listGenerator).recycle(result);
	}

	@Test
	public void listWorkloadCompletesOnceNamespaceExhausted() {
		final Config listConfig = TestConfigBuilder.config();
		listConfig.val("item-type", "path");
		listConfig.val("load-op-type", "list");
		listConfig.val("load-op-recycle-mode", false);

		@SuppressWarnings("unchecked")
		final LoadGenerator<Item, Operation<Item>> listGenerator = mock(LoadGenerator.class);
		@SuppressWarnings("unchecked")
		final StorageDriver<Item, Operation<Item>> listDriver = mock(StorageDriver.class);
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);

		doNothing().when(metrics).start();
		doNothing().when(metrics).close();
		doNothing().when(metrics).markSucc(anyLong(), anyLong(), any(long[].class), any(long[].class));
		when(metrics.lastSnapshot()).thenReturn(snapshot);
		when(snapshot.successSnapshot().count()).thenReturn(0L);
		when(snapshot.failsSnapshot().count()).thenReturn(0L);
		doNothing().when(listDriver).operationResultOutput(any());
		when(listDriver.activeOpCount()).thenReturn(0);

		final AtomicBoolean itemInputFinished = new AtomicBoolean(true);
		final AtomicBoolean recycleQueueEmpty = new AtomicBoolean(false);
		final AtomicLong generatedCount = new AtomicLong(0);

		when(listGenerator.isItemInputFinished()).thenAnswer(inv -> itemInputFinished.get());
		when(listGenerator.isNothingToRecycle()).thenAnswer(inv -> recycleQueueEmpty.get());
		when(listGenerator.generatedOpCount()).thenAnswer(inv -> generatedCount.get());
		doNothing().when(listGenerator).recycle(any());

		final LoadStepContextImpl<Item, Operation<Item>> stepCtx = new LoadStepContextImpl<>(
						"list-step",
						listGenerator,
						listDriver,
						metrics,
						listConfig.configVal("load"),
						false);

		final ListOperation<PathItemImpl> truncatedPage = new ListOperationImpl<>(
						0,
						OpType.LIST,
						new PathItemImpl("/"),
						null);
		truncatedPage.status(Operation.Status.SUCC);
		truncatedPage.objectsListed(1000);
		truncatedPage.bytesListed(0);
		truncatedPage.truncated(true);
		truncatedPage.continuationToken("token-1");

		assertTrue(stepCtx.put((Operation) truncatedPage));
		generatedCount.incrementAndGet();
		recycleQueueEmpty.set(false);

		final ListOperation<PathItemImpl> finalPage = new ListOperationImpl<>(
						0,
						OpType.LIST,
						new PathItemImpl("/"),
						null);
		finalPage.status(Operation.Status.SUCC);
		finalPage.objectsListed(237);
		finalPage.bytesListed(0);
		finalPage.truncated(false);
		finalPage.continuationToken(null);

		assertTrue(stepCtx.put((Operation) finalPage));
		generatedCount.incrementAndGet();
		recycleQueueEmpty.set(true);

		assertTrue(stepCtx.isDone());
	}

	@Test
	void listShardRecorderAggregatesEndToEndStats() throws Exception {
		final Config listConfig = TestConfigBuilder.config();
		listConfig.val("item-type", "path");
		listConfig.val("load-op-type", "list");
		listConfig.val("load-op-recycle-mode", false);
		listConfig.val("load-op-recycle-content-update", false);

		@SuppressWarnings("unchecked")
		final LoadGenerator<Item, Operation<Item>> listGenerator = mock(LoadGenerator.class);
		when(listGenerator.isItemInputFinished()).thenReturn(true);
		when(listGenerator.isNothingToRecycle()).thenReturn(true);
		when(listGenerator.generatedOpCount()).thenReturn(0L);
		doNothing().when(listGenerator).recycle(any());
		doNothing().when(listGenerator).close();
		// The try-with-resources block below closes stepCtx, which reaches doShutdown()'s
		// awaitRetryQueueDrained() - an unstubbed mock returns false (Mockito doesn't honor
		// the interface's own true default), which would otherwise make this pay that
		// method's full bounded wait for nothing.
		when(listGenerator.isNothingPendingRetry()).thenReturn(true);

		@SuppressWarnings("unchecked")
		final StorageDriver<Item, Operation<Item>> listDriver = (StorageDriver<Item, Operation<Item>>) (StorageDriver<?, ?>) DummyStorageDriverMock.create();

		final TrackingMetricsContext trackingMetrics = new TrackingMetricsContext(buildMetricsCtx("list-shards"));
		final var recorder = new ListShardMetricsRecorderImpl(Duration.ZERO, Duration.ZERO);

		try (final var stepCtx = new LoadStepContextImpl<>(
						"list-shards",
						listGenerator,
						listDriver,
						trackingMetrics,
						listConfig.configVal("load"),
						false,
						recorder)) {

			final ListOptions options = ListOptions.builder().maxKeys(1000).build();
			final ListShard shard = new ListShard("logs/", null, null, null);

			final ListOperationImpl<PathItemImpl> firstPage = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("logs/"), null);
			firstPage.shard(shard);
			firstPage.objectsListed(1000);
			firstPage.bytesListed(0);
			firstPage.truncated(true);
			firstPage.continuationToken("token-1");
			firstPage.options(options);
			firstPage.status(Operation.Status.SUCC);
			assertTrue(stepCtx.put((Operation) firstPage));

			final ListOperationImpl<PathItemImpl> finalPage = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("logs/"), null);
			finalPage.shard(firstPage.shard());
			finalPage.objectsListed(123);
			finalPage.bytesListed(0);
			finalPage.truncated(false);
			finalPage.options(options);
			finalPage.status(Operation.Status.SUCC);
			assertTrue(stepCtx.put((Operation) finalPage));

			final var snapshot = recorder.snapshot();
			assertEquals(2, snapshot.totalPages());
			assertEquals(1123, snapshot.totalObjects(), "objects=" + snapshot.totalObjects());
			assertEquals(1, snapshot.shards().size());
			final var shardSnapshot = snapshot.shards().iterator().next();
			assertEquals("logs/", shardSnapshot.prefix());
			assertEquals(2, shardSnapshot.pages());
			assertEquals(1123, shardSnapshot.objects());
			assertFalse(shardSnapshot.active());

			assertEquals(1123, trackingMetrics.successCount.get());
			assertEquals(0, trackingMetrics.byteCount.get());
		}
	}

	/* Helpers for tests */
	private MetricsContext<AllMetricsSnapshot> buildMetricsCtx(final String id) {
		final MetricsContext<AllMetricsSnapshot> ctx = MetricsContextImpl.builder()
						.loadStepId(id)
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		ctx.start();
		return ctx;
	}

	private static final class TrackingMetricsContext implements MetricsContext<AllMetricsSnapshot> {
		private final MetricsContext<AllMetricsSnapshot> delegate;
		final AtomicLong successCount = new AtomicLong();
		final AtomicLong byteCount = new AtomicLong();
		final AtomicLong singleTtfb = new AtomicLong();
		final AtomicLong arrayTtfb = new AtomicLong();

		TrackingMetricsContext(final MetricsContext<AllMetricsSnapshot> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Map metadata() {
			return delegate.metadata();
		}

		@Override
		public String loadStepId() {
			return delegate.loadStepId();
		}

		@Override
		public long runId() {
			return delegate.runId();
		}

		@Override
		public OpType opType() {
			return delegate.opType();
		}

		@Override
		public int concurrencyLimit() {
			return delegate.concurrencyLimit();
		}

		@Override
		public SizeInBytes itemDataSize() {
			return delegate.itemDataSize();
		}

		@Override
		public void markSucc(final long bytes, final long duration, final long latency) {
			successCount.incrementAndGet();
			byteCount.addAndGet(bytes);
			delegate.markSucc(bytes, duration, latency);
		}

		@Override
		public void markSucc(final long bytes, final long duration, final long latency, final long ttfb) {
			successCount.incrementAndGet();
			byteCount.addAndGet(bytes);
			singleTtfb.set(ttfb);
			delegate.markSucc(bytes, duration, latency, ttfb);
		}

		@Override
		public void markPartSucc(final long bytes, final long duration, final long latency) {
			delegate.markPartSucc(bytes, duration, latency);
		}

		@Override
		public void markPartSucc(final long bytes, final long duration, final long latency, final long ttfb) {
			delegate.markPartSucc(bytes, duration, latency, ttfb);
		}

		@Override
		public void markSucc(final long count, final long bytes, final long[] durationValues, final long[] latencyValues) {
			successCount.addAndGet(count);
			byteCount.addAndGet(bytes);
			delegate.markSucc(count, bytes, durationValues, latencyValues);
		}

		@Override
		public void markSucc(
						final long count,
						final long bytes,
						final long[] durationValues,
						final long[] latencyValues,
						final long[] ttfbValues) {
			successCount.addAndGet(count);
			byteCount.addAndGet(bytes);
			if (ttfbValues != null && ttfbValues.length > 0) {
				arrayTtfb.set(ttfbValues[0]);
			}
			delegate.markSucc(count, bytes, durationValues, latencyValues, ttfbValues);
		}

		@Override
		public void markPartSucc(final long bytes, final long[] durationValues, final long[] latencyValues) {
			delegate.markPartSucc(bytes, durationValues, latencyValues);
		}

		@Override
		public void markPartSucc(
						final long bytes,
						final long[] durationValues,
						final long[] latencyValues,
						final long[] ttfbValues) {
			delegate.markPartSucc(bytes, durationValues, latencyValues, ttfbValues);
		}

		@Override
		public void markFail() {
			delegate.markFail();
		}

		@Override
		public void markFail(final long count) {
			delegate.markFail(count);
		}

		@Override
		public void start() {
			delegate.start();
		}

		@Override
		public boolean isStarted() {
			return delegate.isStarted();
		}

		@Override
		public long startTimeStamp() {
			return delegate.startTimeStamp();
		}

		@Override
		public void refreshLastSnapshot() {
			delegate.refreshLastSnapshot();
		}

		@Override
		public AllMetricsSnapshot lastSnapshot() {
			return delegate.lastSnapshot();
		}

		@Override
		public int concurrencyThreshold() {
			return delegate.concurrencyThreshold();
		}

		@Override
		public boolean thresholdStateEntered() {
			return delegate.thresholdStateEntered();
		}

		@Override
		public void enterThresholdState() throws IllegalStateException {
			delegate.enterThresholdState();
		}

		@Override
		public boolean thresholdStateExited() {
			return delegate.thresholdStateExited();
		}

		@Override
		public MetricsContext thresholdMetrics() {
			return delegate.thresholdMetrics();
		}

		@Override
		public void exitThresholdState() throws IllegalStateException {
			delegate.exitThresholdState();
		}

		@Override
		public boolean stdOutColorEnabled() {
			return delegate.stdOutColorEnabled();
		}

		@Override
		public boolean avgPersistEnabled() {
			return delegate.avgPersistEnabled();
		}

		@Override
		public boolean sumPersistEnabled() {
			return delegate.sumPersistEnabled();
		}

		@Override
		public boolean timingPersistEnabled() {
			return delegate.timingPersistEnabled();
		}

		@Override
		public long outputPeriodMillis() {
			return delegate.outputPeriodMillis();
		}

		@Override
		public long lastOutputTs() {
			return delegate.lastOutputTs();
		}

		@Override
		public void lastOutputTs(final long ts) {
			delegate.lastOutputTs(ts);
		}

		@Override
		public long elapsedTimeMillis() {
			return delegate.elapsedTimeMillis();
		}

		@Override
		public String comment() {
			return delegate.comment();
		}

		@Override
		public void close() {
			delegate.close();
		}

		@Override
		public int compareTo(final MetricsContext<AllMetricsSnapshot> o) {
			return delegate.compareTo(o);
		}
	}

	/**
	 * A finite {@link Input} of {@link DataItem}s, signaling exhaustion the same way
	 * {@code CsvItemInput} does (throwing {@link EOFException} via the sneaky-throw
	 * {@code Exceptions.throwUnchecked}, since {@code Input.get(List, int)} declares no
	 * checked exception of its own).
	 */
	private static final class FixedCountItemInput implements Input<DataItem> {
		private final int total;
		private int produced;

		FixedCountItemInput(final int total) {
			this.total = total;
		}

		@Override
		public DataItem get() {
			if (produced >= total) {
				return null;
			}
			produced++;
			return new DataItemImpl("retry-it-item-" + produced, 0, 64);
		}

		@Override
		public int get(final List<DataItem> buffer, final int limit) {
			if (produced >= total) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new java.io.EOFException());
				return 0; // unreachable
			}
			int i = 0;
			while (i < limit && produced < total) {
				produced++;
				buffer.add(new DataItemImpl("retry-it-item-" + produced, 0, 64));
				i++;
			}
			return i;
		}

		@Override
		public long skip(final long itemsCount) {
			return 0;
		}

		@Override
		public void reset() {
			produced = 0;
		}

		@Override
		public void close() {}
	}

	/**
	 * Minimal real {@link StorageDriver} that fails an item's first {@code
	 * failuresBeforeSuccess} attempts (tracked per item name) with the given status, then
	 * succeeds - just enough real driver behavior to drive a genuine
	 * fail-then-retry-then-succeed cycle through the real {@code LoadGeneratorImpl}, without
	 * needing a full protocol implementation. Modeled on {@code DummyStorageDriverMock}.
	 */
	private static final class FlakyThenSucceedsDriver
					extends com.dell.spt.base.concurrent.AsyncRunnableBase
					implements StorageDriver<DataItem, Operation<DataItem>> {
		private final int failuresBeforeSuccess;
		private final Operation.Status failureStatus;
		// Keyed by item *identity*, not name: result() (see OperationImpl#result / its
		// buildItemPath() call) can rewrite an item's name in place - e.g. prepending a
		// path prefix - the first time it's copied, so the same logical item's name is not
		// stable across a retry's original attempt vs. its later copies. The item object
		// reference itself, however, is shared (copy-constructed, not cloned) all the way
		// through every result() copy in the chain, so identity is the stable key.
		private final IdentityHashMap<DataItem, Integer> attemptsByItem = new IdentityHashMap<>();
		private final Object attemptsByItemLock = new Object();
		private final AtomicInteger totalPutCalls = new AtomicInteger();
		private final AtomicInteger successCount = new AtomicInteger();
		private volatile Output<Operation<DataItem>> opResultOut;

		FlakyThenSucceedsDriver(final int failuresBeforeSuccess, final Operation.Status failureStatus) {
			this.failuresBeforeSuccess = failuresBeforeSuccess;
			this.failureStatus = failureStatus;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean put(final Operation<DataItem> op) {
			totalPutCalls.incrementAndGet();
			op.reset();
			op.startRequest();
			op.finishRequest();
			op.startResponse();
			final int attempt;
			synchronized (attemptsByItemLock) {
				attempt = attemptsByItem.merge(op.item(), 1, Integer::sum);
			}
			if (attempt <= failuresBeforeSuccess) {
				op.status(failureStatus);
			} else {
				if (op instanceof DataOperation<?>) {
					final DataOperation<DataItem> dataOp = (DataOperation<DataItem>) op;
					try {
						dataOp.countBytesDone(dataOp.item().size());
					} catch (final IOException ignored) {
						// test data always has a fixed size; not expected
					}
				}
				op.status(Operation.Status.SUCC);
				successCount.incrementAndGet();
			}
			op.finishResponse();
			// Mirror the real driver contract (e.g. StorageDriverBase.handleCompleted()):
			// send a result *copy* downstream, not the live op - LoadStepContextImpl only
			// ever sees copies in production, and retry/reset/recycle correctness (opRetryCount
			// surviving the copy, resetOpRetryCount() on success, etc.) depends on that.
			return opResultOut.put(op.result());
		}

		@Override
		public int put(final List<Operation<DataItem>> ops, final int from, final int to) {
			int i = from;
			while (i < to && put(ops.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops) {
			return put(ops, 0, ops.size());
		}

		@Override
		public void operationResultOutput(final Output<Operation<DataItem>> opResultOut) {
			this.opResultOut = opResultOut;
		}

		@Override
		public List<DataItem> list(
						final ItemFactory<DataItem> itemFactory, final String path, final String prefix,
						final int idRadix, final DataItem lastPrevItem, final int count) {
			return List.of();
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return 16;
		}

		@Override
		public int activeOpCount() {
			return 0; // every put() call above resolves synchronously - never "active"
		}

		@Override
		public long scheduledOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public long completedOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public boolean isIdle() {
			return true;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

		int totalPutCalls() {
			return totalPutCalls.get();
		}

		int successCount() {
			return successCount.get();
		}
	}

	/**
	 * Like {@link FlakyThenSucceedsDriver}, but genuinely asynchronous: {@code put()}
	 * returns immediately (as a real cooperative/Netty driver's {@code put()} does - it
	 * just enqueues, the actual request happens later on a different thread), and the
	 * result (success or the configured failure) is delivered on a separate thread after a
	 * short delay. That delay only needs to be long enough for the real {@code
	 * LoadGeneratorImpl} driving this to have already reached its "dispatched everything,
	 * item input exhausted" state before the completion arrives - a fully synchronous
	 * driver's completion (as {@link FlakyThenSucceedsDriver} above provides) can never
	 * arrive later than that point, so it can't exercise this timing at all.
	 */
	private static final class AsyncFlakyThenSucceedsDriver
					extends com.dell.spt.base.concurrent.AsyncRunnableBase
					implements StorageDriver<DataItem, Operation<DataItem>> {
		private final int failuresBeforeSuccess;
		private final Operation.Status failureStatus;
		private final long completionDelayMillis;
		private final IdentityHashMap<DataItem, Integer> attemptsByItem = new IdentityHashMap<>();
		private final Object attemptsByItemLock = new Object();
		private final AtomicInteger totalPutCalls = new AtomicInteger();
		private final AtomicInteger successCount = new AtomicInteger();
		private volatile Output<Operation<DataItem>> opResultOut;

		AsyncFlakyThenSucceedsDriver(
						final int failuresBeforeSuccess, final Operation.Status failureStatus, final long completionDelayMillis) {
			this.failuresBeforeSuccess = failuresBeforeSuccess;
			this.failureStatus = failureStatus;
			this.completionDelayMillis = completionDelayMillis;
		}

		@Override
		public boolean put(final Operation<DataItem> op) {
			totalPutCalls.incrementAndGet();
			final Thread completionThread = new Thread(
							() -> {
								try {
									Thread.sleep(completionDelayMillis);
								} catch (final InterruptedException e) {
									Thread.currentThread().interrupt();
									return;
								}
								completeOp(op);
							},
							"async-driver-completion-" + totalPutCalls.get());
			completionThread.setDaemon(true);
			completionThread.start();
			return true;
		}

		@SuppressWarnings("unchecked")
		private void completeOp(final Operation<DataItem> op) {
			op.reset();
			op.startRequest();
			op.finishRequest();
			op.startResponse();
			final int attempt;
			synchronized (attemptsByItemLock) {
				attempt = attemptsByItem.merge(op.item(), 1, Integer::sum);
			}
			if (attempt <= failuresBeforeSuccess) {
				op.status(failureStatus);
			} else {
				if (op instanceof DataOperation<?>) {
					final DataOperation<DataItem> dataOp = (DataOperation<DataItem>) op;
					try {
						dataOp.countBytesDone(dataOp.item().size());
					} catch (final IOException ignored) {
						// test data always has a fixed size; not expected
					}
				}
				op.status(Operation.Status.SUCC);
				successCount.incrementAndGet();
			}
			op.finishResponse();
			opResultOut.put(op.result());
		}

		@Override
		public int put(final List<Operation<DataItem>> ops, final int from, final int to) {
			int i = from;
			while (i < to && put(ops.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops) {
			return put(ops, 0, ops.size());
		}

		@Override
		public void operationResultOutput(final Output<Operation<DataItem>> opResultOut) {
			this.opResultOut = opResultOut;
		}

		@Override
		public List<DataItem> list(
						final ItemFactory<DataItem> itemFactory, final String path, final String prefix,
						final int idRadix, final DataItem lastPrevItem, final int count) {
			return List.of();
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return 16;
		}

		@Override
		public int activeOpCount() {
			return 0;
		}

		@Override
		public long scheduledOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public long completedOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public boolean isIdle() {
			return true;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

		int totalPutCalls() {
			return totalPutCalls.get();
		}

		int successCount() {
			return successCount.get();
		}
	}

	/**
	 * A driver whose very first {@code put()} call blocks until the test releases a latch,
	 * then every call (including that first one, once released) succeeds normally. Used to
	 * deterministically hold the real generator's own work-loop thread inside a single
	 * {@code doWork()} call (specifically inside that first dispatch) for as long as the
	 * test needs - long enough to inject something into {@link LoadGenerator#retry} and
	 * prove it cannot be drained (the generator's thread is provably still stuck elsewhere)
	 * while a concurrent {@code stop()} is made to wait for it anyway.
	 */
	private static final class BlockingFirstDispatchDriver
					extends com.dell.spt.base.concurrent.AsyncRunnableBase
					implements StorageDriver<DataItem, Operation<DataItem>> {
		private final java.util.concurrent.CountDownLatch releaseFirstDispatch;
		private final AtomicInteger totalPutCalls = new AtomicInteger();
		private final OperationLifecycleTracker<Operation<DataItem>> operationLifecycle = new OperationLifecycleTracker<>();
		private volatile Output<Operation<DataItem>> opResultOut;

		BlockingFirstDispatchDriver(final java.util.concurrent.CountDownLatch releaseFirstDispatch) {
			this.releaseFirstDispatch = releaseFirstDispatch;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean put(final Operation<DataItem> op) {
			operationLifecycle.driverQueued(op);
			operationLifecycle.dispatched(op);
			final int callNumber = totalPutCalls.incrementAndGet();
			if (callNumber == 1) {
				try {
					releaseFirstDispatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					operationLifecycle.unresolved(op);
					return false;
				}
			}
			op.reset();
			op.startRequest();
			op.finishRequest();
			op.startResponse();
			if (op instanceof DataOperation<?>) {
				final DataOperation<DataItem> dataOp = (DataOperation<DataItem>) op;
				try {
					dataOp.countBytesDone(dataOp.item().size());
				} catch (final IOException e) {
					throw new AssertionError("test item size should be readable", e);
				}
			}
			op.status(Operation.Status.SUCC);
			op.finishResponse();
			if (!operationLifecycle.completionStarted(op)) {
				return false;
			}
			final boolean retained = opResultOut.put(op.result());
			if (retained) {
				operationLifecycle.terminal(op);
			} else {
				operationLifecycle.unresolved(op);
			}
			return retained;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops, final int from, final int to) {
			int i = from;
			while (i < to && put(ops.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops) {
			return put(ops, 0, ops.size());
		}

		@Override
		public void operationResultOutput(final Output<Operation<DataItem>> opResultOut) {
			this.opResultOut = opResultOut;
		}

		@Override
		public List<DataItem> list(
						final ItemFactory<DataItem> itemFactory, final String path, final String prefix,
						final int idRadix, final DataItem lastPrevItem, final int count) {
			return List.of();
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return 16;
		}

		@Override
		public int activeOpCount() {
			return 0;
		}

		@Override
		public long scheduledOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public long completedOpCount() {
			return totalPutCalls.get();
		}

		@Override
		public boolean isIdle() {
			return true;
		}

		@Override
		public OperationLifecycleTracker<Operation<DataItem>> operationLifecycle() {
			return operationLifecycle;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

		int totalPutCalls() {
			return totalPutCalls.get();
		}
	}

	@Test
	public void shutdownRecoversRetryQueuedBehindBlockedDispatchWithoutWaiting() throws Exception {
		// A blocked Output implementation must not hold the generator's admission lock.
		// Stop closes admission first, interrupts that already-started handoff, and recovers
		// retry work still owned by the generator as unattempted instead of waiting for the
		// work loop to redispatch it.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final var itemInput = new FixedCountItemInput(1);
		final var releaseFirstDispatch = new java.util.concurrent.CountDownLatch(1);
		final var blockingDriver = new BlockingFirstDispatchDriver(releaseFirstDispatch);

		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(blockingDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();

		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("shutdown-drain-regression");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"shutdown-drain-regression-step", realGenerator, blockingDriver, metrics,
						testConfig.configVal("load"), false);

		stepCtx.start();
		try {
			// Wait for the generator to actually be stuck inside its first (blocking)
			// dispatch - i.e. it has dispatched the one item and is now provably unable to
			// loop back to doWork() to drain anything.
			final long dispatchDeadline = System.currentTimeMillis() + 5_000;
			while (blockingDriver.totalPutCalls() < 1 && System.currentTimeMillis() < dispatchDeadline) {
				Thread.sleep(5);
			}
			assertEquals(1, blockingDriver.totalPutCalls(), "the one op should have reached the (now-blocked) driver");

			// Inject directly into the generator's retry queue - simulating a retry that a
			// scheduling task has already (successfully, per awaitRetryTasksSettled())
			// enqueued - while the generator's thread is provably stuck above.
			final DataOperation<DataItem> injectedRetry = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("injected-retry-item", 0, 1024), null, "test-bucket", null, List.of(), 0);
			realGenerator.retry(injectedRetry);
			assertFalse(realGenerator.isNothingPendingRetry(), "the injected retry should be sitting in the generator's retry queue");

			// Stop must complete even though the first handoff has not been released.
			final Thread stopperThread = new Thread(stepCtx::stop, "stopper");
			stopperThread.start();
			stopperThread.join(5_000);
			assertFalse(stopperThread.isAlive(), "stop() should close admission without waiting for redispatch");
			assertTrue(realGenerator.isNothingPendingRetry(), "the recovered retry queue must be empty");
			assertEquals(
							1, blockingDriver.totalPutCalls(),
							"the generator-buffered retry must not cross the closed driver gate");
			assertEquals(OperationLifecycleState.UNATTEMPTED, injectedRetry.lifecycle().state());
			assertEquals(1, stepCtx.operationLifecycle().unattempted());
		} finally {
			releaseFirstDispatch.countDown(); // in case an assertion failed before this ran
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void blockedDispatchShutdownIsBoundedAndDoesNotConvertRecoveryToFailure() throws Exception {
		// Generator-buffered retry work has not crossed actual dispatch. Even when another
		// handoff is blocked, shutdown recovers it promptly as unattempted and does not turn
		// it into an ordinary operational failure.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final var itemInput = new FixedCountItemInput(1);
		final var releaseFirstDispatch = new java.util.concurrent.CountDownLatch(1);
		final var blockingDriver = new BlockingFirstDispatchDriver(releaseFirstDispatch);

		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(blockingDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();

		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("shutdown-drain-timeout-regression");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"shutdown-drain-timeout-regression-step", realGenerator, blockingDriver, metrics,
						testConfig.configVal("load"), false);

		stepCtx.start();
		try {
			// Wait for the generator to actually be stuck inside its first (blocking, and
			// in this test, *never released*) dispatch.
			final long dispatchDeadline = System.currentTimeMillis() + 5_000;
			while (blockingDriver.totalPutCalls() < 1 && System.currentTimeMillis() < dispatchDeadline) {
				Thread.sleep(5);
			}
			assertEquals(1, blockingDriver.totalPutCalls(), "the one op should have reached the (now-blocked) driver");

			// Inject directly into the generator's retry queue, exactly as the sibling
			// test above does - except this time nothing will ever unblock the generator's
			// thread to drain it.
			final DataOperation<DataItem> injectedRetry = new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("injected-retry-item-timeout", 0, 1024), null, "test-bucket", null, List.of(), 0);
			realGenerator.retry(injectedRetry);
			assertFalse(realGenerator.isNothingPendingRetry(), "the injected retry should be sitting in the generator's retry queue");

			// Start the public stop() lifecycle concurrently without releasing the first
			// handoff. Closing admission and recovering the retry remains bounded.
			final long stopStartedAt = System.currentTimeMillis();
			final Thread stopperThread = new Thread(stepCtx::stop, "stopper");
			stopperThread.start();

			stopperThread.join(10_000);
			final long stopDurationMillis = System.currentTimeMillis() - stopStartedAt;
			assertFalse(stopperThread.isAlive(), "stop() must complete boundedly even though the retry queue can never drain");
			assertTrue(
							stopDurationMillis < 5_000,
							"stop() took " + stopDurationMillis + "ms - admission closure must remain bounded");

			// The injected retry must never have reached the driver: only the original
			// (permanently blocked) dispatch counts.
			assertEquals(1, blockingDriver.totalPutCalls(), "the stranded retry must not have been dispatched to the driver");

			// It must not still be sitting in the generator's retry queue either - the
			// timeout path must have actively drained it, not just given up and left it
			// there for doClose() to silently discard.
			assertTrue(realGenerator.isNothingPendingRetry(), "the retry queue must be empty (drained) before close(), not just abandoned");

			assertEquals(OperationLifecycleState.UNATTEMPTED, injectedRetry.lifecycle().state());

			// Recovery is lifecycle accounting, not an operational request failure.
			metrics.refreshLastSnapshot(true);
			final AllMetricsSnapshot snapshot = metrics.lastSnapshot();
			assertEquals(
							0, snapshot.failsSnapshot().count(),
							"unattempted recovery must not be converted into an ordinary failure");
		} finally {
			releaseFirstDispatch.countDown();
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void realGeneratorRetryOnlyFiniteWorkloadTerminatesWithoutHanging() throws Exception {
		// Integration test for finding #1/#9: a mocked LoadGenerator only proves put()
		// eventually calls recycle() - it can't catch the real liveness bug, where
		// LoadGeneratorImpl itself never signals item-input-finished once retry is enabled
		// (LoadGeneratorBuilderImpl passes recycleFlag || retryFlag into it, so the
		// generator must keep polling its recycle queue for late retries even after item
		// input is exhausted). A finite, retry-only (recycle-mode off), no-count-limit
		// workload could hang forever waiting for a generator that's waiting for recycled
		// work that will never arrive. This drives the *real* LoadGeneratorImpl together
		// with the real LoadStepContextImpl to prove the isDone() fix actually terminates.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final int totalItems = 2;
		final int failuresBeforeSuccess = 1;
		final var itemInput = new FixedCountItemInput(totalItems);
		final var flakyDriver = new FlakyThenSucceedsDriver(failuresBeforeSuccess, Operation.Status.RESP_FAIL_SVC);

		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(flakyDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();

		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("retry-integration");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"retry-integration-step", realGenerator, flakyDriver, metrics,
						testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());

		// Use the real public lifecycle (start()/stop()/shutdown()/close()), not the
		// protected doStart()/doStop() template hooks directly: those hooks don't transition
		// the AsyncRunnable state, and isDone()'s very first check requires STARTED/SHUTDOWN
		// state, so calling the hooks directly would make isDone() true immediately for the
		// wrong reason without ever exercising the retry-only completion logic below it.
		stepCtx.start();
		try {
			final long deadline = System.currentTimeMillis() + 10_000;
			while (!stepCtx.isDone() && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}
			assertTrue(stepCtx.isDone(), "retry-only finite workload should complete, not hang");
			// Each item: 1 failed attempt (retried) + 1 successful attempt = 2 dispatches.
			assertEquals(totalItems * (failuresBeforeSuccess + 1), flakyDriver.totalPutCalls());
			assertEquals(totalItems, flakyDriver.successCount());
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	/** Builds a real (not mocked) generator + step context wired to the given item count/driver. */
	@SuppressWarnings("unchecked")
	private LoadStepContextImpl<DataItem, Operation<DataItem>> buildRealRetryStepCtx(
					final int totalItems, final FlakyThenSucceedsDriver flakyDriver, final String metricsId)
					throws IOException {
		final var itemInput = new FixedCountItemInput(totalItems);
		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(flakyDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx(metricsId);
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						metricsId + "-step", realGenerator, flakyDriver, metrics, testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());
		return stepCtx;
	}

	private void runUntilDoneOrTimeout(final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx, final String failureMessage) {
		stepCtx.start();
		final long deadline = System.currentTimeMillis() + 10_000;
		while (!stepCtx.isDone() && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(10);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		assertTrue(stepCtx.isDone(), failureMessage);
	}

	@Test
	public void oneAssembledOperationCompletesAfterOneTerminalResult() throws Exception {
		testConfig.val("load-op-type", "delete");
		testConfig.val("load-op-retry", false);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);
		final var consumedIdentityCount = 4;
		final var fullInput = new FixedCountItemInput(consumedIdentityCount);
		final var assembler = new OperationAssembler<DataItem, Operation<DataItem>>() {
			@Override
			public int originIndex() {
				return 0;
			}

			@Override
			public OpType opType() {
				return OpType.DELETE;
			}

			@Override
			public OperationAssemblyResult assemble(
							final List<DataItem> items, final List<Operation<DataItem>> operations) {
				final var request = new DataOperationImpl<DataItem>(
								0, OpType.DELETE, items.get(0), "/bucket", null, null, List.of(), 0);
				operations.add(request);
				return new OperationAssemblyResult(items.size(), operations.size());
			}

			@Override
			public void close() {}
		};
		final var cardinalityDriver = DummyStorageDriverMock.<DataItem, Operation<DataItem>> create();
		final var cardinalityGenerator = new LoadGeneratorImpl<>(
						fullInput,
						assembler,
						List.of(),
						cardinalityDriver,
						consumedIdentityCount,
						0,
						1000,
						false,
						false);
		final var metrics = buildMetricsCtx("cardinality-neutral-completion");
		final var stepCtx = new LoadStepContextImpl<>(
						"cardinality-neutral-completion-step",
						cardinalityGenerator,
						cardinalityDriver,
						metrics,
						testConfig.configVal("load"),
						false);
		try {
			runUntilDoneOrTimeout(stepCtx, "one emitted request should complete after one terminal result");
			metrics.refreshLastSnapshot(true);
			assertEquals(consumedIdentityCount, cardinalityGenerator.consumedItemCount());
			assertEquals(1, cardinalityGenerator.generatedOpCount());
			assertEquals(1, cardinalityDriver.completedOpCount());
			assertEquals(1, metrics.lastSnapshot().successSnapshot().count());
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void retryGetsItsFullConfiguredAttemptEvenWhenCountLimitReachedByOriginalDispatch() throws Exception {
		// Finding #1 (round 2): with load-op-limit-count=1, LoadGeneratorImpl used to reach
		// its dispatch-count limit and self-stop right after the *original* (pre-retry)
		// dispatch of the one op it's allowed - potentially before that op has even
		// completed, let alone before a retry decision could be made - so a retryable
		// failure would be counted failed with zero retry attempts used, despite a
		// configured retryLimit > 0. LoadGeneratorImpl now only self-stops on count-limit
		// when load-op-retry is off; with it on, the operation must actually get to use its
		// retry budget and succeed via its retry.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 1);

		final var flakyDriver = new FlakyThenSucceedsDriver(1, Operation.Status.RESP_FAIL_SVC);
		final var stepCtx = buildRealRetryStepCtx(1, flakyDriver, "count-limit-retry");
		try {
			runUntilDoneOrTimeout(stepCtx, "count-limited retry-enabled workload should complete, not hang");
			// 1 failed attempt + 1 successful retry attempt = 2 dispatches total, and the
			// operation must have actually succeeded via that retry, not been force-failed.
			assertEquals(2, flakyDriver.totalPutCalls());
			assertEquals(1, flakyDriver.successCount());
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void realLifecycleRetryExhaustionWithCountLimitEndsInExactlyOneTerminalFailure() throws Exception {
		// Test gap: retry exhaustion was only exercised through direct put() calls against
		// a mocked generator. Drives a real generator + real (public) lifecycle, combined
		// with load-op-limit-count=1 (the exact configuration finding #1/round-2 was about)
		// and a driver that never succeeds, to prove the operation gets its full
		// retryLimit worth of attempts (not zero, not unbounded) before being counted as
		// exactly one terminal failure - not lost, not double-counted.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 2);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 1);

		// Never succeeds within the 3 total attempts (1 original + 2 retries) this test expects.
		final var alwaysFailsDriver = new FlakyThenSucceedsDriver(100, Operation.Status.RESP_FAIL_SVC);
		final var itemInput = new FixedCountItemInput(1);
		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(alwaysFailsDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("exhaustion-count-limit");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"exhaustion-count-limit-step", realGenerator, alwaysFailsDriver, metrics,
						testConfig.configVal("load"), false);
		stepCtx.setRetryScheduler(synchronousRetryScheduler());
		try {
			runUntilDoneOrTimeout(stepCtx, "count-limited retry-exhausted workload should complete, not hang");
			// 1 original attempt + 2 retries (retryLimit) = 3 total dispatches, none of
			// which succeed.
			assertEquals(3, alwaysFailsDriver.totalPutCalls());
			assertEquals(0, alwaysFailsDriver.successCount());
			// Pin terminal accounting precisely: exactly one failure recorded in the
			// reported metrics (not zero - lost - and not more than one - double-counted
			// across the exhausted retry attempts).
			// lastSnapshot() alone would return a stale (possibly pre-failure, cached)
			// snapshot if anything already triggered one earlier - force a fresh one.
			metrics.refreshLastSnapshot(true);
			final AllMetricsSnapshot snapshot = metrics.lastSnapshot();
			assertEquals(1, snapshot.failsSnapshot().count(), "exactly one terminal failure should be recorded");
			assertEquals(0, snapshot.successSnapshot().count(), "no success should be recorded");
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void emptyFiniteRetryOnlyWorkloadCompletesWithoutHanging() throws Exception {
		// Finding #3 (round 2): the retry-only isDone() completion check used to require
		// counterResults.sum() > 0 (guarding against a startup race, mirroring the existing
		// recycle-mode isNothingToRecycle() check) - but for a genuinely *empty* finite item
		// input, counterResults never leaves 0 since no operation is ever generated at all,
		// so that guard made this case indistinguishable from "nothing has happened *yet*"
		// and the step could never complete. The simplified check (counterResults >=
		// generatedOpCount(), both legitimately 0 here) handles this correctly instead.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final var flakyDriver = new FlakyThenSucceedsDriver(1, Operation.Status.RESP_FAIL_SVC);
		final var stepCtx = buildRealRetryStepCtx(0, flakyDriver, "empty-retry-only");
		try {
			runUntilDoneOrTimeout(stepCtx, "empty retry-only workload should complete immediately, not hang");
			assertEquals(0, flakyDriver.totalPutCalls());
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	@Test
	public void asyncDriverCompletionAfterGeneratorExhaustionStillGetsRetried() throws Exception {
		// P0 (round 4 review): every other integration test in this class uses a
		// *synchronous* driver, whose completion is delivered within the very same put()
		// call the generator makes - it can never arrive later than the moment the
		// generator reaches its own "dispatched everything, item input exhausted" state,
		// so it can't exercise this timing at all. A real (especially cooperative/Netty)
		// driver is asynchronous: put() returns as soon as the request is *accepted*, and
		// the actual result can arrive well after the generator has already moved on. If
		// the generator used that "dispatched == generated, item input exhausted" signal
		// to self-stop (rather than deferring entirely to isDone()'s terminal-results-vs-
		// generated comparison while load-op-retry is enabled), a retryable failure
		// arriving after that point would find the generator already stopped and be
		// forced to a terminal failure having used none of its configured retry budget.
		// Deliberately uses the real (not synchronous-test-stub) retry scheduler too, for
		// full end-to-end authenticity including a genuine backoff delay.
		testConfig.val("load-op-retry", true);
		testConfig.val("load-op-retryLimit", 3);
		testConfig.val("load-op-recycle-mode", false);
		testConfig.val("load-op-limit-count", 0);

		final var itemInput = new FixedCountItemInput(1);
		// Comfortably longer than the microseconds it takes the real generator to reach
		// itemInputFinishFlag for a 1-item input once dispatched.
		final var asyncDriver = new AsyncFlakyThenSucceedsDriver(1, Operation.Status.RESP_FAIL_SVC, 150L);

		final LoadGeneratorBuilder realGeneratorBuilder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(testConfig.configVal("item"))
						.loadConfig(testConfig.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(asyncDriver)
						.authConfig(testConfig.configVal("storage").configVal("auth"))
						.originIndex(0);
		@SuppressWarnings("unchecked")
		final LoadGenerator<DataItem, Operation<DataItem>> realGenerator = (LoadGenerator<DataItem, Operation<DataItem>>) realGeneratorBuilder.build();

		final MetricsContext<AllMetricsSnapshot> metrics = buildMetricsCtx("async-p0-regression");
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepCtx = new LoadStepContextImpl<>(
						"async-p0-regression-step", realGenerator, asyncDriver, metrics,
						testConfig.configVal("load"), false);
		// Deliberately not overriding the retry scheduler: this test wants the full,
		// authentic async path end to end, not just an async driver completion feeding
		// into an otherwise-synchronous retry.

		stepCtx.start();
		try {
			final long deadline = System.currentTimeMillis() + 10_000;
			while (!stepCtx.isDone() && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}
			assertTrue(stepCtx.isDone(), "async finite-input retry-only workload should complete, not hang");
			// 1 failed attempt + 1 successful retry attempt = 2 dispatches total, and the
			// operation must have actually succeeded via that retry, not been forced to a
			// terminal failure because the generator had already stopped by the time the
			// async completion (and then the retry) arrived.
			assertEquals(2, asyncDriver.totalPutCalls());
			assertEquals(1, asyncDriver.successCount());
		} finally {
			stepCtx.stop();
			stepCtx.shutdown();
			stepCtx.close();
		}
	}

	private DataOperation<DataItem> baseDataOp(final String name, final long size) {
		final DataItem item = new DataItemImpl(name, 0, size);
		return new DataOperationImpl<>(0, OpType.CREATE, item, "/in", "/out", null, List.of(), 0);
	}

	private DataOperation<DataItem> newSuccDataOp(final String name, final long size) {
		final DataOperation<DataItem> op = baseDataOp(name, size);
		// simulate full successful timing and bytes
		op.reset();
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		if (op instanceof DataOperationImpl) {
			((DataOperationImpl<DataItem>) op).countBytesDone(size);
			((DataOperationImpl<DataItem>) op).startDataResponse();
		}
		((Operation<DataItem>) op).finishResponse();
		((Operation<DataItem>) op).status(Operation.Status.SUCC);
		return op;
	}

	// simple collecting output for operations used in assertions
	private static class CollectingOpOutput<I extends DataItem> implements Output<Operation<I>> {
		final List<Operation<I>> received = new ArrayList<>();
		final java.util.concurrent.atomic.AtomicBoolean poisoned = new java.util.concurrent.atomic.AtomicBoolean(false);

		@Override
		public boolean put(final Operation<I> o) {
			if (o == null) {
				poisoned.set(true);
				return true;
			}
			received.add(o);
			return true;
		}

		@Override
		public int put(final List<Operation<I>> list, final int from, final int to) {
			return 0;
		}

		@Override
		public int put(final List<Operation<I>> list) {
			return 0;
		}

		@Override
		public Input<Operation<I>> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	private static final class ThrowingIoOpOutput<I extends DataItem>
					extends CollectingOpOutput<I> {
		@Override
		public boolean put(final Operation<I> op) {
			throwUnchecked(new IOException("disk full"));
			return false;
		}
	}

	// flaky output that returns false first N times to trigger backpressure loop in doStop
	private static class FlakyOpOutput<I extends DataItem> implements Output<Operation<I>> {
		private final int failTimes;
		private final AtomicInteger calls = new AtomicInteger(0);
		final List<Operation<I>> received = new ArrayList<>();
		final java.util.concurrent.atomic.AtomicBoolean poisoned = new java.util.concurrent.atomic.AtomicBoolean(false);

		FlakyOpOutput(final int failTimes) {
			this.failTimes = failTimes;
		}

		@Override
		public boolean put(final Operation<I> o) {
			if (o == null) {
				poisoned.set(true);
				return true;
			}
			final int c = calls.getAndIncrement();
			if (c < failTimes) {
				return false;
			}
			received.add(o);
			return true;
		}

		@Override
		public int put(final List<Operation<I>> list, final int from, final int to) {
			return 0;
		}

		@Override
		public int put(final List<Operation<I>> list) {
			return 0;
		}

		@Override
		public Input<Operation<I>> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	/* Utility Methods */
	Input<Item> createCSVItemInput() throws InterruptedException {
		File csvFile = new File(TMP_DIR_PATH.toString(), "test.csv");

		try {
			csvFile.createNewFile();
		} catch (IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to create the item input file \"{}\"", csvFile);
		}

		testConfig.val("item-input-file", TMP_DIR_PATH.toString() + "/test.csv");
		var mockDriver = DummyStorageDriverMock.create();
		return ItemInputFactory.createItemInput(testConfig.configVal("item"), batchSize, mockDriver);
	}

	Path createItemOutputFile() {
		File outputFile = new File(TMP_DIR_PATH.toString(), "item-output-file.txt");

		try {
			outputFile.createNewFile();
		} catch (IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to create the item output file \"{}\"", outputFile);
		}
		return Paths.get(TMP_DIR_PATH.toString(), "item-output-file.txt");
	}

	@AfterAll
	static void cleanup() {
		if (TMP_DIR_PATH != null) {
			try {
				try (var stream = java.nio.file.Files.walk(TMP_DIR_PATH)) {
					stream.sorted((o1, o2) -> o2.compareTo(o1)).forEach(path -> {
						try {
							if (java.nio.file.Files.deleteIfExists(path)) {
								System.out.println("Deleted file: " + path);
							}
						} catch (IOException e) {
							LogUtil.exception(Level.WARN, e, "Failed to Delete temp files");
						}
					});
				}
			} catch (IOException e) {
				LogUtil.exception(Level.WARN, e, "Failed to delete ", TMP_DIR_PATH.toString());
			}
		}
	}

	/* Utility Methods */
	void initDirectory() {
		TMP_DIR_PATH = Path.of(System.getProperty("java.io.tmpdir"), "test_item_input_factory");
		TMP_DIR_PATH.toFile().mkdirs();
	}
}
