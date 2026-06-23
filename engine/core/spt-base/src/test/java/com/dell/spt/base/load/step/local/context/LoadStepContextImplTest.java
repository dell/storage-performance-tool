package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.io.ItemInfoFileOutput;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorderImpl;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilder;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.rmi.RemoteException;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/* Alot of the functionality from ItemInputFactoryTest is used here since need an ItemInputFactory */
public class LoadStepContextImplTest {
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
	public void doShutdownLogsAndContinuesOnDriverRemoteException() throws Exception {
		final LoadGenerator<DataItem, Operation<DataItem>> generatorMock = mock(LoadGenerator.class);
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
	public void markListSuccessIncrementsMetricsUsingObjectCount() throws Exception {
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
		assertEquals(3400L, trackingCtx.byteCount.get());
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

		final ListOperation<PathItemImpl> listOp = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("prefix"), null);
		listOp.reset();
		listOp.startRequest();
		listOp.finishRequest();
		listOp.startResponse();
		listOp.startDataResponse();
		listOp.finishResponse();
		listOp.status(Operation.Status.SUCC);
		listOp.objectsListed(3);
		listOp.bytesListed(123);
		listOp.countBytesDone(123);

		assertTrue(stepCtx.put((Operation) listOp));
		assertEquals(3L, trackingCtx.successCount.get());
		assertEquals(123L, trackingCtx.byteCount.get());
		assertTrue(trackingCtx.arrayTtfb.get() > 0L);
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
			delegate.markSucc(bytes, duration, latency);
		}

		@Override
		public void markSucc(final long bytes, final long duration, final long latency, final long ttfb) {
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
