package com.dell.spt.storage.driver.coop.nio;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.storage.driver.coop.nio.mock.NioStorageDriverMock;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NioStorageDriverBase's Condition-based worker signaling.
 * Uses NioStorageDriverMock as the concrete implementation to exercise
 * the submit → signal → worker wake → invokeNio → complete pipeline.
 */
public class NioStorageDriverBaseTest {

	private Config storageConfig;
	private DataInput dataInput;
	private OpResultCollector results;

	@BeforeEach
	void setUp() throws Exception {
		storageConfig = buildStorageConfig();
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4KB"), 4, false);
		results = new OpResultCollector();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (dataInput != null)
			dataInput.close();
	}

	private NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> newDriver() throws Exception {
		return newDriver(storageConfig);
	}

	private NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> newDriver(Config config) throws Exception {
		var drv = new NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>>(
						"test-nio", dataInput, config, false, 16);
		drv.operationResultOutput(results);
		return drv;
	}

	private BlockingInvokeDriver newBlockingDriver(Config config) throws Exception {
		var drv = new BlockingInvokeDriver("test-nio-blocking", dataInput, config, false, 16);
		drv.operationResultOutput(results);
		return drv;
	}

	private RetainedActiveDriver newRetainedActiveDriver(final Config config) throws Exception {
		final var drv = new RetainedActiveDriver("test-nio-active", dataInput, config, false, 16);
		drv.operationResultOutput(results);
		return drv;
	}

	private ThrowOnceInvokeDriver newThrowOnceInvokeDriver(final Config config) throws Exception {
		final var drv = new ThrowOnceInvokeDriver("test-nio-throw", dataInput, config, false, 16);
		drv.operationResultOutput(results);
		return drv;
	}

	private AlwaysThrowInvokeDriver newAlwaysThrowInvokeDriver(final Config config) throws Exception {
		final var drv = new AlwaysThrowInvokeDriver("test-nio-always-throw", dataInput, config, false, 16);
		drv.operationResultOutput(results);
		return drv;
	}

	private static DataOperationImpl<DataItemImpl> newCreateOp(String name, long size) {
		return new DataOperationImpl<>(0, OpType.CREATE, new DataItemImpl(name, 0, size), null, "/tmp/test", null, null, 0);
	}

	/**
	 * Basic end-to-end: submit a single op, verify it completes through the
	 * Condition-signaled worker path.
	 */
	@Test
	void testSingleOpCompletes() throws Exception {
		try (var drv = newDriver()) {
			drv.start();
			assertTrue(drv.put(newCreateOp("item1", 1024)));
			var result = results.await(2000);
			assertNotNull(result, "Op should complete within timeout");
			assertEquals(Operation.Status.SUCC, result.status());
			drv.stop();
		}
	}

	/**
	 * Submit a batch of ops and verify all complete. Tests that the signal/wake
	 * cycle handles sustained throughput correctly.
	 */
	@Test
	void testBatchOpsAllComplete() throws Exception {
		final int count = 50;
		try (var drv = newDriver()) {
			drv.start();
			for (int i = 0; i < count; i++) {
				var op = newCreateOp("batch-" + i, 512);
				int retries = 0;
				while (!drv.put(op) && retries++ < 200) {
					Thread.sleep(1);
				}
				assertTrue(retries < 200, "Failed to submit op " + i + " after retries");
			}
			for (int i = 0; i < count; i++) {
				var result = results.await(5000);
				assertNotNull(result, "Op " + i + " should complete within timeout");
				assertEquals(Operation.Status.SUCC, result.status());
			}
			drv.stop();
		}
	}

	/**
	 * Start the driver, let workers settle into Condition.await(), then submit an op.
	 * Verifies the signal wakes idle workers promptly — the key behavior of the
	 * Condition-based design replacing the old Thread.sleep(1) polling.
	 */
	@Test
	void testIdleWorkerWakesPromptly() throws Exception {
		try (var drv = newDriver()) {
			drv.start();
			// Let workers settle into Condition.await()
			Thread.sleep(100);

			long submitTime = System.nanoTime();
			assertTrue(drv.put(newCreateOp("wake-test", 256)));
			var result = results.await(500);
			long elapsedMs = (System.nanoTime() - submitTime) / 1_000_000;

			assertNotNull(result, "Op should complete after idle period");
			assertEquals(Operation.Status.SUCC, result.status());
			// With Condition signaling, total latency (dispatch poll + signal + worker process)
			// should be well under 100ms. The dispatch task polls at 1ms intervals, so expect
			// ~1-5ms typical latency.
			assertTrue(elapsedMs < 100, "Op should complete within 100ms but took " + elapsedMs + "ms");
			drv.stop();
		}
	}

	/**
	 * Start the driver with workers idle, then stop. Verifies that the interrupt
	 * from stop() correctly wakes workers from Condition.await() without hanging.
	 */
	@Test
	void testGracefulShutdownFromIdle() throws Exception {
		try (var drv = newDriver()) {
			drv.start();
			// Workers are idle in await
			Thread.sleep(50);
			// Stop should wake workers via interrupt and exit cleanly
			drv.stop();
		}
		// If we get here without hanging, the test passed
	}

	@Test
	void testDriverRestartsAfterBoundedStopWithoutRetainingQueuedWork() throws Exception {
		try (var drv = newDriver()) {
			drv.start();
			assertTrue(drv.put(newCreateOp("before-restart", 256)));
			assertNotNull(results.await(2_000));

			final long stopStarted = System.nanoTime();
			drv.stop();
			final long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStarted);
			assertTrue(stopMillis < 2_000, "stop should remain bounded but took " + stopMillis + " ms");
			assertEquals(0, drv.operationLifecycle().snapshot().generatorBuffered());
			assertEquals(0, drv.operationLifecycle().snapshot().driverQueued());
			assertEquals(0, drv.operationLifecycle().snapshot().inFlight());

			drv.start();
			assertTrue(drv.put(newCreateOp("after-restart", 256)));
			assertNotNull(results.await(2_000), "workers and dispatcher should restart cleanly");
			assertEquals(0, drv.activeOpCount());
			drv.stop();
		}
	}

	@Test
	void repeatedRestartCyclesTerminateEveryWorkerAndBoundOutstandingState() throws Exception {
		final var config = buildStorageConfig(8, 2);
		try (var drv = newDriver(config)) {
			for (var cycle = 0; cycle < 5; cycle++) {
				drv.start();
				for (var i = 0; i < 8; i++) {
					assertTrue(drv.put(newCreateOp("cycle-" + cycle + "-" + i, 256)));
				}
				for (var i = 0; i < 8; i++) {
					assertNotNull(results.await(2_000));
				}
				drv.stop();
				assertWorkerTasksTerminated(drv);
				assertEquals(0, drv.activeOpCount());
				assertEquals(0, drv.operationLifecycle().snapshot().generatorBuffered());
				assertEquals(0, drv.operationLifecycle().snapshot().driverQueued());
				assertEquals(0, drv.operationLifecycle().snapshot().inFlight());
			}
		}
	}

	@Test
	void outputFailureDuringAdmissionClosureReleasesPermitOnceAndPreservesRestartBound() throws Exception {
		final var outputEntered = new CountDownLatch(1);
		final var releaseOutput = new CountDownLatch(1);
		final var drv = newDriver(buildStorageConfig(1, 1));
		drv.operationResultOutput(new Output<>() {
			@Override
			public boolean put(final Operation<DataItemImpl> val) {
				outputEntered.countDown();
				while (releaseOutput.getCount() > 0) {
					try {
						releaseOutput.await();
					} catch (final InterruptedException ignored) {
						// Keep output blocked until admission has closed.
					}
				}
				throw new IllegalStateException("output failed");
			}

			@Override
			public int put(
							final List<Operation<DataItemImpl>> vals, final int from, final int to) {
				return 0;
			}

			@Override
			public int put(final List<Operation<DataItemImpl>> vals) {
				return 0;
			}

			@Override
			public Input<Operation<DataItemImpl>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		});
		final var failed = newCreateOp("output-failure", 256);
		try {
			drv.start();
			assertTrue(drv.put(failed));
			assertTrue(outputEntered.await(2, TimeUnit.SECONDS));

			drv.closeAdmission();
			releaseOutput.countDown();
			awaitLifecycle(
							drv,
							() -> failed.lifecycle().state() == OperationLifecycleState.UNRESOLVED,
							"output failure should retain a lifecycle outcome");
			awaitNioReservationsCleared(drv);
			assertEquals(0, drv.activeOpCount(), "one accepted operation must release exactly one permit");

			drv.stop();
			drv.operationResultOutput(results);
			drv.start();
			assertTrue(drv.put(newCreateOp("after-output-failure", 256)));
			assertNotNull(results.await(2_000));
			assertEquals(0, drv.activeOpCount());
		} finally {
			releaseOutput.countDown();
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	@Test
	void invokeFailureSettlesCurrentOperationAndPreservesTrailingBatchAndRestartCapacity() throws Exception {
		final var drv = newThrowOnceInvokeDriver(buildStorageConfig(3, 1));
		final var failed = newCreateOp("invoke-failure", 256);
		final var trailingOne = newCreateOp("trailing-one", 256);
		final var trailingTwo = newCreateOp("trailing-two", 256);
		try {
			drv.start();
			assertEquals(3, drv.put(List.of(failed, trailingOne, trailingTwo)));

			final var completed = new ArrayList<Operation<DataItemImpl>>(3);
			completed.add(results.await(2_000));
			completed.add(results.await(2_000));
			completed.add(results.await(2_000));
			assertTrue(completed.stream().allMatch(java.util.Objects::nonNull),
							"the failing invocation and every trailing local operation must retain an owner");
			assertEquals(1, completed.stream()
							.filter(op -> op.status() == Operation.Status.FAIL_UNKNOWN)
							.count());
			assertEquals(2, completed.stream()
							.filter(op -> op.status() == Operation.Status.SUCC)
							.count());
			awaitNioReservationsCleared(drv);
			assertEquals(0, drv.activeOpCount(), "the accepted batch must release exactly three permits");
			assertEquals(0, drv.operationLifecycle().snapshot().inFlight());
			assertEquals(3, drv.operationLifecycle().snapshot().terminal());

			drv.stop();
			drv.start();
			assertTrue(drv.put(newCreateOp("after-invoke-failure", 256)));
			assertNotNull(results.await(2_000), "the restarted driver must retain full admission capacity");
			assertEquals(0, drv.activeOpCount());
		} finally {
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	@Test
	void repeatedInvokeFailuresRetainOutcomesAndUseOneShotHighSeverityGuard() throws Exception {
		final var drv = newAlwaysThrowInvokeDriver(buildStorageConfig(3, 1));
		try {
			drv.start();
			assertEquals(3, drv.put(List.of(
							newCreateOp("repeated-invoke-failure-1", 256),
							newCreateOp("repeated-invoke-failure-2", 256),
							newCreateOp("repeated-invoke-failure-3", 256))));
			for (var i = 0; i < 3; i++) {
				assertEquals(Operation.Status.FAIL_UNKNOWN, results.await(2_000).status());
			}
			final var logGuardField = NioStorageDriverBase.class.getDeclaredField("operationFailureReported");
			logGuardField.setAccessible(true);
			assertTrue(((AtomicBoolean) logGuardField.get(drv)).get());
			assertEquals(0, drv.activeOpCount());
			assertEquals(3, drv.operationLifecycle().snapshot().terminal());
		} finally {
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	/**
	 * Submit ops, then stop while some may still be in flight.
	 * Verifies no hang during shutdown with active work.
	 */
	@Test
	void testShutdownDuringProcessing() throws Exception {
		try (var drv = newDriver()) {
			drv.start();
			// Submit a burst of ops
			for (int i = 0; i < 20; i++) {
				drv.put(newCreateOp("inflight-" + i, 1024));
			}
			// Stop immediately while ops may still be processing
			drv.stop();
		}
		// If we get here without hanging, the test passed
	}

	/**
	 * Regression test for the permit leak in submit(List, int, int).
	 *
	 * With concurrency=1, drainPermits() returns at most 1 permit, but the
	 * distribution loop is bounded by (to - j) and opBuffCapacity — not by
	 * the number of permits acquired. This causes all ops in the batch to
	 * enter worker buffers with only 1 permit consumed. Each completion then
	 * releases a permit, inflating availablePermits far past concurrencyLimit.
	 *
	 * After all ops complete, activeOpCount() should be 0. With the bug it
	 * goes deeply negative (e.g. -99 for 100 ops with concurrency=1).
	 */
	@Test
	void testPermitIntegrityAfterBatchCompletion() throws Exception {
		final int opCount = 100;
		final var lowConcurrencyConfig = buildStorageConfig(1);
		try (var drv = newDriver(lowConcurrencyConfig)) {
			drv.start();
			for (int i = 0; i < opCount; i++) {
				var op = newCreateOp("permit-leak-" + i, 256);
				int retries = 0;
				while (!drv.put(op) && retries++ < 500) {
					Thread.sleep(1);
				}
				assertTrue(retries < 500, "Failed to submit op " + i + " after retries");
			}
			// Wait for all ops to complete
			for (int i = 0; i < opCount; i++) {
				var result = results.await(5000);
				assertNotNull(result, "Op " + i + " should complete within timeout");
			}
			// Allow worker threads to finish releasing permits
			Thread.sleep(100);

			int activeOps = drv.activeOpCount();
			assertTrue(activeOps >= 0,
							"activeOpCount() should never be negative after all ops complete, but was " + activeOps
											+ " (permit leak: semaphore has more permits than concurrencyLimit)");
			assertEquals(0, activeOps,
							"activeOpCount() should be exactly 0 when all ops have completed");
			assertTrue(drv.isIdle(), "Driver should be idle after all ops complete");
		}
	}

	/**
	 * Verifies that activeOpCount() never goes negative during processing.
	 *
	 * A monitoring thread samples activeOpCount() while ops are in flight.
	 * With the permit leak, the semaphore overflows as fast-completing ops
	 * release permits that were never acquired, causing negative readings
	 * even mid-run.
	 */
	@Test
	void testActiveOpCountNeverNegativeDuringProcessing() throws Exception {
		final int opCount = 200;
		final var lowConcurrencyConfig = buildStorageConfig(1);
		try (var drv = newDriver(lowConcurrencyConfig)) {
			drv.start();

			final var minObserved = new AtomicInteger(Integer.MAX_VALUE);
			final var monitoring = new AtomicInteger(1);

			// Monitor thread samples activeOpCount at high frequency
			Thread monitor = new Thread(() -> {
				while (monitoring.get() != 0) {
					int active = drv.activeOpCount();
					minObserved.accumulateAndGet(active, Math::min);
					Thread.yield();
				}
			});
			monitor.setDaemon(true);
			monitor.start();

			for (int i = 0; i < opCount; i++) {
				var op = newCreateOp("monitor-" + i, 256);
				int retries = 0;
				while (!drv.put(op) && retries++ < 500) {
					Thread.sleep(1);
				}
				assertTrue(retries < 500, "Failed to submit op " + i);
			}
			// Wait for all ops to complete
			for (int i = 0; i < opCount; i++) {
				var result = results.await(5000);
				assertNotNull(result, "Op " + i + " should complete");
			}
			Thread.sleep(100);

			monitoring.set(0);
			monitor.join(1000);

			int min = minObserved.get();
			assertTrue(min >= 0,
							"activeOpCount() was observed as " + min
											+ " during processing — permits leaked past concurrencyLimit");
		}
	}

	@Test
	void testBatchSubmitProgressWhenWorkerIsExecutingBlockingInvoke() throws Exception {
		final var config = buildStorageConfig(2, 1);
		final var drv = newBlockingDriver(config);
		try {
			drv.start();
			assertTrue(drv.submit(newCreateOp("blocking-0", 256)));
			assertTrue(drv.awaitInvokeEntered(2_000), "First blocking invoke should start");
			final int submitted = drv.submit(List.of(newCreateOp("blocking-1", 256)), 0, 1);
			assertEquals(1, submitted,
							"Batch submit should make progress while another op is in blocking invoke");
			drv.releaseInvokes();
			assertNotNull(results.await(2_000));
			assertNotNull(results.await(2_000));
		} finally {
			drv.releaseInvokes();
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	@Test
	void closeAdmissionRecoversWorkerQueuedWorkButDrainsActualDispatch() throws Exception {
		final var config = buildStorageConfig(2, 1);
		final var drv = newBlockingDriver(config);
		final var dispatched = newCreateOp("dispatched", 256);
		final var queued = newCreateOp("worker-queued", 256);
		try {
			drv.start();
			assertTrue(drv.put(dispatched));
			assertTrue(drv.awaitInvokeEntered(2_000), "first operation should reach actual invocation");
			assertTrue(drv.put(queued));
			awaitLifecycle(
							drv,
							() -> drv.operationLifecycle().snapshot().dispatched() == 1
											&& drv.operationLifecycle().snapshot().driverQueued() == 1,
							"one operation should be dispatched and one retained in the worker queue");

			drv.closeAdmission();
			final var recovered = drv.recoverQueuedOperations();

			assertEquals(List.of(queued), recovered);
			assertEquals(OperationLifecycleState.UNATTEMPTED, queued.lifecycle().state());
			assertEquals(OperationLifecycleState.DISPATCHED, dispatched.lifecycle().state());
			assertEquals(1, drv.operationLifecycle().snapshot().inFlight());

			drv.releaseInvokes();
			assertNotNull(results.await(2_000), "already-dispatched operation should finish during drain");
			awaitLifecycle(
							drv,
							() -> drv.operationLifecycle().snapshot().inFlight() == 0,
							"dispatched operation should become terminal");
			assertEquals(OperationLifecycleState.TERMINAL, dispatched.lifecycle().state());
		} finally {
			drv.releaseInvokes();
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	@Test
	void closeAdmissionRetainsActiveNioWorkForTheBoundedDrain() throws Exception {
		final var drv = newRetainedActiveDriver(buildStorageConfig(1, 1));
		final var dispatched = newCreateOp("retained-active", 256);
		try {
			drv.start();
			assertTrue(drv.put(dispatched));
			assertTrue(drv.awaitInvocation(2_000));
			awaitLifecycle(
							drv,
							() -> dispatched.lifecycle().state() == OperationLifecycleState.DISPATCHED,
							"operation should cross the actual dispatch boundary");

			drv.closeAdmission();
			assertTrue(drv.recoverQueuedOperations().isEmpty());
			assertNull(results.await(100), "active work must not be terminalized by admission closure");
			assertEquals(OperationLifecycleState.DISPATCHED, dispatched.lifecycle().state());
			assertEquals(1, drv.operationLifecycle().snapshot().inFlight());

			drv.allowCompletion();
			assertNotNull(results.await(2_000));
			awaitLifecycle(
							drv,
							() -> dispatched.lifecycle().state() == OperationLifecycleState.TERMINAL,
							"active work should remain runnable during the drain");
		} finally {
			drv.allowCompletion();
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	@Test
	void testBatchSubmitDistributesAcrossMultipleWorkers() throws Exception {
		final int workerCount = 4;
		final int opCount = 8;
		final var config = buildStorageConfig(8, workerCount);
		final var drv = newBlockingDriver(config);
		try {
			drv.start();
			final List<Operation<DataItemImpl>> ops = new ArrayList<>(opCount);
			for (int i = 0; i < opCount; i++) {
				ops.add(newCreateOp("fair-" + i, 256));
			}
			final int submitted = drv.submit(ops, 0, opCount);
			assertEquals(opCount, submitted);
			assertTrue(drv.awaitWorkerThreadCountAtLeast(2, 2_000),
							"Batch submit should distribute work so at least two workers execute invokeNio");
			drv.releaseInvokes();
			for (int i = 0; i < opCount; i++) {
				assertNotNull(results.await(5_000), "Op " + i + " should complete");
			}
		} finally {
			drv.releaseInvokes();
			if (drv.isStarted()) {
				drv.stop();
			}
			drv.close();
		}
	}

	// --- Test infrastructure ---

	/** Minimal storage config matching the driver constructor hierarchy. */
	private static Config buildStorageConfig() {
		return buildStorageConfig(4);
	}

	private static Config buildStorageConfig(int concurrency) {
		return buildStorageConfig(concurrency, 2);
	}

	private static Config buildStorageConfig(int concurrency, int driverThreads) {
		Map<String, Object> schema = new HashMap<>();
		schema.put("namespace", String.class);

		Map<String, Object> auth = new HashMap<>();
		auth.put("uid", String.class);
		auth.put("secret", String.class);
		auth.put("token", String.class);
		schema.put("auth", auth);

		Map<String, Object> limitQueue = new HashMap<>();
		limitQueue.put("input", Integer.class);
		Map<String, Object> limit = new HashMap<>();
		limit.put("concurrency", Integer.class);
		limit.put("queue", limitQueue);
		Map<String, Object> driver = new HashMap<>();
		driver.put("type", String.class);
		driver.put("threads", Integer.class);
		driver.put("limit", limit);
		schema.put("driver", driver);

		Map<String, Object> integrityInput = new HashMap<>();
		integrityInput.put("provenance", String.class);
		integrityInput.put("expectedProducerId", String.class);
		Map<String, Object> integrity = new HashMap<>();
		integrity.put("mode", String.class);
		integrity.put("algorithm", String.class);
		integrity.put("input", integrityInput);
		schema.put("integrity", integrity);

		Map<String, Object> netNode = new HashMap<>();
		netNode.put("slice", Boolean.class);
		Map<String, Object> net = new HashMap<>();
		net.put("node", netNode);
		schema.put("net", net);

		Map<String, Object> values = new HashMap<>();
		values.put("namespace", "");

		Map<String, Object> authVals = new HashMap<>();
		authVals.put("uid", "");
		authVals.put("secret", "");
		authVals.put("token", "");
		values.put("auth", authVals);

		Map<String, Object> limitQueueVals = new HashMap<>();
		limitQueueVals.put("input", 64);
		Map<String, Object> limitVals = new HashMap<>();
		limitVals.put("concurrency", concurrency);
		limitVals.put("queue", limitQueueVals);
		Map<String, Object> driverVals = new HashMap<>();
		driverVals.put("type", "dummy-mock");
		driverVals.put("threads", driverThreads);
		driverVals.put("limit", limitVals);
		values.put("driver", driverVals);

		Map<String, Object> integrityInputVals = new HashMap<>();
		integrityInputVals.put("provenance", "none");
		integrityInputVals.put("expectedProducerId", "");
		Map<String, Object> integrityVals = new HashMap<>();
		integrityVals.put("mode", "none");
		integrityVals.put("algorithm", "sha256");
		integrityVals.put("input", integrityInputVals);
		values.put("integrity", integrityVals);

		Map<String, Object> netNodeVals = new HashMap<>();
		netNodeVals.put("slice", false);
		Map<String, Object> netVals = new HashMap<>();
		netVals.put("node", netNodeVals);
		values.put("net", netVals);

		return new BasicConfig("-", schema, values);
	}

	private static void awaitLifecycle(
					final NioStorageDriverBase<?, ?> driver,
					final java.util.function.BooleanSupplier condition,
					final String message) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(condition.getAsBoolean(), message + "; lifecycle=" + driver.operationLifecycle().snapshot());
	}

	private static void awaitNioReservationsCleared(final NioStorageDriverBase<?, ?> driver) throws Exception {
		final var field = NioStorageDriverBase.class.getDeclaredField("opBuffReserved");
		field.setAccessible(true);
		final var reserved = (int[]) field.get(driver);
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (java.util.Arrays.stream(reserved).anyMatch(value -> value != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(java.util.Arrays.stream(reserved).allMatch(value -> value == 0),
						"NIO worker reservations should be cleared");
	}

	/** Collects completed operations for test assertions. */
	static class OpResultCollector implements Output<Operation<DataItemImpl>> {
		private final LinkedBlockingQueue<Operation<DataItemImpl>> queue = new LinkedBlockingQueue<>();

		@Override
		public boolean put(Operation<DataItemImpl> val) {
			return queue.offer(val);
		}

		@Override
		public int put(List<Operation<DataItemImpl>> vals, int from, int to) {
			int n = 0;
			for (int i = from; i < to; i++) {
				if (queue.offer(vals.get(i)))
					n++;
				else
					break;
			}
			return n;
		}

		@Override
		public int put(List<Operation<DataItemImpl>> vals) {
			return put(vals, 0, vals.size());
		}

		@Override
		public Input<Operation<DataItemImpl>> getInput() {
			return null;
		}

		@Override
		public void close() {
			queue.clear();
		}

		Operation<DataItemImpl> await(long timeoutMs) throws InterruptedException {
			return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
		}
	}

	@SuppressWarnings("unchecked")
	private static void assertWorkerTasksTerminated(final NioStorageDriverBase<?, ?> driver) throws Exception {
		final var field = NioStorageDriverBase.class.getDeclaredField("ioWorkers");
		field.setAccessible(true);
		final var workers = (List<Task>) field.get(driver);
		assertFalse(workers.isEmpty());
		for (final var worker : workers) {
			assertTrue(worker.await(10, TimeUnit.MILLISECONDS), "worker task thread should be terminated");
		}
	}

	static class RetainedActiveDriver extends NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> {
		private final AtomicInteger invocations = new AtomicInteger();
		private final AtomicBoolean completionAllowed = new AtomicBoolean();

		RetainedActiveDriver(
						final String testStepName,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize) throws Exception {
			super(testStepName, dataInput, storageConfig, verifyFlag, batchSize);
		}

		@Override
		protected void invokeNio(final Operation<DataItemImpl> op) {
			invocations.incrementAndGet();
			if (completionAllowed.get()) {
				super.invokeNio(op);
			} else {
				op.status(Operation.Status.ACTIVE);
				Thread.yield();
			}
		}

		boolean awaitInvocation(final long timeoutMs) throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
			while (invocations.get() == 0 && System.nanoTime() < deadline) {
				Thread.sleep(1);
			}
			return invocations.get() > 0;
		}

		void allowCompletion() {
			completionAllowed.set(true);
		}
	}

	static class ThrowOnceInvokeDriver extends NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> {
		private final AtomicBoolean shouldThrow = new AtomicBoolean(true);

		ThrowOnceInvokeDriver(
						final String testStepName,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize) throws Exception {
			super(testStepName, dataInput, storageConfig, verifyFlag, batchSize);
		}

		@Override
		protected void invokeNio(final Operation<DataItemImpl> op) {
			if (shouldThrow.compareAndSet(true, false)) {
				throw new IllegalStateException("invoke failed");
			}
			super.invokeNio(op);
		}
	}

	static class AlwaysThrowInvokeDriver extends NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> {

		AlwaysThrowInvokeDriver(
						final String testStepName,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize) throws Exception {
			super(testStepName, dataInput, storageConfig, verifyFlag, batchSize);
		}

		@Override
		protected void invokeNio(final Operation<DataItemImpl> op) {
			throw new IllegalStateException("invoke always fails");
		}
	}

	static class BlockingInvokeDriver extends NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>> {
		private final CountDownLatch invokeEntered = new CountDownLatch(1);
		private final CountDownLatch releaseInvoke = new CountDownLatch(1);
		private final Set<Long> workerThreadIds = ConcurrentHashMap.newKeySet();

		BlockingInvokeDriver(
						final String testStepName,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize) throws Exception {
			super(testStepName, dataInput, storageConfig, verifyFlag, batchSize);
		}

		@Override
		protected void invokeNio(final Operation<DataItemImpl> op) {
			workerThreadIds.add(Thread.currentThread().threadId());
			invokeEntered.countDown();
			try {
				releaseInvoke.await();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				op.status(Operation.Status.INTERRUPTED);
				return;
			}
			super.invokeNio(op);
		}

		boolean awaitInvokeEntered(final long timeoutMs) throws InterruptedException {
			return invokeEntered.await(timeoutMs, TimeUnit.MILLISECONDS);
		}

		boolean awaitWorkerThreadCountAtLeast(final int minCount, final long timeoutMs) throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
			while (System.nanoTime() < deadline) {
				if (workerThreadIds.size() >= minCount) {
					return true;
				}
				Thread.sleep(1);
			}
			return workerThreadIds.size() >= minCount;
		}

		void releaseInvokes() {
			releaseInvoke.countDown();
		}
	}
}
