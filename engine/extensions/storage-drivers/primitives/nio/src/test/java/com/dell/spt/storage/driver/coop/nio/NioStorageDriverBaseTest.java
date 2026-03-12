package com.dell.spt.storage.driver.coop.nio;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
		var drv = new NioStorageDriverMock<DataItemImpl, Operation<DataItemImpl>>(
						"test-nio", dataInput, storageConfig, false, 16);
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

	// --- Test infrastructure ---

	/** Minimal storage config matching the driver constructor hierarchy. */
	private static Config buildStorageConfig() {
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
		limitVals.put("concurrency", 4);
		limitVals.put("queue", limitQueueVals);
		Map<String, Object> driverVals = new HashMap<>();
		driverVals.put("type", "dummy-mock");
		driverVals.put("threads", 2);
		driverVals.put("limit", limitVals);
		values.put("driver", driverVals);

		Map<String, Object> netNodeVals = new HashMap<>();
		netNodeVals.put("slice", false);
		Map<String, Object> netVals = new HashMap<>();
		netVals.put("node", netNodeVals);
		values.put("net", netVals);

		return new BasicConfig("-", schema, values);
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
}
