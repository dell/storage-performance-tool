package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.RetryDelayScheduler;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The default retry scheduler, with real delays. The backoff's full jitter can draw 0, which runs
 * a retry inline on the caller, so each test sends enough failures that some are delayed.
 */
class LoadStepContextRetryDelayTest {

	private static final int FAILURES = 16;
	private static final long WAIT_MILLIS = 10_000;

	@TempDir
	Path tempDir;

	@Test
	void delayedRetryRunsOnTheSchedulerThread() {
		final var step = new Step("retry-delay-thread", true);
		step.failOperations(FAILURES);
		verify(step.generator, timeout(WAIT_MILLIS).times(FAILURES)).retry(any());
		step.assertDelayedWorkRanOnSchedulerThread(step.retryThreads);
	}

	@Test
	void retryScheduledAfterCancellationEndsAsCountedFailure() throws Exception {
		final var step = new Step("retry-delay-late", true);
		step.context.start();
		step.context.doStop();
		assertDoesNotThrow(() -> step.failOperations(FAILURES));
		verify(step.metrics, timeout(WAIT_MILLIS).times(FAILURES)).markFail();
		verify(step.generator, never()).retry(any());
		step.assertDelayedWorkRanOnSchedulerThread(step.failureThreads);
	}

	@Test
	void manyContextsShareOneSchedulerThread() throws Exception {
		final Set<Thread> delayed = Collections.newSetFromMap(new IdentityHashMap<>());
		for (var i = 0; i < 20; i++) {
			final var step = new Step("retry-delay-shared-" + i, true);
			step.context.start();
			step.failOperations(FAILURES);
			verify(step.generator, timeout(WAIT_MILLIS).times(FAILURES)).retry(any());
			step.assertDelayedWorkRanOnSchedulerThread(step.retryThreads);
			delayed.addAll(step.delayedRetryThreads());
			step.context.stop();
			step.context.close();
		}
		assertEquals(1, delayed.size(), "contexts that were started, stopped and closed must share one thread");
		assertTrue(schedulerThreads().contains(delayed.iterator().next()), "the shared thread must still run");
	}

	@Test
	void noSchedulerThreadBeforeTheFirstDelayedRetry() throws Exception {
		final var output = tempDir.resolve("first-use.log");
		final var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		final var process = new ProcessBuilder(
						java, "-cp", System.getProperty("java.class.path"), FirstUseProbe.class.getName())
						.directory(tempDir.toFile())
						.redirectErrorStream(true)
						.redirectOutput(output.toFile())
						.start();
		try {
			assertTrue(process.waitFor(120, TimeUnit.SECONDS), "first-use probe timed out");
		} finally {
			process.destroyForcibly();
		}
		final var log = Files.readString(output);
		assertEquals(0, process.exitValue(), log);
		assertTrue(log.contains(FirstUseProbe.PASSED), log);
	}

	private static List<Thread> schedulerThreads() {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(thread -> RetryDelayScheduler.THREAD_NAME.equals(thread.getName()))
						.toList();
	}

	private static long schedulerThreadCount() {
		return schedulerThreads().size();
	}

	/** Runs in its own JVM: in the test JVM another test may already have started the thread. */
	static final class FirstUseProbe {

		static final String PASSED = "first-use probe passed";

		public static void main(final String[] args) throws Exception {
			Class.forName(RetryDelayScheduler.class.getName());
			requireThreads("class load", 0);

			final var retryOff = new Step("first-use-retry-off", false);
			retryOff.context.start();
			requireThreads("start with retry off", 0);
			retryOff.failOperations(FAILURES);
			requireThreads("failures with retry off", 0);
			retryOff.context.doStop();

			final var retryOn = new Step("first-use-retry-on", true);
			retryOn.context.start();
			requireThreads("start with retry on", 0);
			retryOn.succeedOperations(FAILURES);
			requireThreads("successes with retry on", 0);

			retryOn.failOperations(FAILURES);
			requireThreads("failures with retry on", 1);
			retryOn.context.stop();
			retryOn.context.close();
			retryOff.context.close();
			System.out.println(PASSED);
			System.out.flush();
			// No exit call: the JVM must end on its own, which the daemon timer thread must not hold.
		}

		private static void requireThreads(final String stage, final long expected) {
			final long actual = schedulerThreadCount();
			if (actual != expected) {
				throw new AssertionError(stage + ": " + actual + " scheduler threads, expected " + expected);
			}
		}
	}

	private static final class Step {

		final LoadGenerator<DataItem, Operation<DataItem>> generator;
		final MetricsContext<AllMetricsSnapshot> metrics;
		final LoadStepContextImpl<DataItem, Operation<DataItem>> context;
		final Queue<String> retryThreads = new ConcurrentLinkedQueue<>();
		final Queue<Thread> retryThreadObjects = new ConcurrentLinkedQueue<>();
		final Queue<String> failureThreads = new ConcurrentLinkedQueue<>();

		@SuppressWarnings("unchecked")
		Step(final String id, final boolean retry) {
			final Config config = TestConfigBuilder.config();
			config.val("item-type", "data");
			config.val("load-op-retry", retry);
			config.val("load-op-retryLimit", 5);
			config.val("load-op-recycle-mode", false);
			config.val("load-op-wait-finish", false);
			generator = mock(LoadGenerator.class);
			when(generator.supportsRetry()).thenReturn(true);
			when(generator.isNothingPendingRetry()).thenReturn(true);
			doAnswer(call -> {
				retryThreadObjects.add(Thread.currentThread());
				return retryThreads.add(Thread.currentThread().getName());
			})
							.when(generator)
							.retry(any());
			metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);
			doAnswer(call -> failureThreads.add(Thread.currentThread().getName())).when(metrics).markFail();
			final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
			context = new LoadStepContextImpl<>(id, generator, driver, metrics, config.configVal("load"), false);
		}

		void failOperations(final int count) {
			for (var i = 0; i < count; i++) {
				final var op = operation("failed-" + i);
				op.status(Operation.Status.RESP_FAIL_SVC);
				assertTrue(context.put((Operation<DataItem>) op));
			}
		}

		void succeedOperations(final int count) {
			for (var i = 0; i < count; i++) {
				final var op = operation("succeeded-" + i);
				op.reset();
				op.startRequest();
				op.finishRequest();
				op.startResponse();
				op.finishResponse();
				op.status(Operation.Status.SUCC);
				assertTrue(context.put((Operation<DataItem>) op));
			}
		}

		void assertDelayedWorkRanOnSchedulerThread(final Queue<String> threads) {
			final var caller = Thread.currentThread().getName();
			final List<String> delayed = threads.stream().filter(name -> !caller.equals(name)).toList();
			assertFalse(delayed.isEmpty(), "every retry ran inline");
			delayed.forEach(name -> assertEquals(RetryDelayScheduler.THREAD_NAME, name));
		}

		/** The retries that did not run inline on the calling thread. */
		List<Thread> delayedRetryThreads() {
			final var caller = Thread.currentThread();
			return retryThreadObjects.stream().filter(thread -> thread != caller).toList();
		}

		private static DataOperation<DataItem> operation(final String name) {
			final DataItem item = new DataItemImpl(name, 0, 64);
			return new DataOperationImpl<>(0, OpType.CREATE, item, "/in", "/out", null, List.of(), 0);
		}
	}
}
