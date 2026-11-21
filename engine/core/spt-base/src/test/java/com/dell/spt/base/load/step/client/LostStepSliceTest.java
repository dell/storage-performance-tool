package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

/**
 * An entry that loses a remote step slice, whose await, stop, close or metrics fetch fails in RMI,
 * fails the step: the totals can leave out all of that worker's operations. The step's other slices still stop
 * and close, and the entry logs one error per lost slice, naming its node. A stop or await whose
 * response comes after the RMI response timeout is a slow call, not a loss, and gets a warning.
 *
 * <p>The client step, the slices and their RMI over the loopback interface are the real ones; a
 * proxy around each remote slice's stub records its calls and injects the RMI failures. A slow call
 * goes through a real RMI relay whose client sockets time out their reads, as the shipped
 * {@code sun.rmi.transport.tcp.responseTimeout} makes them, and which answers late.
 */
@Timeout(180)
class LostStepSliceTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final String LOST = "lost the step slice on node ";
	private static final long LOG_WAIT_MILLIS = 5_000;
	/** The relay's read timeout, standing in for the shipped 10 s RMI response timeout. */
	private static final int RESPONSE_TIMEOUT_MILLIS = 300;
	/**
	 * How long a locked await poll waits on its lock's condition, so that a poll of that slice is in
	 * flight when its stop comes; see awaitsWaitForStop.
	 */
	private static final long AWAIT_WAIT_MILLIS = 50;
	/** When a slow call's relay answers, after the slice's call has run. */
	private static final long LATE_ANSWER_MILLIS = 1_500;
	private static final String STOP_WARNING = "failed to stop the step slice";
	private static final String CLOSE_WARNING = "failed to close the step service";
	private static final String AWAIT_WARNING = "did not answer its await within the RMI response timeout";
	private static final String METRICS_FETCH = "metricsSnapshots";
	private static final String FILES_NOT_COLLECTED = "the entry collects none of the files of node ";
	/** The key of the entry's own slice in the call times. */
	private static final String ENTRY = "entry";
	/** How soon a step whose slice is lost must end: the ticket's 10 s, less the test's own setup. */
	private static final long LOST_STEP_END_MILLIS = 10_000;
	/** The longest any step of these tests runs before the test fails instead of waiting on. */
	private static final Duration RUN_BOUND = Duration.ofSeconds(45);
	/** The longest the cleanup of a step that did not end may take. */
	private static final Duration CLOSE_BOUND = Duration.ofSeconds(30);

	private final Config config = TestConfigBuilder.config();
	private final MetricsManagerImpl entryMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<AutoCloseable> closeables = new ArrayList<>();
	/** The calls each remote slice got, by its node address. */
	private final Map<String, List<String>> calls = new ConcurrentHashMap<>();
	/**
	 * Every call each remote slice and its node's file manager got once the entry had recorded that
	 * slice as lost, by node address; a file manager's call is named "file:" and its method.
	 */
	private final Map<String, List<String>> callsAfterLoss = new ConcurrentHashMap<>();
	/** The step under test, whose record of its lost slices the recording reads. */
	private volatile LoadStepClientBase<?> client;
	/** The injected failure by node address and call. */
	private final Map<String, Map<String, RemoteException>> faults = new ConcurrentHashMap<>();
	/** The calls, by node address, that go through a relay answering after the response timeout. */
	private final Map<String, Set<String>> slowCalls = new ConcurrentHashMap<>();
	/** The calls, by node address, whose first call only goes through such a relay. */
	private final Map<String, Set<String>> slowOnceCalls = new ConcurrentHashMap<>();
	/** The calls, by node address, that fail as RMI fails a connect that nothing listens for. */
	private final Map<String, Set<String>> refusedCalls = new ConcurrentHashMap<>();
	/** The nodes whose slice's await answers done, as a worker's await does once its exit hook stopped it. */
	private final Set<String> doneAwaits = ConcurrentHashMap.newKeySet();
	/** The nodes whose await has answered done. */
	private final Set<String> answeredDone = ConcurrentHashMap.newKeySet();
	/** The nodes whose metrics fetch is refused once their await answered done, as after the worker exited. */
	private final Set<String> refusedFetchesAfterDone = ConcurrentHashMap.newKeySet();
	/** The nodes whose metrics fetch has been tried after their slice was lost. */
	private final Map<String, Integer> fetches = new ConcurrentHashMap<>();
	/** The nodes whose metrics fetch is refused once their slice got its stop, as when the worker closes it. */
	private final Set<String> refusedFetchesAfterStop = ConcurrentHashMap.newKeySet();
	/** The fetches refused that way, by node address. */
	private final Map<String, Integer> refusedAfterStop = new ConcurrentHashMap<>();
	/** The nodes whose file service closes at their slice's stop, so the close cannot collect their files. */
	private final Set<String> filesGoneAtStop = ConcurrentHashMap.newKeySet();
	/** When each node's slice first got each call, by "address#call". */
	private final Map<String, Long> firstCallNanos = new ConcurrentHashMap<>();
	/** Each node's success counts from its metrics fetches, with when each fetch answered. */
	private final Map<String, List<long[]>> successCounts = new ConcurrentHashMap<>();
	/** How long the entry's own slice's first await takes, as a drain inside the await; 0: none. */
	private volatile long localDrainMillis;
	/** When the entry's own slice's slow await began and ended. */
	private final long[] localDrainNanos = new long[2];
	/** Whether the entry's own slice is wrapped, to record its calls and sample its successes. */
	private volatile boolean recordLocal;
	/** The entry's own slice, once the step has created it. */
	private volatile LoadStep localSlice;
	/** How long each node's stop takes to answer after the slice has stopped, as a worker's drain; by node address. */
	private final Map<String, Long> slowStopReplies = new ConcurrentHashMap<>();
	/**
	 * The nodes whose slice's await waits while its stop is in flight, as a worker's await waits on
	 * the slice's state lock that its stop holds for the drain; the slow stop reply is part of that stop.
	 */
	private final Set<String> awaitsWaitForStop = ConcurrentHashMap.newKeySet();
	/** How many stops each node's slice got, by node address. */
	private final Map<String, Integer> stops = new ConcurrentHashMap<>();
	/** The worker services by node address, so that a test can take a worker down. */
	private final Map<String, List<AutoCloseable>> workerServices = new ConcurrentHashMap<>();
	/** Optional per-node interceptor for {@code await} calls, used to pause polling deterministically. */
	private final Map<String, AwaitHook> awaitHooks = new ConcurrentHashMap<>();
	private final List<String> addrs = new ArrayList<>();
	private String stepId;
	private String originalRmiHostname;
	private ErrorCapture errors;
	private Logger errLogger;
	private Level errLevel;

	@BeforeEach
	void configure() throws Exception {
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		stepId = "lost-slice-" + System.nanoTime();
		errors = new ErrorCapture(stepId);
		errors.start();
		errLogger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		errLogger.addAppender(errors);
		// Another test may have left the logger above WARN, which would drop the slow-stop warning.
		errLevel = errLogger.getLevel();
		errLogger.setLevel(Level.WARN);

		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("output-metrics-timing-persist", false);
		config.val("output-metrics-trace-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("item-input-file", null);
		config.val("item-input-path", null);
		config.val("load-op-type", "noop");
		config.val("load-op-limit-count", 30L);
		config.val("load-op-limit-rate", 100.0);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 1);
		startWorkers(2);
	}

	@AfterEach
	void closeAll() throws Exception {
		try {
			errLogger.removeAppender(errors);
			errLogger.setLevel(errLevel);
			errors.stop();
			// A step that did not end must not hang the test JVM in its cleanup: it fails the test instead.
			assertTimeoutPreemptively(CLOSE_BOUND, () -> {
				for (final AutoCloseable closeable : closeables.reversed()) {
					closeable.close();
				}
			}, "the cleanup did not end");
		} finally {
			ServiceUtil.shutdown();
			if (originalRmiHostname == null) {
				System.clearProperty(RMI_HOSTNAME);
			} else {
				System.setProperty(RMI_HOSTNAME, originalRmiHostname);
			}
		}
	}

	@Test
	void aStopFailingOnOneOfTwoSlicesFailsTheStepAndTheOtherStillStopsAndCloses() throws Exception {
		final String lost = addrs.get(1);
		final String other = addrs.get(0);
		fault(lost, "stop", new ConnectException("injected: worker unreachable"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 3);
		assertTrue(calls(other).contains("stop"), "the other slice is stopped: " + calls(other));
		assertTrue(calls(other).contains("close"), "the other slice is closed: " + calls(other));
		assertTrue(calls(lost).contains("close"), "the lost slice still gets its close: " + calls(lost));
		assertEquals(List.of(lost), lostNodes(), "one error, naming the lost slice's node");
	}

	@Test
	void aSliceLostAtItsAwaitGetsOneCloseAndNoOtherCall() throws Exception {
		final String lost = addrs.get(0);
		fault(lost, "await", new ConnectException("injected: worker unreachable"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertOnlyOneCloseAfterLoss(lost);
		assertFalse(calls(lost).contains("stop"), "a lost slice gets no stop: " + calls(lost));
		assertEquals(List.of(lost), lostNodes());
	}

	@Test
	void aReleasedSliceIsLostOnceAcrossItsStopAndClose() throws Exception {
		final String lost = addrs.get(0);
		fault(lost, "stop", new NoSuchObjectException("injected: no such object in table"));
		fault(lost, "close", new NoSuchObjectException("injected: no such object in table"));
		fault(lost, "shutdown", new NoSuchObjectException("injected: no such object in table"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertEquals(0, failure.getSuppressed().length, "one failure for the one lost slice");
		assertEquals(List.of(lost), lostNodes(), "one error line for the slice");
		assertFalse(
						errors.messages().stream().anyMatch(message -> message.contains("failed to close the step service")
										|| message.contains("failed to stop the step slice")
										|| message.contains("failed to shutdown the step service")),
						"no second error for the lost slice: " + errors.messages());
	}

	@Test
	void slicesThatAnswerAfterANormalEndAreNotLost() throws Exception {
		final LinearLoadStepClient client = newClient();
		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			client.run();
		}

		assertNull(client.runFailure());
		for (final String addr : addrs) {
			assertTrue(calls(addr).containsAll(List.of("start", "await", "stop", "close")), addr + ": " + calls(addr));
		}
		assertEquals(List.of(), lostNodes());
	}

	@Test
	void aSliceThatNeverStartedIsNotReportedLostInTheTeardown() throws Exception {
		final String failed = addrs.get(1);
		fault(failed, "start", new ConnectException("injected: worker unreachable"));
		fault(failed, "stop", new ConnectException("injected: worker unreachable"));
		fault(failed, "close", new ConnectException("injected: worker unreachable"));
		final LinearLoadStepClient client = newClient();

		final Throwable failure;
		failure = assertInstanceOf(RunTerminalException.class, runBounded(client));

		assertFalse(failure.getMessage().contains("lost this node's step slice"), failure.getMessage());
		LogUtil.flushAll();
		Thread.sleep(200);
		assertEquals(List.of(), lostNodes(), "the start failure is the step's failure");
	}

	@Test
	void aSliceLostAtItsCloseOnlyFailsTheStep() throws Exception {
		final String lost = addrs.get(1);
		fault(lost, "close", new ConnectException("injected: worker unreachable"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 3);
		assertTrue(calls(lost).contains("stop"), "its stop answered: " + calls(lost));
		assertEquals(List.of(lost), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(" at its close (")),
						errors.messages().toString());
		// Its close was its one close: the loss sends it no second one.
		assertEquals(List.of(), callsAfterLoss(lost), "no call after a loss at the close");
	}

	@Test
	void aStopAnsweringAfterTheResponseTimeoutWarnsAndTheStepEndsAsBefore() throws Exception {
		final String slow = addrs.get(1);
		slow(slow, "stop");
		final LinearLoadStepClient client = newClient();

		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			client.run();
		}

		assertNull(client.runFailure(), "a slow stop does not fail the step");
		assertTrue(client.isClosed());
		awaitWarning(STOP_WARNING);
		assertEquals(List.of(), lostNodes(), "the slice is not lost");
		final List<String> stopWarnings = errors.messages().stream()
						.filter(message -> message.contains(STOP_WARNING))
						.toList();
		// The entry stops its slices from its await and again from its own stop, and the relay answers
		// each late, so each gives this warning.
		assertFalse(stopWarnings.isEmpty(), errors.messages().toString());
		assertTrue(stopWarnings.stream().allMatch(message -> message.contains("recording slice @ " + slow)),
						stopWarnings.toString());
		assertTrue(calls(slow).contains("close"), "the slow slice is still closed: " + calls(slow));
	}

	@Test
	void aCloseTimingOutAfterASlowStopOfTheSameSliceWarnsToo() throws Exception {
		final String slow = addrs.get(0);
		slow(slow, "stop");
		slow(slow, "close");
		final LinearLoadStepClient client = newClient();

		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			client.run();
		}

		assertNull(client.runFailure(), "the close waits for the same slow stop on the worker");
		awaitWarning(CLOSE_WARNING);
		assertEquals(List.of(), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(CLOSE_WARNING)),
						errors.messages().toString());
	}

	@Test
	void aCloseTimingOutAfterAStopThatAnsweredLosesTheSlice() throws Exception {
		final String lost = addrs.get(0);
		slow(lost, "close");

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertEquals(List.of(lost), lostNodes());
		assertTrue(failure.getCause() instanceof UnmarshalException, String.valueOf(failure.getCause()));
	}

	@Test
	void anAwaitAnsweringAfterTheResponseTimeoutWarnsAndTheStepEndsAsBefore() throws Exception {
		final String slow = addrs.get(1);
		// The relay's await waits until the slice is done, as a pipeline stage's drain inside the
		// worker's await call does, then answers after the response timeout.
		slow(slow, "await");
		final LinearLoadStepClient client = newClient();

		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			client.run();
		}

		assertNull(client.runFailure(), "a slow await does not fail the step");
		assertTrue(client.isClosed());
		awaitWarning(AWAIT_WARNING);
		assertEquals(List.of(), lostNodes(), "the slice is not lost");
		final List<String> awaitWarnings = errors.messages().stream()
						.filter(message -> message.contains(AWAIT_WARNING))
						.toList();
		assertEquals(1, awaitWarnings.size(), errors.messages().toString());
		assertTrue(awaitWarnings.getFirst().contains("recording slice @ " + slow), awaitWarnings.toString());
		assertTrue(calls(slow).containsAll(List.of("await", "stop", "close")),
						"the entry goes on to stop and close the slice: " + calls(slow));
		assertEquals(30L, client.metricsSnapshots().getFirst().successSnapshot().count(), "the totals are complete");
	}

	@Test
	void aWorkerThatDiesDuringItsAwaitStillFailsTheStepAndNamesIt() throws Exception {
		final String lost = addrs.get(1);
		// What RMI gives the caller when the worker's process ends during the call.
		fault(lost, "await", new UnmarshalException("Error unmarshaling return header", new EOFException()));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 3);
		assertEquals(List.of(lost), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(LOST + lost + " at its await (")),
						errors.messages().toString());
		assertFalse(errors.messages().stream().anyMatch(message -> message.contains(AWAIT_WARNING)),
						errors.messages().toString());
	}

	@Test
	void aCloseTimingOutAfterALaterStopAnsweredLosesTheSlice() throws Exception {
		final String lost = addrs.get(0);
		// The entry stops its slices from its await and again from its own stop: the first stop is
		// slow, the second answers, so the close no longer waits behind a slow stop.
		slowOnce(lost, "stop");
		slow(lost, "close");

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertEquals(List.of(lost), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(" at its close (")),
						errors.messages().toString());
		assertEquals(1, errors.messages().stream().filter(message -> message.contains(STOP_WARNING)).count(),
						"only the first stop timed out: " + errors.messages());
	}

	@Test
	void aSliceLostAtItsAwaitGetsNoWarningAtItsSlowStopAndClose() throws Exception {
		final String lost = addrs.get(0);
		fault(lost, "await", new ConnectException("injected: worker unreachable"));
		slow(lost, "stop");
		slow(lost, "close");

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertEquals(0, failure.getSuppressed().length, "one failure for the one lost slice");
		assertEquals(List.of(lost), lostNodes());
		assertFalse(errors.messages().stream().anyMatch(message -> message.contains(CLOSE_WARNING)
						|| message.contains(STOP_WARNING)), "no second line for the lost slice: " + errors.messages());
	}

	@Test
	void aStopRefusedByTheWorkerStillFailsTheStepAndNamesTheWorker() throws Exception {
		final String lost = addrs.get(1);
		refused(lost, "stop");

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 3);
		assertTrue(failure.getCause() instanceof ConnectException, String.valueOf(failure.getCause()));
		assertEquals(List.of(lost), lostNodes());
	}

	@Test
	void aLostSliceIsReportedOnOneLineWithItsNodeAndTheTotals() throws Exception {
		final String lost = addrs.get(0);
		fault(lost, "stop", new UnmarshalException("injected: error unmarshaling return",
						new EOFException("injected\nsecond line")));

		runFailing();

		final String line = errors.messages().stream()
						.filter(message -> message.contains(LOST))
						.findFirst()
						.orElseThrow();
		assertFalse(line.contains("\n") || line.contains("\r"), line);
		assertTrue(line.contains(LOST + lost + " at its stop ("), line);
		assertTrue(line.contains("injected second line"), line);
		assertTrue(line.endsWith("the step's totals can leave out all of that node's operations, so the step fails"), line);
	}

	@Test
	@Timeout(60)
	void aSliceThatAnsweredDoneAndThenRefusesItsMetricsFetchStopsAStepWithNoLimit() throws Exception {
		unlimited();
		final String lost = addrs.get(1);
		// A worker's exit hook stops its slice, whose await then answers done, and the worker exits.
		doneAwaits.add(lost);
		refusedFetchesAfterDone.add(lost);

		final long began = System.nanoTime();
		final RunTerminalException failure = runFailing();
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertTrue(answeredDone.contains(lost), "its await answered done");
		assertLostSliceFailure(failure, 3);
		assertTrue(failure.getCause() instanceof ConnectException, String.valueOf(failure.getCause()));
		assertTrue(tookMillis < LOST_STEP_END_MILLIS, "the step stops at once: " + tookMillis + " ms");
		assertEquals(List.of(lost), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(LOST + lost + " at its metrics fetch (")),
						errors.messages().toString());
		final String other = addrs.get(0);
		assertTrue(calls(other).containsAll(List.of("stop", "close")), "the other slice stops and closes: " + calls(other));
	}

	@Test
	@Timeout(60)
	void aReleasedSliceAtItsMetricsFetchStopsAStepWithNoLimitAndGetsNoFurtherFetch() throws Exception {
		unlimited();
		final String lost = addrs.get(0);
		fault(lost, METRICS_FETCH, new NoSuchObjectException("injected: no such object in table"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertTrue(failure.getCause() instanceof NoSuchObjectException, String.valueOf(failure.getCause()));
		assertEquals(List.of(lost), lostNodes());
		final int fetchesAfterTheStep = fetches.getOrDefault(lost, 0);
		Thread.sleep(500);
		assertEquals(fetchesAfterTheStep, fetches.getOrDefault(lost, 0), "no fetch after the step ended");
		// The step's own metrics output and the aggregator each made at most the one fetch that lost it.
		assertTrue(fetchesAfterTheStep <= 2, "a lost slice gets no further fetch: " + fetchesAfterTheStep);
	}

	@Test
	@Timeout(60)
	void aWorkerThatDiesDuringAMetricsFetchStopsAStepWithNoLimit() throws Exception {
		unlimited();
		final String lost = addrs.get(1);
		// What RMI gives the caller when the worker's process ends during the call.
		fault(lost, METRICS_FETCH, new UnmarshalException("Error unmarshaling return header", new EOFException()));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 3);
		assertEquals(List.of(lost), lostNodes());
	}

	@Test
	@Timeout(60)
	void aMetricsFetchAnsweringAfterTheResponseTimeoutLosesTheSliceAndStopsAStepWithNoLimit() throws Exception {
		unlimited();
		final String slow = addrs.get(1);
		// A worker that stops answering, as one frozen: its fetch's response read times out.
		slow(slow, METRICS_FETCH);

		final long began = System.nanoTime();
		final RunTerminalException failure = runFailing();
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertLostSliceFailure(failure, 3);
		assertTrue(LoadStepClientBase.isResponseReadTimeout((RemoteException) failure.getCause()),
						"lost at a response read timeout: " + failure.getCause());
		assertTrue(tookMillis < LOST_STEP_END_MILLIS, "the step stops at once: " + tookMillis + " ms");
		assertEquals(List.of(slow), lostNodes());
		assertTrue(errors.messages().stream().anyMatch(message -> message.contains(LOST + slow + " at its metrics fetch (")),
						errors.messages().toString());
		assertOnlyOneCloseAfterLoss(slow);
	}

	@Test
	@Timeout(60)
	void aMetricsFetchAnsweringAfterTheResponseTimeoutFailsATimeLimitedStepBeforeItsLimit() throws Exception {
		config.val("load-op-limit-count", 0L);
		config.val("load-step-limit-time", "30s");
		final String slow = addrs.get(0);
		slow(slow, METRICS_FETCH);

		final long began = System.nanoTime();
		final RunTerminalException failure = runFailing();
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertLostSliceFailure(failure, 2);
		assertTrue(tookMillis < LOST_STEP_END_MILLIS, "the loss ends the step long before its 30 s limit: " + tookMillis + " ms");
		assertEquals(List.of(slow), lostNodes());
	}

	@Test
	@Timeout(60)
	void aLiveWorkerLostAtItsMetricsFetchGetsNoFileCallAndTheStepFailsForItsLoss() throws Exception {
		unlimited();
		// A CREATE collects every node's operation.lifecycle.csv and NIC files at the close.
		config.val("load-op-type", "create");
		config.val("item-data-size", "1KiB");
		config.val("item-output-path", "/l56-lost-files");
		final String lost = addrs.get(1);
		faults.computeIfAbsent(lost, key -> new ConcurrentHashMap<>());
		final Thread injector = Thread.ofPlatform().daemon(true).start(() -> {
			try {
				while (!calls(lost).contains("await")) {
					Thread.sleep(10);
				}
				Thread.sleep(500);
				fault(lost, METRICS_FETCH, new ConnectException("injected: connection reset"));
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});

		final LinearLoadStepClient client = newClient();
		final RunTerminalException failure = assertInstanceOf(RunTerminalException.class, runBounded(client));
		injector.join();
		awaitLostNodes();

		// The close finds no lifecycle file of the lost node, and that failure follows the step's.
		assertLostSliceFailure(failure, 3);
		assertSame(failure, client.runFailure());
		assertEquals(List.of(lost), lostNodes());
		assertOnlyOneCloseAfterLoss(lost);
		final String other = addrs.get(0);
		assertTrue(calls(other).containsAll(List.of("stop", "close")), "the other slice stops and closes: " + calls(other));
		// One warning for the lost node's files, however many of them the close skipped.
		final List<String> notCollected = errors.messages().stream().filter(message -> message.contains(FILES_NOT_COLLECTED)).toList();
		assertEquals(1, notCollected.size(), "one warning for the lost node's files: " + notCollected);
		assertTrue(notCollected.getFirst().contains(FILES_NOT_COLLECTED + lost + ","), notCollected.getFirst());
	}

	@Test
	@Timeout(60)
	void aWorkerTakenDownMidStepStopsAStepWithNoLimitAndItsLossIsTheStepsFailure() throws Exception {
		unlimited();
		// A CREATE aggregates every node's operation.lifecycle.csv at the close.
		config.val("load-op-type", "create");
		config.val("item-data-size", "1KiB");
		config.val("item-output-path", "/l56-lost-slice");
		final String lost = addrs.get(1);
		final LinearLoadStepClient client = newClient();
		final long[] killedNanos = new long[1];
		killedNanos[0] = System.nanoTime();
		final Thread killer = Thread.ofPlatform().daemon(true).start(() -> {
			try {
				// Mid-step: after the slice's start has returned and the step has run a while.
				while (!calls(lost).contains("await")) {
					Thread.sleep(10);
				}
				Thread.sleep(500);
				killedNanos[0] = System.nanoTime();
				for (final AutoCloseable service : workerServices.get(lost)) {
					service.close();
				}
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		});

		final RunTerminalException failure;
		failure = assertInstanceOf(RunTerminalException.class, runBounded(client));
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedNanos[0]);
		killer.join();
		awaitLostNodes();

		// The worker's missing files fail the close too, which must not replace the step's failure.
		assertLostSliceFailure(failure, 3);
		assertTrue(tookMillis < LOST_STEP_END_MILLIS, "the step stops at once: " + tookMillis + " ms after the worker went");
		assertEquals(List.of(lost), lostNodes());
		assertSame(failure, client.runFailure());
		assertTrue(Arrays.stream(failure.getSuppressed()).anyMatch(suppressed -> suppressed instanceof RunTerminalException
						&& suppressed.getMessage().startsWith("failed to aggregate ")),
						"the close's failure follows the step's: " + Arrays.toString(failure.getSuppressed()));
	}

	@Test
	@Timeout(60)
	void aSliceLostAtItsAwaitStopsAStepWithNoLimit() throws Exception {
		unlimited();
		final String lost = addrs.get(0);
		fault(lost, "await", new ConnectException("injected: worker unreachable"));

		final long began = System.nanoTime();
		final RunTerminalException failure = runFailing();
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		assertLostSliceFailure(failure, 2);
		assertTrue(tookMillis < LOST_STEP_END_MILLIS, "the step stops at once: " + tookMillis + " ms");
		final String other = addrs.get(1);
		assertTrue(calls(other).containsAll(List.of("stop", "close")), "the other slice stops and closes: " + calls(other));
	}

	@Test
	@Timeout(60)
	void aWorkerTakenDownInATimeLimitedStepFailsItWithItsLossBeforeTheLimit() throws Exception {
		config.val("load-op-limit-count", 0L);
		config.val("load-step-limit-time", "8s");
		config.val("load-op-type", "create");
		config.val("item-data-size", "1KiB");
		config.val("item-output-path", "/l56-lost-slice");
		final String lost = addrs.get(0);
		final LinearLoadStepClient client = newClient();
		final long[] killedNanos = new long[1];
		final Thread killer = takeDownAfterItsAwait(lost, killedNanos);

		final RunTerminalException failure;
		failure = assertInstanceOf(RunTerminalException.class, runBounded(client));
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedNanos[0]);
		killer.join();
		awaitLostNodes();

		// The worker's missing files fail the close too, which must not replace the step's failure.
		assertLostSliceFailure(failure, 2);
		assertEquals(List.of(lost), lostNodes());
		assertTrue(tookMillis < 5_000, "the step stops before its 8 s limit: " + tookMillis + " ms after the worker went");
	}

	private Thread takeDownAfterItsAwait(final String addr, final long[] killedNanos) {
		return Thread.ofPlatform().daemon(true).start(() -> {
			try {
				while (!calls(addr).contains("await")) {
					Thread.sleep(10);
				}
				Thread.sleep(500);
				killedNanos[0] = System.nanoTime();
				for (final AutoCloseable service : workerServices.get(addr)) {
					service.close();
				}
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		});
	}

	@Test
	@Timeout(60)
	void aSliceLostWhileNoPollerReachesItStillStopsAStepWithNoLimit() throws Exception {
		unlimited();
		final String lost = addrs.get(1);
		fault(lost, METRICS_FETCH, new ConnectException("injected: worker unreachable"));
		final LinearLoadStepClient client = newClient();

		// One poller: the await polls one slice, which with no limit never ends, and no other.
		final ForkJoinPool onePoller = new ForkJoinPool(1);
		final RunTerminalException failure;
		try {
			// The slices resolve on the step's thread, which is the pool's.
			failure = assertThrows(RunTerminalException.class, () -> {
				try {
					onePoller.submit(() -> {
						try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
							client.run();
						}
					}).get(RUN_BOUND.toMillis(), TimeUnit.MILLISECONDS);
				} catch (final ExecutionException e) {
					throw e.getCause();
				}
			});
		} finally {
			onePoller.shutdownNow();
		}
		awaitLostNodes();

		assertLostSliceFailure(failure, 3);
		assertEquals(List.of(lost), lostNodes());
	}

	@Test
	void aSliceLostWhileTheEntrysOwnSliceDrainsStopsTheOtherWorkerAtOnce() throws Exception {
		unlimited();
		final String lost = addrs.get(0);
		final String other = addrs.get(1);
		// The entry's own slice drains inside its await, as a pipeline stage that ends at a size limit
		// does, while the worker goes.
		localDrainMillis = 6_000;
		final LinearLoadStepClient client = newClient();
		final long[] killedNanos = new long[1];
		final Thread killer = takeDownAfterItsAwait(lost, killedNanos);

		final Throwable failure = runBounded(client);
		final long endedNanos = System.nanoTime();
		killer.join();
		awaitLostNodes();

		assertLostSliceFailure(assertInstanceOf(RunTerminalException.class, failure), 2);
		assertEquals(List.of(lost), lostNodes());
		assertTrue(localDrainNanos[0] != 0 && localDrainNanos[0] < killedNanos[0] && killedNanos[0] < localDrainNanos[1],
						"the worker went while the entry's own slice drained");
		final long stopAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(firstCallNanos.get(other + "#stop") - killedNanos[0]);
		final long exitAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(endedNanos - killedNanos[0]);
		final long opsAfterLoss = successesAfter(other, killedNanos[0]);
		System.out.printf(
						"lost-slice stop measurement: the other worker's stop %d ms after the loss, its successes after the loss %d, the step's end %d ms after the loss%n",
						stopAfterLossMillis, opsAfterLoss, exitAfterLossMillis);
		assertTrue(stopAfterLossMillis < 2_000,
						"the other worker is stopped at once, not after the entry's drain: " + stopAfterLossMillis + " ms");
	}

	@Test
	void aSliceLostWhileAnotherWorkersStopDrainsStopsTheEntrysOwnSliceAtOnce() throws Exception {
		unlimited();
		config.val("load-op-limit-rate", 90.0);
		final String lost = addrs.get(0);
		final String draining = addrs.get(1);
		// The other worker has stopped, but answers its stop only after a 5 s drain, as a worker whose
		// target holds the replies of its last requests.
		slowStopReplies.put(draining, 5_000L);
		recordLocal = true;
		final LinearLoadStepClient client = newClient();
		final long[] killedNanos = new long[1];
		final Thread killer = takeDownAfterItsAwait(lost, killedNanos);
		final List<long[]> localSuccesses = Collections.synchronizedList(new ArrayList<>());
		final AtomicBoolean sampling = new AtomicBoolean(true);
		final Thread sampler = Thread.ofPlatform().daemon(true).start(() -> sampleSuccesses(localSuccesses, sampling));

		final Throwable failure;
		try {
			failure = runBounded(client);
		} finally {
			sampling.set(false);
			sampler.join();
		}
		killer.join();
		awaitLostNodes();

		assertLostSliceFailure(assertInstanceOf(RunTerminalException.class, failure), 2);
		assertEquals(List.of(lost), lostNodes());
		final long drainingStopNanos = firstCallNanos.get(draining + "#stop");
		final long localStopNanos = firstCallNanos.get(ENTRY + "#stop");
		final long localStopAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(localStopNanos - killedNanos[0]);
		final long localSuccessesAfterLoss = successesAfter(localSuccesses, killedNanos[0]);
		System.out.printf(
						"lost-slice drain measurement: the other worker's stop %d ms after the loss, the entry's own stop %d ms after it, the entry's successes after the loss %d%n",
						TimeUnit.NANOSECONDS.toMillis(drainingStopNanos - killedNanos[0]), localStopAfterLossMillis,
						localSuccessesAfterLoss);
		assertTrue(localStopAfterLossMillis < 2_000,
						"the entry's own slice stops at once, not after the other worker's drain: " + localStopAfterLossMillis + " ms");
		// The entry runs a third of 90 op/s: 2 s of its load is 60 operations, and the 5 s drain about 150.
		assertTrue(localSuccessesAfterLoss < 60,
						"the entry's own slice stops generating at the loss: " + localSuccessesAfterLoss + " successes after it");
		assertEquals(1, stopCalls(draining), "the draining worker is stopped once");
	}

	@Test
	void aSliceLostWhileAnotherWorkersAwaitWaitsForItsStopStopsTheEntrysOwnSliceAtOnce() throws Exception {
		unlimited();
		config.val("load-op-limit-rate", 90.0);
		final String lost = addrs.get(0);
		final String draining = addrs.get(1);
		// The other worker's stop drains for 5 s, and its await waits for that stop, as a worker's await
		// waits on the slice's state lock that its stop holds for the drain: the entry's poll of that
		// worker does not answer until the stop has.
		slowStopReplies.put(draining, 5_000L);
		awaitsWaitForStop.add(draining);
		recordLocal = true;
		final LinearLoadStepClient client = newClient();
		final long[] killedNanos = new long[1];
		final Thread killer = takeDownAfterItsAwait(lost, killedNanos);
		final List<long[]> localSuccesses = Collections.synchronizedList(new ArrayList<>());
		final AtomicBoolean sampling = new AtomicBoolean(true);
		final Thread sampler = Thread.ofPlatform().daemon(true).start(() -> sampleSuccesses(localSuccesses, sampling));

		final Throwable failure;
		try {
			failure = runBounded(client);
		} finally {
			sampling.set(false);
			sampler.join();
		}
		killer.join();
		awaitLostNodes();

		assertLostSliceFailure(assertInstanceOf(RunTerminalException.class, failure), 2);
		assertEquals(List.of(lost), lostNodes());
		final long drainingStopNanos = firstCallNanos.get(draining + "#stop");
		final long localStopNanos = firstCallNanos.get(ENTRY + "#stop");
		final long localStopAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(localStopNanos - killedNanos[0]);
		final long localSuccessesAfterLoss = successesAfter(localSuccesses, killedNanos[0]);
		System.out.printf(
						"lost-slice locked-await measurement: the other worker's stop %d ms after the loss, the entry's own stop %d ms after it, the entry's successes after the loss %d%n",
						TimeUnit.NANOSECONDS.toMillis(drainingStopNanos - killedNanos[0]), localStopAfterLossMillis,
						localSuccessesAfterLoss);
		assertTrue(localStopAfterLossMillis < 2_000,
						"the entry's own slice stops at once, not after the other worker's await answers: " + localStopAfterLossMillis
										+ " ms");
		// The entry runs a third of 90 op/s: 2 s of its load is 60 operations, and the 5 s drain about 150.
		assertTrue(localSuccessesAfterLoss < 60,
						"the entry's own slice stops generating at the loss: " + localSuccessesAfterLoss + " successes after it");
		assertEquals(1, stopCalls(draining), "the draining worker is stopped once");
		assertEquals(1, stopCalls(ENTRY), "the entry's own slice is stopped once");
		assertEquals(List.of(), errors.messages().stream().filter(m -> m.contains(AWAIT_WARNING)).toList(),
						"no await warning for the draining worker");
	}

	@Test
	void aLiveWorkerLostAtItsMetricsFetchIsStoppedAtOnceWhileAnotherWorkersStopDrains() throws Exception {
		unlimited();
		final String lost = addrs.get(0);
		final String draining = addrs.get(1);
		// The lost worker stays up and generating: only its metrics fetch fails, as on a reset connection.
		faults.computeIfAbsent(lost, key -> new ConcurrentHashMap<>());
		// The other worker's stop drains for 5 s, and its await waits for that stop.
		slowStopReplies.put(draining, 5_000L);
		awaitsWaitForStop.add(draining);
		final LinearLoadStepClient client = newClient();
		final long[] lostNanos = new long[1];
		final Thread injector = Thread.ofPlatform().daemon(true).start(() -> {
			try {
				while (!calls(lost).contains("await")) {
					Thread.sleep(10);
				}
				Thread.sleep(500);
				lostNanos[0] = System.nanoTime();
				fault(lost, METRICS_FETCH, new ConnectException("injected: connection reset"));
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});

		final Throwable failure = runBounded(client);
		injector.join();
		awaitLostNodes();

		assertLostSliceFailure(assertInstanceOf(RunTerminalException.class, failure), 2);
		assertEquals(List.of(lost), lostNodes());
		// Its one close stops it on the worker, as a close stops a slice first.
		final long lostCloseAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(firstCallNanos.get(lost + "#close") - lostNanos[0]);
		System.out.printf("lost-slice live-lost measurement: the lost worker's close %d ms after its fetch failed%n",
						lostCloseAfterLossMillis);
		assertTrue(lostCloseAfterLossMillis < 2_000,
						"the live lost worker is closed at once, not after the other worker's drain: " + lostCloseAfterLossMillis + " ms");
		assertEquals(0, stopCalls(lost), "the lost worker gets no stop");
		assertOnlyOneCloseAfterLoss(lost);
		assertEquals(1, stopCalls(draining), "the draining worker is stopped once");
	}

	private int stopCalls(final String addr) {
		return stops.getOrDefault(addr, 0);
	}

	/** Samples the entry's own slice's success count every 20 ms while {@code sampling} holds. */
	private void sampleSuccesses(final List<long[]> samples, final AtomicBoolean sampling) {
		while (sampling.get()) {
			final LoadStep local = localSlice;
			if (local != null) {
				try {
					final List<? extends AllMetricsSnapshot> snapshots = local.metricsSnapshots();
					if (snapshots != null && !snapshots.isEmpty() && snapshots.getFirst().successSnapshot() != null) {
						samples.add(new long[]{System.nanoTime(), snapshots.getFirst().successSnapshot().count()
						});
					}
				} catch (final Exception e) {
					// A closed slice has no metrics.
				}
			}
			try {
				Thread.sleep(20);
			} catch (final InterruptedException e) {
				return;
			}
		}
	}

	@Test
	void aMetricsFetchFailingOnceTheStepStopsDoesNotLoseTheSlice() throws Exception {
		final String closing = addrs.get(1);
		// A fetch still in flight, or the terminal metrics read, as the worker closes its slice.
		refusedFetchesAfterStop.add(closing);
		final LinearLoadStepClient client = newClient();

		assertNull(runBounded(client));

		assertNull(client.runFailure(), "a fetch that fails after the stop is no loss");
		assertTrue(refusedAfterStop.getOrDefault(closing, 0) > 0, "a fetch was refused after the stop: " + calls(closing));
		LogUtil.flushAll();
		Thread.sleep(200);
		assertEquals(List.of(), lostNodes());
	}

	@Test
	void aSliceLostAtItsStopStaysTheStepsFailureWhenTheCloseFailsToCollectItsFiles() throws Exception {
		// A CREATE aggregates every node's operation.lifecycle.csv at the close.
		config.val("load-op-type", "create");
		config.val("item-data-size", "1KiB");
		config.val("item-output-path", "/l56-lost-slice");
		final String lost = addrs.get(1);
		fault(lost, "stop", new ConnectException("injected: worker unreachable"));
		filesGoneAtStop.add(lost);
		final LinearLoadStepClient client = newClient();

		final RunTerminalException failure = assertInstanceOf(RunTerminalException.class, runBounded(client));
		awaitLostNodes();

		// The loss is first found at the stop; the close's terminal failure follows from it.
		assertLostSliceFailure(failure, 3);
		assertEquals(List.of(lost), lostNodes());
		assertSame(failure, client.runFailure());
		assertTrue(Arrays.stream(failure.getSuppressed()).anyMatch(suppressed -> suppressed instanceof RunTerminalException
						&& suppressed.getMessage().startsWith("failed to aggregate ")),
						"the close's failure follows the step's: " + Arrays.toString(failure.getSuppressed()));
	}

	@Test
	void aSliceLostAtItsAwaitGetsNoFurtherMetricsFetch() throws Exception {
		unlimited();
		final String lost = addrs.get(0);
		fault(lost, "await", new ConnectException("injected: worker unreachable"));

		runFailing();

		final List<String> lostCalls = calls(lost);
		final int awaitAt = lostCalls.indexOf("await");
		assertTrue(awaitAt >= 0, lostCalls.toString());
		assertFalse(lostCalls.subList(awaitAt, lostCalls.size()).contains(METRICS_FETCH),
						"no metrics fetch after the slice was lost at its await: " + lostCalls);
	}

	@Test
	void onlyAResponseReadTimeoutCountsAsASlowCall() {
		final SocketTimeoutException readTimedOut = new SocketTimeoutException("Read timed out");
		assertTrue(LoadStepClientBase.isResponseReadTimeout(
						new UnmarshalException("Error unmarshaling return header", readTimedOut)));
		assertTrue(LoadStepClientBase.isResponseReadTimeout(new UnmarshalException(
						"Error unmarshaling return", new IOException("wrapped", readTimedOut))));

		final java.net.ConnectException connectTimedOut = new java.net.ConnectException("Connect timed out");
		connectTimedOut.initCause(new SocketTimeoutException("Connect timed out"));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(new ConnectException("connect", connectTimedOut)));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(
						new ConnectIOException("handshake", new SocketTimeoutException("Read timed out"))));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(
						new ConnectException("refused", new java.net.ConnectException("Connection refused"))));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(
						new UnmarshalException("Error unmarshaling return header", new EOFException())));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(new UnmarshalException(
						"Error unmarshaling return header", new SocketException("Connection reset"))));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(new UnmarshalException("no cause")));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(new NoSuchObjectException("no such object in table")));
		assertFalse(LoadStepClientBase.isResponseReadTimeout(new RemoteException("other", readTimedOut)));
	}

	/** A step with neither a count nor a time limit, which only a stop or a failure ends. */
	private void unlimited() {
		config.val("load-op-limit-count", 0L);
		config.val("load-step-limit-time", 0);
	}

	@Test
	void exitHookCloseDuringAwaitDoesNotRecordALostSlice() throws Exception {
		final String worker = addrs.get(0);
		config.val("load-step-node-addrs", List.of(worker));
		config.val("load-op-limit-count", 10_000L);
		config.val("load-op-limit-rate", 10.0);
		final CountDownLatch secondAwaitEntered = new CountDownLatch(1);
		final CountDownLatch releaseSecondAwait = new CountDownLatch(1);
		final AtomicInteger awaitCount = new AtomicInteger();
		final AtomicReference<Throwable> observedAwaitFailure = new AtomicReference<>();
		awaitHooks.put(worker, new AwaitHook() {
			@Override
			public void beforeAwait() throws InterruptedException {
				if (awaitCount.incrementAndGet() >= 2) {
					secondAwaitEntered.countDown();
					assertTrue(releaseSecondAwait.await(15, TimeUnit.SECONDS), "the test released the await poll");
				}
			}

			@Override
			public void onFailure(final Throwable cause) {
				observedAwaitFailure.compareAndSet(null, cause);
			}
		});
		final LinearLoadStepClient client = newClient();
		final CompletableFuture<Boolean> awaited = new CompletableFuture<>();
		try {
			try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
				client.start();
			}
			Thread.ofPlatform().name("scenario").daemon(true).start(() -> {
				try {
					awaited.complete(client.await(1, TimeUnit.MINUTES));
				} catch (final Throwable e) {
					awaited.completeExceptionally(e);
				}
			});
			assertTrue(secondAwaitEntered.await(15, TimeUnit.SECONDS), "the scenario thread polled the worker slice");

			JvmExit.markBegun();
			assertDoesNotThrow(client::close);
			assertTrue(client.isClosed());

			releaseSecondAwait.countDown();
			final ExecutionException failure = assertThrows(
							ExecutionException.class, () -> awaited.get(15, TimeUnit.SECONDS));
			assertInstanceOf(InterruptedException.class, failure.getCause(), String.valueOf(failure.getCause()));
			assertEquals(client.loadStepId() + ": stopped, the engine is exiting", failure.getCause().getMessage());
			assertInstanceOf(
							NoSuchObjectException.class,
							observedAwaitFailure.get(),
							String.valueOf(observedAwaitFailure.get()));
			LogUtil.flushAll();
			Thread.sleep(200);
			assertEquals(List.of(), lostNodes(), "the exit-hook close must not record a lost slice");
		} finally {
			releaseSecondAwait.countDown();
			JvmExitTestSupport.reset();
		}
	}

	private RunTerminalException runFailing() throws Exception {
		final LinearLoadStepClient client = newClient();
		final RunTerminalException failure;
		failure = assertInstanceOf(RunTerminalException.class, runBounded(client));
		assertTrue(client.isClosed(), "the step still closes");
		awaitLostNodes();
		return failure;
	}

	private void assertLostSliceFailure(final RunTerminalException failure, final int engine) {
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals(stepId, failure.stepId());
		assertEquals(
						"engine " + engine + " of 3: the entry lost this node's step slice, so the step's totals can leave out all of its operations; see the entry's log",
						failure.getMessage());
		assertTrue(failure.getCause() instanceof RemoteException, String.valueOf(failure.getCause()));
	}

	/**
	 * Runs the step on a pool of its own, with a poller for each slice whatever the host's CPU count,
	 * and returns its failure, or null. A step that never ends fails the test at {@link #RUN_BOUND},
	 * and the pool is shut down, interrupting its await's pollers: JUnit's own timeout interrupts the
	 * test's thread, which does not wake a thread parked in the await's parallel stream. The slices
	 * resolve on the step's thread, so the recording is set up there.
	 */
	private Throwable runBounded(final LinearLoadStepClient client) throws Exception {
		final ForkJoinPool pollers = new ForkJoinPool(addrs.size() + 1);
		try {
			return pollers.submit(() -> {
				try (
								MockedStatic<LoadStepSliceUtil> util = recordingSlices();
								MockedStatic<LoadStepFactory> local = localDrainMillis > 0 || recordLocal ? drainingLocalSlice() : null) {
					client.run();
					return null;
				} catch (final Throwable failure) {
					return failure;
				}
			}).get(RUN_BOUND.toMillis(), TimeUnit.MILLISECONDS);
		} catch (final TimeoutException e) {
			throw new AssertionError("the step did not end within " + RUN_BOUND, e);
		} finally {
			pollers.shutdownNow();
		}
	}

	/**
	 * Wraps the entry's own slice, whose first await then takes {@link #localDrainMillis}, and records
	 * when each of its calls first came, under {@link #ENTRY}.
	 */
	private MockedStatic<LoadStepFactory> drainingLocalSlice() {
		final MockedStatic<LoadStepFactory> factory = mockStatic(LoadStepFactory.class, CALLS_REAL_METHODS);
		factory.when(() -> LoadStepFactory.createLocalLoadStep(any(), anyList(), anyList(), any(), anyString()))
						.thenAnswer(invocation -> {
							final LoadStep local = (LoadStep) invocation.callRealMethod();
							localSlice = local;
							final InvocationHandler handler = (proxy, method, args) -> {
								if (method.getDeclaringClass() == Object.class) {
									return switch (method.getName()) {
									case "equals" -> proxy == args[0];
									case "hashCode" -> System.identityHashCode(proxy);
									default -> "draining local slice";
									};
								}
								firstCallNanos.putIfAbsent(ENTRY + "#" + method.getName(), System.nanoTime());
								if ("stop".equals(method.getName())) {
									stops.merge(ENTRY, 1, Integer::sum);
								}
								if ("await".equals(method.getName()) && localDrainMillis > 0 && localDrainNanos[0] == 0) {
									localDrainNanos[0] = System.nanoTime();
									Thread.sleep(localDrainMillis);
									localDrainNanos[1] = System.nanoTime();
								}
								try {
									return method.invoke(local, args);
								} catch (final InvocationTargetException e) {
									throw e.getCause();
								}
							};
							return Proxy.newProxyInstance(
											LoadStep.class.getClassLoader(), new Class<?>[]{LoadStep.class
							}, handler);
						});
		return factory;
	}

	private LinearLoadStepClient newClient() {
		final LinearLoadStepClient client = new LinearLoadStepClient(config, extensions(), List.of(), entryMgr) {
			@Override
			FileManager resolveFileManager(final String nodeAddr) {
				final FileManager files = LoadStepClientBase.resolveFileManagerWithRetries(
								nodeAddr, 10, FileManagerClient::resolve, Thread::sleep);
				return files == null ? null : recordingFiles(nodeAddr, (FileManagerService) files);
			}
		};
		this.client = client;
		closeables.add(() -> {
			if (!client.isClosed()) {
				client.close();
			}
		});
		return client;
	}

	private void fault(final String addr, final String call, final RemoteException failure) {
		faults.computeIfAbsent(addr, key -> new ConcurrentHashMap<>()).put(call, failure);
	}

	private void slow(final String addr, final String call) {
		slowCalls.computeIfAbsent(addr, key -> ConcurrentHashMap.newKeySet()).add(call);
	}

	private void slowOnce(final String addr, final String call) {
		slowOnceCalls.computeIfAbsent(addr, key -> ConcurrentHashMap.newKeySet()).add(call);
	}

	private void refused(final String addr, final String call) {
		refusedCalls.computeIfAbsent(addr, key -> ConcurrentHashMap.newKeySet()).add(call);
	}

	private void awaitWarning(final String text) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LOG_WAIT_MILLIS);
		while (errors.messages().stream().noneMatch(message -> message.contains(text))
						&& System.nanoTime() < deadline) {
			LogUtil.flushAll();
			Thread.sleep(10);
		}
		// Room for a wrong lost-slice line to arrive.
		Thread.sleep(200);
	}

	private List<String> calls(final String addr) {
		synchronized (calls.get(addr)) {
			return List.copyOf(calls.get(addr));
		}
	}

	/**
	 * Records a call to the node's slice or file manager that came once the entry had recorded the
	 * slice as lost.
	 */
	private void recordIfLost(final String addr, final String name) {
		final LoadStepClientBase<?> step = client;
		if (step != null && lostNodeIndexes(step).contains(addrs.indexOf(addr) + 1)) {
			callsAfterLoss.computeIfAbsent(addr, key -> Collections.synchronizedList(new ArrayList<>())).add(name);
		}
	}

	@SuppressWarnings("unchecked")
	private static Set<Integer> lostNodeIndexes(final LoadStepClientBase<?> step) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("lostNodeIndexes");
			field.setAccessible(true);
			return (Set<Integer>) field.get(step);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError("the step's record of its lost slices", e);
		}
	}

	private List<String> callsAfterLoss(final String addr) {
		final List<String> after = callsAfterLoss.getOrDefault(addr, List.of());
		synchronized (after) {
			return List.copyOf(after);
		}
	}

	/**
	 * Asserts that the entry made no call to the lost slice once it had recorded it as lost, but its
	 * one close: no stop, shutdown, remote loadStepId, terminal metrics fetch or file call. An await
	 * poll and a metrics fetch that were already past their check of the loss may still arrive, one
	 * each.
	 */
	private void assertOnlyOneCloseAfterLoss(final String lost) {
		final List<String> after = callsAfterLoss(lost);
		assertEquals(1, after.stream().filter("close"::equals).count(), "one close after the loss: " + after);
		final List<String> others = after.stream()
						.filter(call -> !"close".equals(call))
						.toList();
		assertTrue(others.stream().filter("await"::equals).count() <= 1, "no await poll after the loss: " + after);
		assertTrue(others.stream().filter(METRICS_FETCH::equals).count() <= 1, "no metrics fetch after the loss: " + after);
		assertEquals(List.of(), others.stream().filter(call -> !"await".equals(call) && !METRICS_FETCH.equals(call)).toList(),
						"no other call after the loss: " + after);
	}

	/** Wraps a node's file manager so that the calls it gets after the slice's loss are recorded. */
	private FileManager recordingFiles(final String addr, final FileManagerService files) {
		final InvocationHandler handler = (proxy, method, args) -> {
			if (method.getDeclaringClass() == Object.class) {
				return switch (method.getName()) {
				case "equals" -> proxy == args[0];
				case "hashCode" -> System.identityHashCode(proxy);
				default -> "recording files @ " + addr;
				};
			}
			recordIfLost(addr, "file:" + method.getName());
			try {
				return method.invoke(files, args);
			} catch (final InvocationTargetException e) {
				throw e.getCause();
			}
		};
		return (FileManager) Proxy.newProxyInstance(
						FileManagerService.class.getClassLoader(), new Class<?>[]{FileManagerService.class
						}, handler);
	}

	/** Wraps each remote slice's stub, on this thread, where the step resolves its slices. */
	private MockedStatic<LoadStepSliceUtil> recordingSlices() {
		final MockedStatic<LoadStepSliceUtil> util = mockStatic(LoadStepSliceUtil.class, CALLS_REAL_METHODS);
		util.when(() -> LoadStepSliceUtil.resolveRemote(any(), anyList(), anyString(), anyString()))
						.thenAnswer(invocation -> {
							final LoadStepService stub = (LoadStepService) invocation.callRealMethod();
							final String addr = invocation.getArgument(3);
							return stub == null ? null : recording(addr, stub);
						});
		return util;
	}

	private LoadStepService recording(final String addr, final LoadStepService stub) {
		final List<String> sliceCalls = calls.get(addr);
		final Map<String, RemoteException> sliceFaults = faults.getOrDefault(addr, Map.of());
		final ReentrantLock stateLock = new ReentrantLock();
		final Condition stateChanged = stateLock.newCondition();
		final InvocationHandler handler = (proxy, method, args) -> {
			final String name = method.getName();
			if (method.getDeclaringClass() == Object.class) {
				return switch (name) {
				case "equals" -> proxy == args[0];
				case "hashCode" -> System.identityHashCode(proxy);
				default -> "recording slice @ " + addr;
				};
			}
			synchronized (sliceCalls) {
				if (sliceCalls.isEmpty() || !name.equals(sliceCalls.getLast())) {
					sliceCalls.add(name);
				}
			}
			recordIfLost(addr, name);
			firstCallNanos.putIfAbsent(addr + "#" + name, System.nanoTime());
			if ("stop".equals(name)) {
				stops.merge(addr, 1, Integer::sum);
			}
			if (METRICS_FETCH.equals(name)) {
				fetches.merge(addr, 1, Integer::sum);
				if (refusedFetchesAfterDone.contains(addr) && answeredDone.contains(addr)) {
					throw refusedConnect();
				}
				if (refusedFetchesAfterStop.contains(addr) && firstCallNanos.containsKey(addr + "#stop")) {
					refusedAfterStop.merge(addr, 1, Integer::sum);
					throw refusedConnect();
				}
			}
			if ("stop".equals(name) && filesGoneAtStop.contains(addr)) {
				workerServices.get(addr).get(1).close();
			}
			if ("await".equals(name)) {
				final AwaitHook hook = awaitHooks.get(addr);
				if (hook != null) {
					hook.beforeAwait();
				}
			}
			final RemoteException fault = sliceFaults.get(name);
			if (fault != null) {
				throw fault;
			}
			if ("await".equals(name) && doneAwaits.contains(addr)) {
				answeredDone.add(addr);
				return true;
			}
			if (slowCalls.getOrDefault(addr, Set.of()).contains(name)
							|| slowOnceCalls.getOrDefault(addr, ConcurrentHashMap.newKeySet()).remove(name)) {
				relay(stub, LATE_ANSWER_MILLIS).call(name);
				return null;
			}
			if (refusedCalls.getOrDefault(addr, Set.of()).contains(name)) {
				throw refusedConnect();
			}
			final boolean locked = awaitsWaitForStop.contains(addr) && ("await".equals(name) || "stop".equals(name));
			if (locked) {
				stateLock.lock();
				if ("await".equals(name)) {
					// As the worker's await: a timed wait that gives the lock up and takes it back at its
					// end, so it waits there while a stop holds the lock.
					var remainingNanos = TimeUnit.MILLISECONDS.toNanos(AWAIT_WAIT_MILLIS);
					while (remainingNanos > 0) {
						remainingNanos = stateChanged.awaitNanos(remainingNanos);
					}
				}
			}
			final Object result;
			try {
				try {
					result = method.invoke(stub, args);
				} catch (final InvocationTargetException e) {
					if ("await".equals(name)) {
						final AwaitHook hook = awaitHooks.get(addr);
						if (hook != null) {
							hook.onFailure(e.getCause());
						}
					}
					throw e.getCause();
				}
				final Long stopReplyMillis = slowStopReplies.get(addr);
				if ("stop".equals(name) && stopReplyMillis != null) {
					Thread.sleep(stopReplyMillis);
				}
			} finally {
				if (locked) {
					stateLock.unlock();
				}
			}
			if (METRICS_FETCH.equals(name) && result instanceof List<?> list && !list.isEmpty()
							&& list.getFirst() instanceof AllMetricsSnapshot snapshot) {
				successCounts.computeIfAbsent(addr, key -> Collections.synchronizedList(new ArrayList<>()))
								.add(new long[]{System.nanoTime(), snapshot.successSnapshot().count()});
			}
			return result;
		};
		return (LoadStepService) Proxy.newProxyInstance(
						LoadStepService.class.getClassLoader(), new Class<?>[]{LoadStepService.class
						}, handler);
	}

	private interface AwaitHook {
		void beforeAwait() throws InterruptedException;

		void onFailure(Throwable cause);
	}

	/** A remote call to the slice, made by the relay's server side; see {@link #relay}. */
	public interface Relay extends Remote {
		void call(String method) throws RemoteException;
	}

	/**
	 * Exports a relay to the slice on a listener of its own, and returns its stub. The relay runs the
	 * named call on the slice, then answers after {@code answerAfterMillis}; its client sockets time
	 * out a read after {@link #RESPONSE_TIMEOUT_MILLIS}, so the caller gets RMI's own failure.
	 */
	private Relay relay(final LoadStepService slice, final long answerAfterMillis) throws RemoteException {
		final Relay impl = method -> {
			try {
				LoadStepService.class.getMethod(method).invoke(slice);
				Thread.sleep(answerAfterMillis);
			} catch (final InvocationTargetException e) {
				if (e.getCause() instanceof RemoteException remote) {
					throw remote;
				}
				throw new RemoteException("relayed " + method, e.getCause());
			} catch (final ReflectiveOperationException | InterruptedException e) {
				throw new RemoteException("relay " + method, e);
			}
		};
		final Relay stub = (Relay) UnicastRemoteObject.exportObject(
						impl, 0, new ReadTimeoutClientSockets(RESPONSE_TIMEOUT_MILLIS), null);
		closeables.add(() -> UnicastRemoteObject.unexportObject(impl, true));
		return stub;
	}

	/** Client sockets whose reads time out, as {@code sun.rmi.transport.tcp.responseTimeout} sets them. */
	private record ReadTimeoutClientSockets(int readTimeoutMillis) implements RMIClientSocketFactory, Serializable {
		@Override
		public Socket createSocket(final String host, final int port) throws IOException {
			final Socket socket = new Socket(host, port);
			socket.setSoTimeout(readTimeoutMillis);
			return socket;
		}
	}

	/**
	 * RMI's own failure for a worker that no longer listens: a call through a stub for a loopback
	 * port that this test holds bound, without listening, until the call has failed, so no other
	 * process can take the port and listen on it meanwhile. A closed relay would not do: RMI serves
	 * a call by object id on any connection it still holds, so a stub can reach an object whose
	 * listener has closed.
	 */
	private static RemoteException refusedConnect() throws IOException {
		try (Socket bound = new Socket()) {
			bound.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			final int port = bound.getLocalPort();
			try {
				LocateRegistry.getRegistry("127.0.0.1", port).list();
			} catch (final ConnectException refused) {
				return refused;
			}
			throw new IllegalStateException("something listens on the bound port " + port);
		}
	}

	private void startWorkers(final int count) throws Exception {
		for (int i = 0; i < count; i++) {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
			final FileManagerServiceImpl files = new FileManagerServiceImpl(port);
			files.start();
			closeables.add(() -> {
				if (!files.isClosed()) {
					files.close();
				}
			});
			final LoadStepManagerServiceImpl steps = new LoadStepManagerServiceImpl(port, extensions(), metrics);
			steps.start();
			final AutoCloseable stepsCloser = () -> {
				if (!steps.isClosed()) {
					steps.close();
				}
			};
			closeables.add(stepsCloser);
			final String addr = "127.0.0.1:" + port;
			workerServices.put(addr, List.of(stepsCloser, () -> {
				if (!files.isClosed()) {
					files.close();
				}
			}));
			addrs.add(addr);
			calls.put(addr, Collections.synchronizedList(new ArrayList<>()));
		}
		config.val("load-step-node-addrs", List.copyOf(addrs));
	}

	/** A node's successes from its last fetch before {@code sinceNanos} to its last fetch. */
	private long successesAfter(final String addr, final long sinceNanos) {
		return successesAfter(successCounts.getOrDefault(addr, List.of()), sinceNanos);
	}

	/** The successes from the last count before {@code sinceNanos} to the highest count. */
	private static long successesAfter(final List<long[]> counts, final long sinceNanos) {
		synchronized (counts) {
			long before = 0;
			long last = 0;
			for (final long[] count : counts) {
				if (count[0] <= sinceNanos) {
					before = count[1];
				}
				last = Math.max(last, count[1]);
			}
			return last - before;
		}
	}

	/** The node of each lost-slice error, in order. */
	private List<String> lostNodes() {
		final List<String> nodes = new ArrayList<>();
		for (final String message : errors.messages()) {
			final int at = message.indexOf(LOST);
			if (at >= 0) {
				final int start = at + LOST.length();
				nodes.add(message.substring(start, message.indexOf(' ', start)));
			}
		}
		return nodes;
	}

	private void awaitLostNodes() throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LOG_WAIT_MILLIS);
		while (lostNodes().isEmpty() && System.nanoTime() < deadline) {
			LogUtil.flushAll();
			Thread.sleep(10);
		}
		// Room for a second, wrong, line to arrive.
		Thread.sleep(200);
	}

	private static List<Extension> extensions() {
		return List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
	}

	private static final class ErrorCapture extends AbstractAppender {
		private final String stepId;
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final Set<Level> levels = Set.of(Level.ERROR, Level.WARN);

		ErrorCapture(final String stepId) {
			super("lostSliceErrorCapture", null, null, true, Property.EMPTY_ARRAY);
			this.stepId = stepId;
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (levels.contains(event.getLevel()) && message.startsWith(stepId + ":")) {
				captured.add(message);
			}
		}

		List<String> messages() {
			synchronized (captured) {
				return List.copyOf(captured);
			}
		}
	}
}
