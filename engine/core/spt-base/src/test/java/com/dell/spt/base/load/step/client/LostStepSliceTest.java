package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
 * An entry that loses a remote step slice, whose await, stop or close fails in RMI, fails the step:
 * that worker's last operations are missing from the totals. The step's other slices still stop
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
	/** When a slow call's relay answers, after the slice's call has run. */
	private static final long LATE_ANSWER_MILLIS = 1_500;
	private static final String STOP_WARNING = "failed to stop the step slice";
	private static final String CLOSE_WARNING = "failed to close the step service";
	private static final String AWAIT_WARNING = "did not answer its await within the RMI response timeout";

	private final Config config = TestConfigBuilder.config();
	private final MetricsManagerImpl entryMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<AutoCloseable> closeables = new ArrayList<>();
	/** The calls each remote slice got, by its node address. */
	private final Map<String, List<String>> calls = new ConcurrentHashMap<>();
	/** The injected failure by node address and call. */
	private final Map<String, Map<String, RemoteException>> faults = new ConcurrentHashMap<>();
	/** The calls, by node address, that go through a relay answering after the response timeout. */
	private final Map<String, Set<String>> slowCalls = new ConcurrentHashMap<>();
	/** The calls, by node address, whose first call only goes through such a relay. */
	private final Map<String, Set<String>> slowOnceCalls = new ConcurrentHashMap<>();
	/** The calls, by node address, that fail as RMI fails a connect that nothing listens for. */
	private final Map<String, Set<String>> refusedCalls = new ConcurrentHashMap<>();
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
			for (final AutoCloseable closeable : closeables.reversed()) {
				closeable.close();
			}
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
	void aSliceLostAtItsAwaitStaysLostWhenItsStopAndCloseAnswer() throws Exception {
		final String lost = addrs.get(0);
		fault(lost, "await", new ConnectException("injected: worker unreachable"));

		final RunTerminalException failure = runFailing();

		assertLostSliceFailure(failure, 2);
		assertTrue(calls(lost).contains("stop"), calls(lost).toString());
		assertTrue(calls(lost).contains("close"), calls(lost).toString());
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
		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			failure = assertThrows(RunTerminalException.class, client::run);
		}

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
		assertTrue(line.endsWith("are missing from the step's totals, so the step fails"), line);
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

	private RunTerminalException runFailing() throws Exception {
		final LinearLoadStepClient client = newClient();
		final RunTerminalException failure;
		try (MockedStatic<LoadStepSliceUtil> util = recordingSlices()) {
			failure = assertThrows(RunTerminalException.class, client::run);
		}
		assertTrue(client.isClosed(), "the step still closes");
		awaitLostNodes();
		return failure;
	}

	private void assertLostSliceFailure(final RunTerminalException failure, final int engine) {
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals(stepId, failure.stepId());
		assertEquals(
						"engine " + engine + " of 3: the entry lost this node's step slice, so its last operations are missing from the step's totals; see the entry's log",
						failure.getMessage());
		assertTrue(failure.getCause() instanceof RemoteException, String.valueOf(failure.getCause()));
	}

	private LinearLoadStepClient newClient() {
		final LinearLoadStepClient client = new LinearLoadStepClient(config, extensions(), List.of(), entryMgr);
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
			final RemoteException fault = sliceFaults.get(name);
			if (fault != null) {
				throw fault;
			}
			if (slowCalls.getOrDefault(addr, Set.of()).contains(name)
							|| slowOnceCalls.getOrDefault(addr, ConcurrentHashMap.newKeySet()).remove(name)) {
				relay(stub, LATE_ANSWER_MILLIS).call(name);
				return null;
			}
			if (refusedCalls.getOrDefault(addr, Set.of()).contains(name)) {
				throw refusedConnect();
			}
			try {
				return method.invoke(stub, args);
			} catch (final InvocationTargetException e) {
				throw e.getCause();
			}
		};
		return (LoadStepService) Proxy.newProxyInstance(
						LoadStepService.class.getClassLoader(), new Class<?>[]{LoadStepService.class
						}, handler);
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
			closeables.add(() -> {
				if (!steps.isClosed()) {
					steps.close();
				}
			});
			final String addr = "127.0.0.1:" + port;
			addrs.add(addr);
			calls.put(addr, Collections.synchronizedList(new ArrayList<>()));
		}
		config.val("load-step-node-addrs", List.copyOf(addrs));
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
