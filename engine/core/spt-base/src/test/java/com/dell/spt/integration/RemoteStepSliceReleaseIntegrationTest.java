package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.svc.ServiceUtil;
import com.dell.spt.testing.tags.IntegrationTest;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * A long-lived worker over repeated step slices whose stop and close fail: each slice's queued
 * operations, its step service and its metrics are released, so the worker's live heap, its
 * threads and its exported services stay flat across steps.
 *
 * <p>The fault is the one a worker met on 5.17.0: an old {@code operation.lifecycle.csv} in the step's
 * folder, written here after the slice starts so the start check lets it run. The slice's stop then
 * refuses to publish; the stop its close repeats does nothing, and the close releases the slice and
 * throws that refusal. The steps' operations are a ballast
 * of {@link DataOperationImpl}, as a driver's input queue holds them, reachable only through the
 * slice's context.
 */
@IntegrationTest
class RemoteStepSliceReleaseIntegrationTest {

	private static final String STEP_TYPE = "release-ballast";
	private static final int FAILING_STEPS = 5;
	private static final int BALLAST_OPERATIONS = 200_000;
	private static final long HEAP_GROWTH_LIMIT_BYTES = 16L << 20;
	private static final String ENTRY_SILENCE_PROPERTY = "spt.step.entrySilenceTimeoutMillis";
	private static final long ENTRY_SILENCE_MILLIS = 2_000;
	/**
	 * The limit for the slice polled every 100 ms: a stall of the polling thread on a loaded runner
	 * must not reach it, so it is 50 polls long, and the test still runs past it and the start.
	 */
	private static final long POLLED_ENTRY_SILENCE_MILLIS = 5_000;

	private final List<WeakReference<List<DataOperationImpl<DataItemImpl>>>> ballasts = new ArrayList<>();
	private final List<String> stepIds = new ArrayList<>();
	private final AtomicInteger contextCloses = new AtomicInteger();

	@Test
	void aWorkerShutdownReleasesASliceInAnyState() throws Exception {
		withWorker((worker, manager, port) -> {
			final LoadStepService unstarted = newSlice(manager, port);
			final LoadStepService stopped = newSlice(manager, port);
			stopped.start();
			stopped.stop();
			final LoadStepService abandoned = newSlice(manager, port);
			abandoned.start();
			final LoadStepService latest = newSlice(manager, port);
			latest.start();
			assertEquals(2, worker.busySliceRunIds().size(), "only the started and unstopped slices are busy");

			worker.close();

			// A slice builds its contexts when it starts, so the unstarted one has none to close.
			assertEquals(3, contextCloses.get(), "every started slice's context closes once");
			assertEquals(List.of(), List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()));
			for (final LoadStepService slice : List.of(unstarted, stopped, abandoned, latest)) {
				assertThrows(java.rmi.RemoteException.class, slice::loadStepId);
			}
			assertEquals(List.of(), worker.busySliceRunIds());
		});
	}

	@Test
	void aWorkerShutdownRacingLateControllerCallsClosesEachSliceOnce() throws Exception {
		withWorker((worker, manager, port) -> {
			final List<LoadStepService> slices = new ArrayList<>();
			for (var i = 0; i < 4; i++) {
				final LoadStepService slice = newSlice(manager, port);
				slice.start();
				slices.add(slice);
			}
			final var pool = Executors.newCachedThreadPool();
			try {
				final var go = new CountDownLatch(1);
				final List<Future<?>> calls = new ArrayList<>();
				calls.add(pool.submit(raced(go, () -> {
					worker.close();
					return null;
				})));
				for (final LoadStepService slice : slices) {
					calls.add(pool.submit(raced(go, () -> lateControllerCall(slice::stop))));
					calls.add(pool.submit(raced(go, () -> lateControllerCall(() -> {
						slice.close();
						return null;
					}))));
				}
				go.countDown();
				for (final Future<?> call : calls) {
					call.get(60, TimeUnit.SECONDS);
				}
			} finally {
				pool.shutdownNow();
			}
			assertEquals(slices.size(), contextCloses.get(), "every slice's context closes once");
			assertEquals(List.of(), List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()));
			assertTrue(worker.isClosed());
		});
	}

	@Test
	void aWorkerStopsAndClosesASliceWhoseEntryWentSilent() throws Exception {
		withEntrySilenceLimit(() -> withWorker((worker, manager, port) -> {
			final LoadStepService abandoned = newSlice(manager, port);
			abandoned.start();
			abandoned.metricsSnapshots();

			awaitClosedContexts(1, ENTRY_SILENCE_MILLIS + 5_000);
			// The slice's context closes before the service leaves the registry and the open set.
			awaitNoOpenSlices(worker, 5_000);

			assertThrows(java.rmi.RemoteException.class, abandoned::loadStepId);
			assertEquals(List.of(), worker.busySliceRunIds());
			assertFalse(worker.hasOpenSlices(), "the closed slice left the worker's open set");
			assertEquals(List.of(LoadStepManagerService.SVC_NAME),
							List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()));
			assertFalse(worker.isClosed(), "the worker stays up for its next step");

			final LoadStepService next = newSlice(manager, port);
			next.start();
			next.stop();
			next.close();
			assertEquals(2, contextCloses.get(), "the worker still serves steps");

			worker.close();
			assertTrue(worker.isClosed());
			assertEquals(List.of(), List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()));
		}));
	}

	@Test
	void aSliceWhoseEntryKeepsCallingOrHasNotCalledYetStaysOpen() throws Exception {
		withEntrySilenceLimit(POLLED_ENTRY_SILENCE_MILLIS, () -> withWorker((worker, manager, port) -> {
			final LoadStepService polled = newSlice(manager, port);
			polled.start();
			final LoadStepService notCalledYet = newSlice(manager, port);
			notCalledYet.start();

			final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2 * POLLED_ENTRY_SILENCE_MILLIS);
			while (System.nanoTime() < end) {
				polled.metricsSnapshots();
				Thread.sleep(100);
			}

			assertEquals(0, contextCloses.get());
			assertEquals(2, worker.busySliceRunIds().size());
			polled.stop();
			polled.close();
			notCalledYet.stop();
			notCalledYet.close();
		}));
	}

	@Test
	void aWorkerShutdownRacingTheSilenceCheckClosesEachSliceOnce() throws Exception {
		withEntrySilenceLimit(() -> withWorker((worker, manager, port) -> {
			final List<LoadStepService> slices = new ArrayList<>();
			for (var i = 0; i < 4; i++) {
				final LoadStepService slice = newSlice(manager, port);
				slice.start();
				slice.metricsSnapshots();
				slices.add(slice);
			}
			// The checkers run once a second, so a close about now meets them at the limit.
			Thread.sleep(ENTRY_SILENCE_MILLIS);
			worker.close();

			assertEquals(slices.size(), contextCloses.get(), "every slice's context closes once");
			assertEquals(List.of(), List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()));
			assertTrue(worker.isClosed());
		}));
	}

	private void awaitClosedContexts(final int expected, final long timeoutMillis) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (contextCloses.get() < expected && System.nanoTime() < deadline) {
			Thread.sleep(20);
		}
		assertEquals(expected, contextCloses.get(), "slices closed by the worker");
	}

	/** Waits for the worker's last open slice to be unexported and to leave its open set. */
	private static void awaitNoOpenSlices(final LoadStepManagerServiceImpl worker, final long timeoutMillis)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (worker.hasOpenSlices() && System.nanoTime() < deadline) {
			Thread.sleep(20);
		}
	}

	/** The limit is read as each step service is built. */
	private static void withEntrySilenceLimit(final Callable0 test) throws Exception {
		withEntrySilenceLimit(ENTRY_SILENCE_MILLIS, test);
	}

	private static void withEntrySilenceLimit(final long limitMillis, final Callable0 test) throws Exception {
		final String original = System.getProperty(ENTRY_SILENCE_PROPERTY);
		System.setProperty(ENTRY_SILENCE_PROPERTY, Long.toString(limitMillis));
		try {
			test.run();
		} finally {
			if (original == null) {
				System.clearProperty(ENTRY_SILENCE_PROPERTY);
			} else {
				System.setProperty(ENTRY_SILENCE_PROPERTY, original);
			}
		}
	}

	@FunctionalInterface
	private interface Callable0 {
		void run() throws Exception;
	}

	private static Callable<Void> raced(final CountDownLatch go, final Callable<Void> call) {
		return () -> {
			go.await();
			return call.call();
		};
	}

	/** A controller that comes late finds the service gone, which it has to tolerate anyway. */
	private static Void lateControllerCall(final Callable<?> call) throws Exception {
		try {
			call.call();
		} catch (final java.rmi.RemoteException gone) {
			// Unexported by the shutdown
		}
		return null;
	}

	private LoadStepService newSlice(final LoadStepManagerService manager, final int port) throws Exception {
		final Config config = TestConfigBuilder.config();
		final String stepId = "slice-release-" + UUID.randomUUID();
		stepIds.add(stepId);
		config.val("load-step-id", stepId);
		config.val("load-op-type", "create");
		return ServiceUtil.resolve(
						"127.0.0.1", port, manager.newStepService(STEP_TYPE, config, List.of()), LoadStepService.class);
	}

	private void withWorker(final WorkerTest test) throws Exception {
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final int port = freePort();
		final LoadStepManagerServiceImpl worker = new LoadStepManagerServiceImpl(
						port, List.of(ballastFactory()), new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR));
		try {
			worker.start();
			test.run(worker, ServiceUtil.resolve(
							"127.0.0.1", port, LoadStepManagerService.SVC_NAME, LoadStepManagerService.class), port);
		} finally {
			try {
				worker.close();
			} finally {
				ServiceUtil.shutdown();
				if (originalHost == null) {
					System.clearProperty("java.rmi.server.hostname");
				} else {
					System.setProperty("java.rmi.server.hostname", originalHost);
				}
				for (final String stepId : stepIds) {
					removeStepFolder(stepId);
				}
			}
		}
	}

	@FunctionalInterface
	private interface WorkerTest {
		void run(LoadStepManagerServiceImpl worker, LoadStepManagerService manager, int port) throws Exception;
	}

	@Test
	void aWorkerReleasesEverySliceWhoseStopAndCloseFail() throws Exception {
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final int port = freePort();
		final MetricsManager workerMetrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final LoadStepManagerServiceImpl worker = new LoadStepManagerServiceImpl(
						port, List.of(ballastFactory()), workerMetrics);
		final List<WeakReference<Object>> services = new ArrayList<>();
		try {
			worker.start();
			final LoadStepManagerService manager = ServiceUtil.resolve(
							"127.0.0.1", port, LoadStepManagerService.SVC_NAME, LoadStepManagerService.class);

			// A clean step first: its slice closes normally and sets the baseline.
			runStep(manager, port, false);
			services.add(new WeakReference<>(worker.getStepService()));
			assertReleased(worker, workerMetrics, port, services, "the clean step");
			final int threadBaseline = liveThreads();
			final long heapBaseline = liveHeapBytes();

			final List<Long> heapSeries = new ArrayList<>();
			final List<Integer> threadSeries = new ArrayList<>();
			for (var step = 1; step <= FAILING_STEPS; step++) {
				runStep(manager, port, true);
				services.add(new WeakReference<>(worker.getStepService()));
				assertReleased(worker, workerMetrics, port, services, "failing step " + step);
				heapSeries.add(liveHeapBytes());
				threadSeries.add(liveThreads());
			}
			System.out.printf(
							"slice release leak guard: heap baseline %d B, after each failing step %s; threads baseline %d, after each %s%n",
							heapBaseline, heapSeries, threadBaseline, threadSeries);
			for (var i = 0; i < FAILING_STEPS; i++) {
				assertTrue(heapSeries.get(i) - heapBaseline < HEAP_GROWTH_LIMIT_BYTES,
								"live heap grew by " + (heapSeries.get(i) - heapBaseline) + " B by failing step " + (i + 1));
				assertTrue(threadSeries.get(i) <= threadBaseline,
								"threads grew from " + threadBaseline + " to " + threadSeries.get(i) + " by failing step " + (i + 1));
			}
		} finally {
			try {
				worker.close();
			} finally {
				ServiceUtil.shutdown();
				if (originalHost == null) {
					System.clearProperty("java.rmi.server.hostname");
				} else {
					System.setProperty("java.rmi.server.hostname", originalHost);
				}
				for (final String stepId : stepIds) {
					removeStepFolder(stepId);
				}
			}
		}
	}

	/** Runs one slice as the Entry Node does: start, stop, close, all over RMI. */
	private void runStep(final LoadStepManagerService manager, final int port, final boolean failing)
					throws Exception {
		final Config config = TestConfigBuilder.config();
		final String stepId = "slice-release-" + UUID.randomUUID();
		stepIds.add(stepId);
		config.val("load-step-id", stepId);
		config.val("load-op-type", "create");
		final String serviceName = manager.newStepService(STEP_TYPE, config, List.of());
		final LoadStepService slice = ServiceUtil.resolve("127.0.0.1", port, serviceName, LoadStepService.class);
		slice.start();
		if (!failing) {
			slice.stop();
			slice.close();
			return;
		}
		final Path lifecycle = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId));
		Files.createDirectories(lifecycle.getParent());
		Files.writeString(lifecycle, OperationLifecycleArtifact.HEADER + "\nan earlier run's row\n");
		assertStale(assertThrows(Exception.class, slice::stop), "stop");
		assertStale(assertThrows(Exception.class, slice::close), "close");
		assertEquals(OperationLifecycleArtifact.HEADER + "\nan earlier run's row\n", Files.readString(lifecycle));
		// A released slice answers no further remote call.
		assertThrows(java.rmi.RemoteException.class, slice::loadStepId);
	}

	private void assertReleased(
					final LoadStepManagerServiceImpl worker,
					final MetricsManager workerMetrics,
					final int port,
					final List<WeakReference<Object>> services,
					final String after) throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (liveBallasts() > 0 || liveServices(services) > 1) {
			if (System.nanoTime() > deadline) {
				break;
			}
			System.gc();
			Thread.sleep(50);
		}
		assertEquals(0, liveBallasts(), "live queued-operation ballasts after " + after);
		assertTrue(liveServices(services) <= 1, liveServices(services) + " live step services after " + after);
		assertEquals(List.of(LoadStepManagerService.SVC_NAME),
						List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()),
						"services bound on the worker after " + after);
		assertTrue(workerMetrics.getAllContexts().isEmpty(),
						"metrics contexts registered on the worker after " + after);
		// The worker's last step service stays readable to its node API, as before.
		assertDoesNotThrow(worker::getStepService);
	}

	private long liveBallasts() {
		return ballasts.stream().filter(ref -> ref.get() != null).count();
	}

	private static long liveServices(final List<WeakReference<Object>> services) {
		return services.stream().filter(ref -> ref.get() != null).count();
	}

	private static long liveHeapBytes() throws InterruptedException {
		for (var i = 0; i < 3; i++) {
			System.gc();
			Thread.sleep(50);
		}
		return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
	}

	/** Platform threads, without RMI's connection threads and the common pool, whose counts vary by themselves. */
	private static int liveThreads() {
		return (int) Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.map(Thread::getName)
						.filter(name -> !name.startsWith("RMI ") && !name.startsWith("ForkJoinPool"))
						.count();
	}

	private static void assertStale(final Throwable failure, final String call) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause.getMessage() != null && cause.getMessage().contains("stale operation lifecycle artifact exists")) {
				return;
			}
		}
		throw new AssertionError("the " + call + " did not report the old lifecycle file", failure);
	}

	private LoadStepFactory<LoadStep, ?> ballastFactory() {
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class, withSettings().stubOnly());
		when(factory.id()).thenReturn(STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(invocation -> new BallastSlice(
						invocation.getArgument(0), invocation.getArgument(3), newBallastContext()));
		return factory;
	}

	/** A context that holds a driver input queue's worth of operations, as a real slice's does. */
	private LoadStepContext newBallastContext() {
		final List<DataOperationImpl<DataItemImpl>> ballast = new ArrayList<>(BALLAST_OPERATIONS);
		for (var i = 0; i < BALLAST_OPERATIONS; i++) {
			ballast.add(new DataOperationImpl<>(0, OpType.CREATE, new DataItemImpl("queued-" + i, 0, 1), null, null, null, null, 0));
		}
		ballasts.add(new WeakReference<>(ballast));
		// Not a Mockito mock: each thread that calls a mock keeps a reference to it in its stubbing state.
		final InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
		case "equals" -> proxy == args[0];
		case "hashCode" -> System.identityHashCode(proxy);
		case "toString" -> "ballast context of " + ballast.size() + " operations";
		case "activeOpCount" -> ballast.isEmpty() ? 0 : 1;
		case "close" -> {
			contextCloses.incrementAndGet();
			yield null;
		}
		default -> defaultValue(method.getReturnType());
		};
		return (LoadStepContext) Proxy.newProxyInstance(
						LoadStepContext.class.getClassLoader(), new Class<?>[]{LoadStepContext.class
						}, handler);
	}

	private static Object defaultValue(final Class<?> type) {
		if (type == boolean.class) {
			return false;
		}
		if (type == int.class || type == short.class || type == byte.class || type == char.class) {
			return type == int.class ? 0 : type == short.class ? (short) 0 : type == byte.class ? (byte) 0 : (char) 0;
		}
		if (type == long.class) {
			return 0L;
		}
		if (type == double.class) {
			return 0d;
		}
		if (type == float.class) {
			return 0f;
		}
		if (type == OptionalLong.class) {
			return OptionalLong.empty();
		}
		if (type == Optional.class) {
			return Optional.empty();
		}
		if (type == List.class || type == Collection.class) {
			return List.of();
		}
		if (type == Set.class) {
			return Set.of();
		}
		if (type == Map.class) {
			return Map.of();
		}
		return null;
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static void removeStepFolder(final String stepId) throws IOException {
		final Path dir = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)).getParent();
		if (dir == null || !Files.exists(dir)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(dir)) {
			for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	/** A worker's local slice with the production metrics wiring, over one ballast context. */
	private static final class BallastSlice extends LoadStepLocalBase {
		// Handed to the contexts at init and dropped here, so only the slice's own lists hold it.
		private LoadStepContext context;

		BallastSlice(final Config config, final MetricsManager metricsMgr, final LoadStepContext context) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsMgr);
			this.context = context;
		}

		@Override
		public String getTypeName() {
			return STEP_TYPE;
		}

		@Override
		protected void init() {
			stepContexts.add(context);
			context = null;
			initMetrics(0, OpType.CREATE, 1, config.configVal("output").configVal("metrics"), new SizeInBytes(1L), false);
		}
	}
}
