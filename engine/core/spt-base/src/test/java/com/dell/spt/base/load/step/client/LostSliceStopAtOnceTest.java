package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.rmi.ConnectException;
import java.rmi.UnmarshalException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * The stop at once of a distributed step that loses a remote slice in its await: which slices it
 * stops, from which thread, that the entry's own slice stops without waiting for a worker's stop or
 * await, and that the step's own stop stops no slice it reached again. The slices are mocks: slice
 * 0 stands for the entry's own, slices 1 and 2 for two workers', and slice 1 is lost at its await.
 */
class LostSliceStopAtOnceTest {

	private static final String STOP_AT_ONCE_THREAD = "spt-lost-slice-stop-";
	private static final Duration BOUND = Duration.ofSeconds(30);

	/** The name of each thread that stopped each slice, by slice index. */
	private final List<List<String>> stoppedBy = List.of(
					Collections.synchronizedList(new ArrayList<>()),
					Collections.synchronizedList(new ArrayList<>()),
					Collections.synchronizedList(new ArrayList<>()));
	/** When each slice's stop began, by slice index. */
	private final long[] stopNanos = new long[3];
	private volatile long lostNanos;
	/** The slice whose loadStepId times out once on a stop at once's thread, or -1 for none. */
	private volatile int failLoadStepIdAtOnceSlice = -1;
	/** When that failed loadStepId call ended. */
	private volatile long failedLoadStepIdEndNanos;

	@Test
	void everySliceGetsOneStopAtOnceOnlyTheLostOneIncluded() throws Exception {
		final TestLoadStepClient client = client(TestConfigBuilder.config(), 0, false);

		final RunTerminalException failure = awaitFailing(client);

		assertTrue(failure.getMessage().startsWith("engine 2 of 3: the entry lost this node's step slice"), failure.getMessage());
		assertEquals(List.of(STOP_AT_ONCE_THREAD), threadPrefixes(2), "the reachable worker is stopped at once: " + stoppedBy.get(2));
		// A lost worker can still be alive and generating, as one lost at a failed metrics fetch.
		assertEquals(List.of(STOP_AT_ONCE_THREAD), threadPrefixes(1), "the lost slice is stopped at once too: " + stoppedBy.get(1));
		assertEquals(List.of(STOP_AT_ONCE_THREAD), threadPrefixes(0), "the entry's own slice is stopped at once: " + stoppedBy.get(0));

		// The close's stop runs the step's stop again: a slice stopped at once still gets no second stop.
		client.doStop();
		assertEquals(1, stoppedBy.get(2).size(), "one stop of the slice stopped at once: " + stoppedBy.get(2));
		assertEquals(1, stoppedBy.get(0).size(), "one stop of the entry's own slice: " + stoppedBy.get(0));
		assertEquals(1, stoppedBy.get(1).size(), "one stop of the lost slice: " + stoppedBy.get(1));
	}

	@Test
	void aStopAtOnceThatFailedBeforeItWasSentIsSentByTheStepsStopOnce() throws Exception {
		// The reachable worker's loadStepId, which the stop calls before the stop itself, times out once,
		// on the stop at once's thread, after a while: the step's stop waits for it and then sends the stop.
		failLoadStepIdAtOnceSlice = 2;
		final TestLoadStepClient client = client(TestConfigBuilder.config(), 0, false);

		awaitFailing(client);

		assertEquals(1, stoppedBy.get(2).size(), "the worker gets its stop once: " + stoppedBy.get(2));
		assertFalse(threadPrefixes(2).contains(STOP_AT_ONCE_THREAD), stoppedBy.get(2).toString());
		assertTrue(stopNanos[2] > failedLoadStepIdEndNanos,
						"the step's stop comes after the failed stop at once ended, so they do not overlap");
		// The close's stop runs the step's stop again: no further stop.
		client.doStop();
		assertEquals(1, stoppedBy.get(2).size(), "still one stop: " + stoppedBy.get(2));
	}

	@Test
	void aLostSliceWhoseStopAtOnceFailedBeforeItWasSentGetsNoSecondStop() throws Exception {
		// The lost worker's loadStepId times out on its stop at once's thread, so that stop is never sent:
		// the step's stop sends a second stop only to a slice that is not lost, as a lost one's would fail.
		failLoadStepIdAtOnceSlice = 1;
		final TestLoadStepClient client = client(TestConfigBuilder.config(), 0, false);

		awaitFailing(client);
		client.doStop();

		assertEquals(List.of(), stoppedBy.get(1), "the lost slice gets no stop after its unsent stop at once");
		assertEquals(1, stoppedBy.get(2).size(), "the reachable worker still gets one stop: " + stoppedBy.get(2));
		assertEquals(1, stoppedBy.get(0).size(), "the entry's own slice still gets one stop: " + stoppedBy.get(0));
	}

	@Test
	void theEntrysOwnSliceStopsWithoutWaitingForAWorkersSlowStop() throws Exception {
		// The reachable worker's stop answers after its 5 s drain, as a worker whose target holds its replies.
		final long slowStopMillis = 5_000;
		final TestLoadStepClient client = client(TestConfigBuilder.config(), slowStopMillis, false);

		final long began = System.nanoTime();
		awaitFailing(client);
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);

		final long localStopAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(stopNanos[0] - lostNanos);
		System.out.printf(
						"lost-slice local stop measurement: the entry's own slice stopped %d ms after the loss; the await ended %d ms after it began%n",
						localStopAfterLossMillis, tookMillis);
		assertTrue(stopNanos[0] != 0, "the entry's own slice is stopped");
		assertTrue(localStopAfterLossMillis < 1_000,
						"the entry's own slice stops at once, not after the worker's slow stop: " + localStopAfterLossMillis + " ms");
		assertTrue(tookMillis >= slowStopMillis, "the await still waits for the worker's stop: " + tookMillis + " ms");
		assertEquals(1, stoppedBy.get(2).size(), "one stop of the slow worker: " + stoppedBy.get(2));
	}

	@Test
	void theEntrysOwnSliceStopsWhileAWorkersAwaitWaitsForItsStop() throws Exception {
		// The reachable worker's await waits while its stop is in flight, as a worker's await waits on
		// the slice's state lock that its stop holds for the drain; both then time out their responses.
		final ErrCapture warnings = ErrCapture.attach();
		final TestLoadStepClient client;
		try {
			client = client(TestConfigBuilder.config(), 1_000, true);
			awaitFailing(client);
			warnings.awaitSettled("failed to stop the step slice");
		} finally {
			warnings.detach();
		}

		final long localStopAfterLossMillis = TimeUnit.NANOSECONDS.toMillis(stopNanos[0] - lostNanos);
		System.out.printf("lost-slice blocked-await measurement: the entry's own slice stopped %d ms after the loss%n",
						localStopAfterLossMillis);
		assertTrue(stopNanos[0] != 0, "the entry's own slice is stopped");
		assertTrue(localStopAfterLossMillis < 500,
						"the entry's own slice stops at once, not after the worker's await answers: " + localStopAfterLossMillis + " ms");
		assertEquals(1, stoppedBy.get(2).size(), "one stop of the slow worker: " + stoppedBy.get(2));
		assertEquals(1, stoppedBy.get(0).size(), "one stop of the entry's own slice: " + stoppedBy.get(0));
		final List<String> slice2Warnings = warnings.messages().stream().filter(m -> m.contains("\"slice-2\"")).toList();
		assertEquals(1, slice2Warnings.size(), "one warning for the slow worker: " + slice2Warnings);
		assertTrue(slice2Warnings.getFirst().contains("failed to stop the step slice"), slice2Warnings.getFirst());
	}

	@Test
	void aStandaloneDeleteStopsNoSliceAtOnce() throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "lost-slice-standalone-delete");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		final TestLoadStepClient client = client(config, 0, false);

		awaitFailing(client);

		for (var i = 0; i < 3; i++) {
			assertFalse(threadPrefixes(i).contains(STOP_AT_ONCE_THREAD),
							"a standalone DELETE stops its slices after its await only: slice " + i + " " + stoppedBy.get(i));
		}
		assertFalse(stoppedBy.get(2).isEmpty(), "the reachable slice is still stopped");
	}

	private RunTerminalException awaitFailing(final TestLoadStepClient client) {
		// Three pollers, one per slice, whatever the host's CPU count.
		final ForkJoinPool pollers = new ForkJoinPool(3);
		try {
			return assertTimeoutPreemptively(BOUND, () -> {
				try {
					pollers.submit(() -> client.await(60, TimeUnit.SECONDS)).get();
				} catch (final java.util.concurrent.ExecutionException e) {
					return assertThrowsCause(e.getCause());
				}
				throw new AssertionError("the await did not fail");
			});
		} finally {
			pollers.shutdownNow();
		}
	}

	private static RunTerminalException assertThrowsCause(final Throwable cause) {
		return assertThrows(RunTerminalException.class, () -> {
			throw cause;
		});
	}

	private List<String> threadPrefixes(final int slice) {
		synchronized (stoppedBy.get(slice)) {
			return stoppedBy.get(slice).stream()
							.map(name -> name.startsWith(STOP_AT_ONCE_THREAD) ? STOP_AT_ONCE_THREAD : name)
							.toList();
		}
	}

	/**
	 * A client with three started slices, the second of which is lost at its await. Slice 2's stop
	 * takes {@code slowStopMillis}; with {@code timingOut}, slice 2's await waits for its stop to end,
	 * and both then fail as calls whose response timed out.
	 */
	private TestLoadStepClient client(final Config config, final long slowStopMillis, final boolean timingOut)
					throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(), List.of(), mock(MetricsManager.class));
		final List<LoadStep> slices = new ArrayList<>();
		final CountDownLatch slice2StopBegun = new CountDownLatch(1);
		final CountDownLatch slice2StopEnded = new CountDownLatch(1);
		for (var i = 0; i < 3; i++) {
			final int index = i;
			final LoadStep slice = mock(LoadStep.class);
			final boolean[] failed = new boolean[1];
			when(slice.loadStepId()).thenAnswer(invocation -> {
				synchronized (failed) {
					if (failLoadStepIdAtOnceSlice == index && !failed[0]
									&& Thread.currentThread().getName().startsWith(STOP_AT_ONCE_THREAD)) {
						failed[0] = true;
						Thread.sleep(300);
						failedLoadStepIdEndNanos = System.nanoTime();
						throw responseTimeout();
					}
				}
				return "slice-" + index;
			});
			when(slice.toString()).thenReturn("slice-" + i);
			if (i == 1) {
				when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
					// Polled a while first, as a worker that goes mid-step.
					Thread.sleep(200);
					lostNanos = System.nanoTime();
					throw new ConnectException("injected: worker unreachable");
				});
			} else if (i == 2 && timingOut) {
				when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
					// A poll in flight until the stop comes, as on a worker whose await waits on the state lock
					// its stop holds: the stop at once begins at the loss, well within this bound.
					if (slice2StopBegun.await(BOUND.toMillis(), TimeUnit.MILLISECONDS)) {
						slice2StopEnded.await();
						throw responseTimeout();
					}
					return false;
				});
			} else {
				when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
					Thread.sleep(1);
					return false;
				});
			}
			doAnswer(invocation -> {
				synchronized (stopNanos) {
					if (stopNanos[index] == 0) {
						stopNanos[index] = System.nanoTime();
					}
				}
				stoppedBy.get(index).add(Thread.currentThread().getName());
				if (index == 2) {
					slice2StopBegun.countDown();
					try {
						if (slowStopMillis > 0) {
							Thread.sleep(slowStopMillis);
						}
					} finally {
						slice2StopEnded.countDown();
					}
					if (timingOut) {
						throw responseTimeout();
					}
				}
				return slice;
			}).when(slice).stop();
			slices.add(slice);
		}
		set(client, "stepSlices", slices);
		final List<String> addrs = new ArrayList<>();
		addrs.add(null);
		addrs.add("127.0.0.1:1");
		addrs.add("127.0.0.1:2");
		set(client, "stepSliceNodeAddrs", addrs);
		final Field started = LoadStepClientBase.class.getDeclaredField("startedSliceCount");
		started.setAccessible(true);
		started.setInt(client, 3);
		return client;
	}

	/** RMI's failure for a call whose response read timed out. */
	private static UnmarshalException responseTimeout() {
		return new UnmarshalException("Error unmarshaling return header", new SocketTimeoutException("Read timed out"));
	}

	/** The WARN and ERROR lines of the engine's error logger, while attached. */
	private static final class ErrCapture extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private Logger logger;
		private Level level;

		private ErrCapture() {
			super("lostSliceStopAtOnceCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		static ErrCapture attach() {
			final ErrCapture capture = new ErrCapture();
			capture.start();
			capture.logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
			capture.level = capture.logger.getLevel();
			capture.logger.addAppender(capture);
			capture.logger.setLevel(Level.WARN);
			return capture;
		}

		void detach() {
			LogUtil.flushAll();
			logger.removeAppender(this);
			logger.setLevel(level);
			stop();
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
				captured.add(event.getMessage().getFormattedMessage());
			}
		}

		/** Waits, for at most 5 s, until a line contains {@code text}, then leaves room for one more. */
		void awaitSettled(final String text) throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (messages().stream().noneMatch(m -> m.contains(text)) && System.nanoTime() < deadline) {
				LogUtil.flushAll();
				Thread.sleep(10);
			}
			LogUtil.flushAll();
			Thread.sleep(200);
		}

		List<String> messages() {
			synchronized (captured) {
				return List.copyOf(captured);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static void set(final TestLoadStepClient client, final String name, final List<?> values) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField(name);
		field.setAccessible(true);
		((List<Object>) field.get(client)).addAll(values);
	}
}
