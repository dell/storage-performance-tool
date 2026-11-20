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
import java.lang.reflect.Field;
import java.rmi.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The stop at once of a distributed step that loses a remote slice in its await: which slices it
 * stops, from which thread, and that the step's own stop neither waits for it before stopping the
 * entry's own slice nor stops a slice it reached again. The slices are mocks: slice 0 stands for the
 * entry's own, slices 1 and 2 for two workers', and slice 1 is lost at its await.
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

	@Test
	void theLostSliceGetsNoStopAtOnceAndAReachableOneGetsOneStopOnly() throws Exception {
		final TestLoadStepClient client = client(TestConfigBuilder.config(), 0);

		final RunTerminalException failure = awaitFailing(client);

		assertTrue(failure.getMessage().startsWith("engine 2 of 3: the entry lost this node's step slice"), failure.getMessage());
		assertEquals(List.of(STOP_AT_ONCE_THREAD), threadPrefixes(2), "the reachable worker is stopped at once: " + stoppedBy.get(2));
		assertFalse(threadPrefixes(1).contains(STOP_AT_ONCE_THREAD),
						"the lost slice gets no stop at once, only the step's own: " + stoppedBy.get(1));
		assertEquals(1, stoppedBy.get(1).size(), stoppedBy.get(1).toString());
		assertFalse(threadPrefixes(0).contains(STOP_AT_ONCE_THREAD), stoppedBy.get(0).toString());

		// The close's stop runs the step's stop again: a slice stopped at once still gets no second stop.
		client.doStop();
		assertEquals(1, stoppedBy.get(2).size(), "one stop of the slice stopped at once: " + stoppedBy.get(2));
	}

	@Test
	void theEntrysOwnSliceStopsWithoutWaitingForAWorkersSlowStop() throws Exception {
		// The reachable worker's stop answers after its 5 s drain, as a worker whose target holds its replies.
		final long slowStopMillis = 5_000;
		final TestLoadStepClient client = client(TestConfigBuilder.config(), slowStopMillis);

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
	void aStandaloneDeleteStopsNoSliceAtOnce() throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "lost-slice-standalone-delete");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		final TestLoadStepClient client = client(config, 0);

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

	/** A client with three started slices, the second of which is lost at its await. */
	private TestLoadStepClient client(final Config config, final long slowStopMillis) throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(), List.of(), mock(MetricsManager.class));
		final List<LoadStep> slices = new ArrayList<>();
		for (var i = 0; i < 3; i++) {
			final int index = i;
			final LoadStep slice = mock(LoadStep.class);
			when(slice.loadStepId()).thenReturn("slice-" + i);
			if (i == 1) {
				when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
					// Polled a while first, as a worker that goes mid-step.
					Thread.sleep(200);
					lostNanos = System.nanoTime();
					throw new ConnectException("injected: worker unreachable");
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
				if (index == 2 && slowStopMillis > 0) {
					Thread.sleep(slowStopMillis);
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

	@SuppressWarnings("unchecked")
	private static void set(final TestLoadStepClient client, final String name, final List<?> values) throws Exception {
		final Field field = LoadStepClientBase.class.getDeclaredField(name);
		field.setAccessible(true);
		((List<Object>) field.get(client)).addAll(values);
	}
}
