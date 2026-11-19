package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.SingleTaskExecutor;
import com.dell.spt.base.concurrent.SingleTaskExecutorImpl;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.client.TestLoadStepClient;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A request a busy node refuses changes nothing. The scenarios run on the servlet's real script engine
 * and executor; the {@code Probe} step type shows what {@code ScenarioUtil.configure} did to the engine.
 */
@Timeout(60)
class RunServletBusyTest {

	private static final long AWAIT_SECONDS = 20;
	private static final long RUN_A = 1111L;
	private static final long RUN_B = 2222L;
	/** Holds the run, then reports the engine's bindings as the run's later steps would read them. */
	private static final String HOLDING_SCENARIO = """
					Probe.hold();
					Probe.report(config.longVal("run-id"), config.stringVal("run-comment"), Probe.configuredRunId());
					""";
	/** Waits for the stop in its body and holds its cleanup, as a scenario's {@code finally} can. */
	private static final String HOLDING_CLEANUP_SCENARIO = """
					try {
						Probe.awaitStop();
					} finally {
						Probe.holdCleanup();
					}
					""";

	private final Probes probes = new Probes();
	private final ApiStatus status = new ApiStatus();
	private final LoadStepManagerService stepSvc = mock(LoadStepManagerService.class);
	private Config nodeConfig;
	private SingleTaskExecutorImpl executor;
	private RunServlet servlet;

	@BeforeEach
	void setUp() {
		nodeConfig = TestConfigBuilder.config();
		executor = new SingleTaskExecutorImpl();
		servlet = servlet(executor, status);
	}

	@AfterEach
	void tearDown() {
		probes.releaseAll();
		executor.close();
	}

	private RunServlet servlet(final SingleTaskExecutor taskExecutor, final ApiStatus apiStatus) {
		return new RunServlet(
						null, List.of(probes.factory()), null, nodeConfig, null, stepSvc, apiStatus, taskExecutor);
	}

	@Test
	void aRefusedPostLeavesTheRunInProgressAndItsBindingsUntouched() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		final var before = status.snapshot();
		final var clientsBefore = probes.created.get();

		final var refused = post(RUN_B, "runB", HOLDING_SCENARIO);

		assertEquals(409, refused.status);
		assertEquals(before, status.snapshot());
		assertEquals(clientsBefore, probes.created.get(), "the refused request ran ScenarioUtil.configure");
		assertEquals(String.valueOf(RUN_A), head().etag);
		probes.release();
		awaitIdle();
		assertEquals(new Report(RUN_A, "runA", RUN_A), probes.onlyReport());
		assertEquals(ApiStatus.State.COMPLETED, status.state());
		assertEquals(RUN_A, status.runId());
	}

	@Test
	void ofSimultaneousPostsExactlyOneStartsWithItsOwnConfiguration() throws Exception {
		final int requests = 8;
		final var pool = Executors.newFixedThreadPool(requests);
		try {
			final var start = new CountDownLatch(1);
			final var answers = new ArrayList<Future<Answer>>();
			for (int i = 0; i < requests; i++) {
				final long runId = RUN_A + i;
				answers.add(pool.submit(() -> {
					start.await();
					return post(runId, "run" + runId, HOLDING_SCENARIO);
				}));
			}
			start.countDown();
			final var accepted = new ArrayList<Answer>();
			for (final var answer : answers) {
				final var result = answer.get(AWAIT_SECONDS, TimeUnit.SECONDS);
				if (result.status == 202) {
					accepted.add(result);
				} else {
					assertEquals(409, result.status);
				}
			}
			assertEquals(1, accepted.size(), "the run holds on its latch, so only one request can start");
			probes.awaitHeld();
			final long acceptedId = Long.parseLong(accepted.get(0).etag);
			assertEquals(acceptedId, status.runId());
			assertEquals(1, probes.created.get(), "only the accepted request may configure the engine");
			probes.release();
			awaitIdle();
			assertEquals(new Report(acceptedId, "run" + acceptedId, acceptedId), probes.onlyReport());
			assertEquals(acceptedId, status.runId());
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	void aRequestThatWouldFailWhileConfiguringIsRefusedOnABusyNode() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		final var before = status.snapshot();
		probes.failConfigure = true;

		assertEquals(409, post(RUN_B, "runB", HOLDING_SCENARIO).status);

		assertEquals(before, status.snapshot());
		probes.failConfigure = false;
		probes.release();
		awaitIdle();
		assertEquals(new Report(RUN_A, "runA", RUN_A), probes.onlyReport());
	}

	@Test
	void theSameRequestAnswersBadRequestOnAnIdleNodeAndTheNextRunStarts() throws Exception {
		probes.failConfigure = true;
		assertEquals(400, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		assertEquals(ApiStatus.State.IDLE, status.state());
		assertEquals(204, head().status);

		probes.failConfigure = false;
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		probes.release();
		awaitIdle();
		assertEquals(new Report(RUN_A, "runA", RUN_A), probes.onlyReport());
	}

	@Test
	void aRequestThatCannotBeMergedStillAnswersBadRequestOnABusyNode() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		final var before = status.snapshot();

		assertEquals(400, postDefaults("load:\n  op:\n    shuffle: \"no\"\n", HOLDING_SCENARIO).status);

		assertEquals(before, status.snapshot());
	}

	@Test
	void aRefusedRequestLeavesTheNodeConfigurationUntouched() throws Exception {
		final var before = Config.deepToMap(nodeConfig);
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();

		assertEquals(409, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		assertEquals(409, postDefaults(null, HOLDING_SCENARIO).status);

		assertEquals(before, Config.deepToMap(nodeConfig));
	}

	@Test
	void aRefusedRequestAddsNothingToTheNodesMapSettings() throws Exception {
		nodeConfig.val("storage-net-nodeInterfaces", new HashMap<>(Map.of("node-a", "eth0")));
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();

		final var refused = """
						run:
						  id: %d
						storage:
						  net:
						    nodeInterfaces:
						      node-b: eth1
						""".formatted(RUN_B);
		assertEquals(409, postDefaults(refused, HOLDING_SCENARIO).status);

		assertEquals(Map.of("node-a", "eth0"), nodeConfig.mapVal("storage-net-nodeInterfaces"));
	}

	@Test
	void afterAStopTheNodeStaysBusyUntilTheCleanupReturns() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_CLEANUP_SCENARIO).status);
		probes.awaitBody();
		assertEquals(200, delete(RUN_A));
		probes.awaitCleanup();

		assertEquals(409, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		final var whileCleaning = head();
		assertEquals(200, whileCleaning.status);
		assertEquals(String.valueOf(RUN_A), whileCleaning.etag);
		assertEquals(200, get(RUN_A));
		assertEquals(204, get(RUN_B));
		assertEquals(ApiStatus.State.RUNNING, status.state());
		assertEquals(RUN_A, status.runId());

		probes.releaseCleanup();
		awaitIdle();
		assertEquals(ApiStatus.State.STOPPED, status.state());
		assertEquals(202, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		assertEquals(ApiStatus.State.RUNNING, status.state());
		assertEquals(RUN_B, status.runId());
		probes.release();
		awaitIdle();
		assertEquals(ApiStatus.State.COMPLETED, status.state());
		assertEquals(new Report(RUN_B, "runB", RUN_B), probes.onlyReport());
	}

	@Test
	void aRepeatedStopDuringTheCleanupMatchesTheStoppingRun() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_CLEANUP_SCENARIO).status);
		probes.awaitBody();
		assertEquals(200, delete(RUN_A));
		probes.awaitCleanup();

		assertEquals(200, delete(RUN_A));
		assertEquals(404, delete(RUN_B));

		probes.releaseCleanup();
		awaitIdle();
		assertEquals(ApiStatus.State.STOPPED, status.state());
		assertEquals(204, head().status);
	}

	@Test
	void aRunStoppedBeforeItStartedIsNotFinishedUntilItsStatusIsPublished() throws Exception {
		final var publishing = new CountDownLatch(1);
		final var publish = new CountDownLatch(1);
		final var heldStatus = mock(ApiStatus.class);
		doAnswer(invocation -> {
			publishing.countDown();
			assertTrue(publish.await(AWAIT_SECONDS, TimeUnit.SECONDS));
			return null;
		}).when(heldStatus).setStopped();
		final var run = new RunServlet.StatusAwareRun(mock(Run.class), heldStatus);
		final var pool = Executors.newSingleThreadExecutor();
		try {
			final var stop = pool.submit(
							() -> run.stopAndAwait(mock(SingleTaskExecutor.class), AWAIT_SECONDS, TimeUnit.SECONDS));
			assertTrue(publishing.await(AWAIT_SECONDS, TimeUnit.SECONDS));
			// A run accepted now would have its RUNNING overwritten by this STOPPED
			assertFalse(run.isFinished());
			publish.countDown();
			assertTrue(stop.get(AWAIT_SECONDS, TimeUnit.SECONDS));
			assertTrue(run.isFinished());
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	void aBusyWorkerRefusesBeforeReadingTheRequest() throws Exception {
		when(stepSvc.busySliceRunIds()).thenReturn(List.of(RUN_A));
		final var before = status.snapshot();
		final var req = mock(HttpServletRequest.class);
		final var resp = mock(HttpServletResponse.class);

		servlet.doPost(req, resp);

		verify(resp).setStatus(HttpServletResponse.SC_CONFLICT);
		verify(req, never()).getPart(anyString());
		verify(req, never()).getHeader(anyString());
		assertEquals(before, status.snapshot());
		assertEquals(0, probes.created.get());
	}

	@Test
	void aWorkerSliceCreatedWhileTheRequestWasReadRefusesItToo() throws Exception {
		when(stepSvc.busySliceRunIds()).thenReturn(List.of(), List.of(RUN_A));
		final var before = status.snapshot();

		assertEquals(409, post(RUN_B, "runB", HOLDING_SCENARIO).status);

		assertEquals(before, status.snapshot());
		assertEquals(0, probes.created.get());
	}

	@Test
	void aBusySliceIsReportedAndRefusesARunUntilItEnds() throws Exception {
		final var busy = busySlices(RUN_A);
		final var before = status.snapshot();

		final var whileBusy = head();
		assertEquals(200, whileBusy.status);
		assertEquals(String.valueOf(RUN_A), whileBusy.etag);
		assertEquals(200, get(RUN_A));
		assertEquals(204, get(RUN_B));
		assertEquals(409, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		assertEquals(before, status.snapshot());
		assertEquals(0, probes.created.get());

		// Stopped or closed: the manager lists it no more, whether or not the service is still open
		busy.set(List.of());
		when(stepSvc.hasOpenSlices()).thenReturn(true);
		assertEquals(204, head().status);
		assertEquals(204, get(RUN_A));
		assertEquals(202, post(RUN_B, "runB", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		assertEquals(ApiStatus.State.RUNNING, status.state());
		assertEquals(RUN_B, status.runId());
	}

	@Test
	void theNewestBusySliceNamesTheRunAndEveryBusySliceMatches() throws Exception {
		busySlices(RUN_B, RUN_A);

		assertEquals(String.valueOf(RUN_B), head().etag);
		assertEquals(200, get(RUN_A));
		assertEquals(200, get(RUN_B));
		assertEquals(204, get(RUN_A + RUN_B));
	}

	@Test
	void aRunningScenarioAndABusySliceAreBothMatched() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		busySlices(RUN_B);

		assertEquals(String.valueOf(RUN_A), head().etag);
		assertEquals(200, get(RUN_A));
		assertEquals(200, get(RUN_B));
		assertEquals(204, get(RUN_A + RUN_B));
	}

	@Test
	void deleteStopsTheMatchingSlicesAndClosesNothing() throws Exception {
		when(stepSvc.stopSlicesOfRun(RUN_A)).thenReturn(true);
		when(stepSvc.hasOpenSlices()).thenReturn(true);

		assertEquals(200, delete(RUN_A));

		verify(stepSvc).stopSlicesOfRun(RUN_A);
		verify(stepSvc, never()).close();
		assertEquals(ApiStatus.State.IDLE, status.state());
	}

	@Test
	void deleteOfAnotherRunAnswersNotFoundWhileASliceIsOpen() throws Exception {
		when(stepSvc.hasOpenSlices()).thenReturn(true);

		assertEquals(404, delete(RUN_B));
	}

	@Test
	void deleteOnANodeWithNothingOpenAnswersNoContent() throws Exception {
		assertEquals(204, delete(RUN_A));
	}

	@Test
	void deleteOfASliceLeavesTheRunningScenarioAlone() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		probes.awaitHeld();
		when(stepSvc.stopSlicesOfRun(RUN_B)).thenReturn(true);

		assertEquals(200, delete(RUN_B));

		assertEquals(String.valueOf(RUN_A), head().etag);
		assertEquals(ApiStatus.State.RUNNING, status.state());
		assertEquals(404, delete(RUN_A + RUN_B));
		assertEquals(ApiStatus.State.RUNNING, status.state());
	}

	@Test
	void deleteOfTheRunningScenarioStillStopsIt() throws Exception {
		assertEquals(202, post(RUN_A, "runA", HOLDING_CLEANUP_SCENARIO).status);
		probes.awaitBody();
		when(stepSvc.stopSlicesOfRun(RUN_A)).thenReturn(true);

		assertEquals(200, delete(RUN_A));

		probes.awaitCleanup();
		probes.releaseCleanup();
		awaitIdle();
		assertEquals(ApiStatus.State.STOPPED, status.state());
	}

	private AtomicReference<List<Long>> busySlices(final Long... runIds) throws Exception {
		final var busy = new AtomicReference<>(List.of(runIds));
		when(stepSvc.busySliceRunIds()).thenAnswer(invocation -> busy.get());
		return busy;
	}

	@Test
	void anExecutorRefusalAnswersConflict() throws Exception {
		final var taskExecutor = mock(SingleTaskExecutor.class);
		doThrow(new RejectedExecutionException()).when(taskExecutor).execute(any());
		servlet = servlet(taskExecutor, status);

		assertEquals(409, post(RUN_A, "runA", HOLDING_SCENARIO).status);
		assertEquals(204, head().status, "a run the executor refused is not the active run");
	}

	private void awaitIdle() throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
		while (head().status != 204) {
			assertTrue(System.nanoTime() < deadline, "the run did not finish");
			Thread.sleep(5);
		}
	}

	private Answer post(final long runId, final String comment, final String scenario) throws Exception {
		return postDefaults("run:\n  id: " + runId + "\n  comment: " + comment + "\n", scenario);
	}

	private Answer postDefaults(final String defaults, final String scenario) throws Exception {
		final var req = mock(HttpServletRequest.class);
		when(req.getHeader("Content-Type")).thenReturn("multipart/form-data; boundary=b");
		// Without a defaults part the request runs on a copy of the node's configuration
		final Part defaultsPart = defaults == null ? null : part(defaults);
		final Part scenarioPart = part(scenario);
		when(req.getPart("defaults")).thenReturn(defaultsPart);
		when(req.getPart("scenario")).thenReturn(scenarioPart);
		final var answer = new RecordingResponse();
		servlet.doPost(req, answer.response);
		return answer.answer();
	}

	private Answer head() throws Exception {
		final var answer = new RecordingResponse();
		servlet.doHead(mock(HttpServletRequest.class), answer.response);
		return answer.answer();
	}

	private int get(final long runId) throws Exception {
		final var answer = new RecordingResponse();
		servlet.doGet(ifMatch(runId), answer.response);
		return answer.answer().status;
	}

	private int delete(final long runId) throws Exception {
		final var answer = new RecordingResponse();
		servlet.doDelete(ifMatch(runId), answer.response);
		return answer.answer().status;
	}

	private static HttpServletRequest ifMatch(final long runId) {
		final var req = mock(HttpServletRequest.class);
		when(req.getHeaders("If-Match")).thenAnswer(
						invocation -> Collections.enumeration(List.of(String.valueOf(runId))));
		return req;
	}

	private static Part part(final String content) throws IOException {
		final Part part = mock(Part.class);
		when(part.getInputStream()).thenReturn(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
		when(part.getContentType()).thenReturn("application/yaml");
		return part;
	}

	private record Answer(int status, String etag) {}

	private record Report(long configRunId, String configComment, long probeRunId) {}

	/** Collects the status and the ETag a servlet method sets. */
	private static final class RecordingResponse {

		private final Map<String, String> headers = new ConcurrentHashMap<>();
		private final AtomicInteger status = new AtomicInteger();
		private final HttpServletResponse response = mock(HttpServletResponse.class, invocation -> {
			switch (invocation.getMethod().getName()) {
			case "setStatus" -> status.set(invocation.getArgument(0));
			case "setHeader" -> headers.put(invocation.getArgument(0), invocation.getArgument(1));
			case "getWriter" -> {
				return new PrintWriter(new StringWriter());
			}
			default -> {
				// the other response calls don't matter here
			}
			}
			return null;
		});

		private Answer answer() {
			return new Answer(status.get(), headers.get("ETAG"));
		}
	}

	/** The state the {@code Probe} objects of one test share with it. */
	private static final class Probes {

		private final AtomicInteger created = new AtomicInteger();
		private final List<Report> reports = new CopyOnWriteArrayList<>();
		private final AtomicReference<CountDownLatch> held = new AtomicReference<>(new CountDownLatch(1));
		private final AtomicReference<CountDownLatch> release = new AtomicReference<>(new CountDownLatch(1));
		private final CountDownLatch inBody = new CountDownLatch(1);
		private final CountDownLatch inCleanup = new CountDownLatch(1);
		private final CountDownLatch cleanupRelease = new CountDownLatch(1);
		private volatile boolean failConfigure;

		@SuppressWarnings({"rawtypes", "unchecked"
		})
		private Extension factory() {
			final LoadStepFactory factory = mock(LoadStepFactory.class);
			when(factory.id()).thenReturn("Probe");
			when(factory.createClient(any(), any(), any())).thenAnswer(invocation -> {
				if (failConfigure) {
					throw new IllegalArgumentException("refused while configuring");
				}
				created.incrementAndGet();
				return new Probe(invocation.getArgument(0), this);
			});
			return factory;
		}

		private void awaitHeld() throws InterruptedException {
			assertTrue(held.get().await(AWAIT_SECONDS, TimeUnit.SECONDS), "the scenario did not start");
		}

		private void release() {
			final var latch = release.get();
			held.set(new CountDownLatch(1));
			release.set(new CountDownLatch(1));
			latch.countDown();
		}

		private void awaitBody() throws InterruptedException {
			assertTrue(inBody.await(AWAIT_SECONDS, TimeUnit.SECONDS), "the scenario did not start");
		}

		private void awaitCleanup() throws InterruptedException {
			assertTrue(inCleanup.await(AWAIT_SECONDS, TimeUnit.SECONDS), "the cleanup did not start");
		}

		private void releaseCleanup() {
			cleanupRelease.countDown();
		}

		private void releaseAll() {
			release.get().countDown();
			cleanupRelease.countDown();
		}

		private Report onlyReport() {
			assertEquals(1, reports.size(), String.valueOf(reports));
			return reports.remove(0);
		}
	}

	/** The object a scenario sees as {@code Probe}: a step client built from the configured request. */
	public static final class Probe extends TestLoadStepClient {

		private final long configuredRunId;
		private final Probes probes;

		private Probe(final Config config, final Probes probes) {
			super(config, List.of(), null, null);
			this.configuredRunId = config.longVal("run-id");
			this.probes = probes;
		}

		public long configuredRunId() {
			return configuredRunId;
		}

		public void hold() throws InterruptedException {
			final var release = probes.release.get();
			probes.held.get().countDown();
			release.await();
		}

		public void report(final long configRunId, final String configComment, final long probeRunId) {
			probes.reports.add(new Report(configRunId, configComment, probeRunId));
		}

		public void awaitStop() throws InterruptedException {
			probes.inBody.countDown();
			new CountDownLatch(1).await();
		}

		public void holdCleanup() {
			probes.inCleanup.countDown();
			// The stop's interruption must not end the held cleanup
			var interrupted = false;
			while (true) {
				try {
					probes.cleanupRelease.await();
					break;
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
