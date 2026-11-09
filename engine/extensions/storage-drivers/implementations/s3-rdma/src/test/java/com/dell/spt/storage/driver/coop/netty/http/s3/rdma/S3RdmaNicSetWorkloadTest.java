package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.ADDRESSES;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.BUCKET;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.KERNEL_SOURCE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.NICS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.SERVER_GET_FILL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.awaitRefused;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.THRESHOLD;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.closedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.op;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.rdma;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Request;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Run;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Workload canaries of the RDMA rows of item 19 (K2): each workload succeeds through the real driver
 * on {@link RdmaNicLoopback}'s loopback NICs, with correct payloads, the attempt's NIC in its token
 * and its Paths, and each buffer back in its NIC's pool at the 5.16 reuse boundary.
 */
@Timeout(90)
final class S3RdmaNicSetWorkloadTest {

	private final RdmaNicLoopback stub = new RdmaNicLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		stub.release();
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
	}

	@Test
	void getsAndPutsArePreparedOnTheLanesAndABufferIsReusedOnlyAfterTheFinalResponse() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, 2, 4);
		final Set<String> preparing = new CopyOnWriteArraySet<>();
		stub.transports.forEach(transport -> transport.setBeforeToken(() -> preparing.add(Thread.currentThread().getName())));

		final var names = IntStream.range(0, 16).mapToObj(i -> "put-" + i).toList();
		for (final var name : names) {
			assertTrue(run.driver().put(op(OpType.CREATE, name)));
		}
		for (var i = 0; i < names.size(); i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		names.forEach(name -> assertArrayEquals(RdmaNicLoopback.expected(name, SIZE), stub.object(BUCKET + "/" + name), name));
		assertEquals(Set.of(RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(0)), RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(1))),
						preparing, "every token was made on a lane");
		run.awaitIdle();

		// The GETs' buffers stay with their requests while the server may write them.
		stub.hold();
		final var reads = IntStream.range(0, 4).mapToObj(i -> op(OpType.READ, "put-" + i)).toList();
		reads.forEach(read -> assertTrue(run.driver().put(read)));
		waitFor(() -> stub.objectRequests().stream().filter(request -> "GET".equals(request.method())).count() == 4, "the reads reach the stub");
		assertEquals(0, run.nic(0).pool.idleBuffers(), "leased until the final response");
		assertEquals(0, run.nic(1).pool.idleBuffers(), "leased until the final response");
		stub.release();
		for (var i = 0; i < reads.size(); i++) {
			final var result = run.results().await();
			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone());
		}
		run.awaitIdle();
		assertEquals(2, run.nic(0).pool.idleBuffers());
		assertEquals(2, run.nic(1).pool.idleBuffers());

		assertTokensOnTheirNics(endpoints);
		final var rows = closedRows(run.driver());
		assertEquals(List.of(20L, 20L, 0L, 0L, 0L, 20L * SIZE), totals(rows), rows::toString);
	}

	@Test
	void anIntegrityVerifiedGetReturnsItsBufferOnlyAfterVerifying() throws Exception {
		stub.objectResponseHeaders = integrityHeaders();
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(2), NICS.subList(0, 2), 2);
		root.val("storage-driver-type", "s3-rdma");
		root.val("storage-integrity-mode", "metadata");
		root.val("storage-integrity-input-provenance", "external");
		final var run = started(root);

		for (var i = 0; i < 2; i++) {
			final var verified = run.executeAlone(op(OpType.READ, "verified-" + i));
			assertEquals(Operation.Status.SUCC, verified.status());
			assertTrue(verified.integrityVerificationResult().verified());
		}
		assertEquals(1, run.nic(0).pool.idleBuffers(), "a verified fill's buffer comes back");
		assertEquals(1, run.nic(1).pool.idleBuffers(), "a verified fill's buffer comes back");

		// A reply the digest does not match fails, and its invalidated buffer is not reused.
		stub.objectResponseHeaders = Map.of(
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_VERSION, "1",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_ALGORITHM, "sha256",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_DIGEST, "0".repeat(64),
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_SIZE, Integer.toString(SIZE));
		final var corrupt = run.executeAlone(op(OpType.READ, "mismatched"));
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, corrupt.status());
		assertEquals(1, run.nic(0).pool.discarded.sum() + run.nic(1).pool.discarded.sum(), "the unverified buffer is discarded");
		assertEquals(1, run.nic(0).pool.hits.sum() + run.nic(1).pool.hits.sum());
	}

	private static Map<String, String> integrityHeaders() throws Exception {
		final byte[] content = new byte[SIZE];
		Arrays.fill(content, SERVER_GET_FILL);
		final String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
		return Map.of(
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_VERSION, "1",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_ALGORITHM, "sha256",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_DIGEST, digest,
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_SIZE, Integer.toString(SIZE));
	}

	@Test
	void httpPayloadsGoOnTheAttemptsNic() throws Exception {
		final var endpoints = stub.listeners(1);
		final var run = started(RdmaNicLoopback.root(rdma(true, true, 1), endpoints, NICS.subList(0, 1), 1));
		final var lane = RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(0));

		// Below the threshold: no buffer, no token, and nothing asked of the lane.
		final Set<String> registering = new CopyOnWriteArraySet<>();
		stub.transports.get(0).setBeforeRegister(() -> registering.add(Thread.currentThread().getName()));
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "below", THRESHOLD - 1)).status());
		assertTrue(registering.isEmpty(), "no buffer was registered");
		assertEquals(THRESHOLD - 1, stub.object(BUCKET + "/below").length);

		// Declined: the attempt's request carries a token, and the body comes over HTTP on its Path.
		stub.decline = request -> true;
		final var declined = run.executeAlone(op(OpType.READ, "below"));
		assertEquals(Operation.Status.SUCC, declined.status());
		assertEquals(1, run.nic(0).stats.rdmaDeclinedServedOverHttp.sum());
		assertEquals(Set.of(lane), registering, "only the proposal used the lane");
		stub.decline = request -> false;

		// A per-operation fallback: the preparation fails, and the payload goes over HTTP.
		stub.transports.get(0).setFailTokenGeneration(true);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "fallback")).status());
		assertEquals(1, run.nic(0).stats.httpFallback.sum());
		assertArrayEquals(RdmaNicLoopback.expected("fallback", SIZE), stub.object(BUCKET + "/fallback"));

		stub.objectRequests().forEach(request -> assertEquals(ADDRESSES.get(0), request.source(), request.toString()));
		assertEquals(endpoints.get(0), stub.objectRequests().get(0).host());
		final var counted = run.nic(0).stats;
		assertEquals(1, counted.httpBelowThreshold.sum());
		assertEquals(1, counted.rdmaRequestsSent.sum());
	}

	@Test
	void rangedReadsGoOverHttpOnTheAttemptsNic() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		final var fixed = IntStream.range(0, 4).mapToObj(i -> rangedRead("fixed-" + i, List.of(new Range(0L, SIZE / 2 - 1, -1L)), 0)).toList();
		final var random = IntStream.range(0, 4).mapToObj(i -> rangedRead("random-" + i, null, 1)).toList();

		for (final var ranged : fixed) {
			final var result = run.executeAlone(ranged);
			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE / 2, ((DataOperation<?>) result).countBytesDone(), "the range received");
		}
		for (final var ranged : random) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(ranged).status());
		}

		final var requests = stub.objectRequests();
		assertEquals(8, requests.size());
		requests.forEach(request -> {
			assertNull(request.token(), () -> "a ranged read is not proposed for RDMA: " + request);
			assertNotNull(request.range(), request.toString());
			assertEquals(request.listener(), request.sourceNic(), () -> "on a Path of its NIC: " + request);
		});
		assertEquals(Set.of(0, 1), requests.stream().map(Request::sourceNic).collect(Collectors.toSet()));
		assertEquals(8, run.nic(0).stats.httpIneligible.sum() + run.nic(1).stats.httpIneligible.sum());
		assertEquals(0, run.nic(0).pool.liveBuffers() + run.nic(1).pool.liveBuffers(), "no buffer was used");
	}

	@Test
	void multipartPartsArePreparedOnTheirNicsLanesAndAbortKeepsTheKernelRoute() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, 2, 2);
		final Set<String> preparing = new CopyOnWriteArraySet<>();
		stub.transports.forEach(transport -> transport.setBeforeToken(() -> preparing.add(Thread.currentThread().getName())));

		final var upload = S3RdmaNicSetDriverTest.upload("uploaded", 2048, 4);
		assertTrue(run.driver().put(upload));
		assertEquals(Operation.Status.SUCC, awaitUpload(run).status());

		final var requests = stub.objectRequests();
		assertEquals(6, requests.size(), () -> "initiate, four parts and complete: " + requests);
		final var parts = requests.stream().filter(S3RdmaNicSetWorkloadTest::isPart).toList();
		assertEquals(4, parts.size());
		parts.forEach(part -> {
			assertEquals(part.sourceNic(), part.tokenNic(), () -> "the part's token names its NIC: " + part);
			assertEquals(part.listener(), part.sourceNic());
		});
		assertEquals(Set.of(0, 1), parts.stream().map(Request::sourceNic).collect(Collectors.toSet()), "the parts went over both NICs");
		assertEquals(preparing, Set.of(RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(0)), RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(1))));
		final var control = requests.stream().filter(request -> "POST".equals(request.method())).toList();
		assertEquals(2, control.size());
		control.forEach(request -> {
			assertNull(request.token(), "initiate and complete go over HTTP");
			assertEquals(request.listener(), request.sourceNic(), () -> "on a Path: " + request);
		});
		assertEquals(8192, stub.object(BUCKET + "/uploaded").length);

		// The second upload's completion fails; the abort that follows is a control request.
		final int completion = requests.size() + 4;
		stub.failObjectRequest = n -> n == completion;
		assertTrue(run.driver().put(S3RdmaNicSetDriverTest.upload("aborted", 2048, 2)));
		assertNotEquals(Operation.Status.SUCC, awaitUpload(run).status());
		waitFor(() -> stub.requests().stream().anyMatch(S3RdmaNicSetWorkloadTest::isAbort), "the failed upload was aborted");
		final var abort = stub.requests().stream().filter(S3RdmaNicSetWorkloadTest::isAbort).findFirst().orElseThrow();
		assertEquals(KERNEL_SOURCE, abort.source(), "the abort keeps the kernel route");
		assertEquals(BUCKET + "/aborted", abort.path());
	}

	private static boolean isPart(final Request request) {
		return request.query() != null && request.query().contains("partNumber=");
	}

	private static boolean isAbort(final Request request) {
		return "DELETE".equals(request.method()) && request.query() != null && request.query().contains("uploadId=");
	}

	private static Operation<DataItem> awaitUpload(final Run run) throws Exception {
		while (true) {
			final var result = run.results().await();
			if (result instanceof CompositeOperation<?> composite && (composite.allSubOperationsDone() || result.status() != Operation.Status.SUCC)) {
				return result;
			}
		}
	}

	@Test
	void aMixedDriversGetsPutsAndDeletesEachUseTheirNic() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, 2, 2);

		final List<Operation<DataItem>> mixed = new ArrayList<>();
		for (var i = 0; i < 4; i++) {
			mixed.add(op(OpType.CREATE, "mixed-" + i));
		}
		for (var i = 0; i < 4; i++) {
			mixed.add(op(OpType.READ, "mixed-" + i));
		}
		for (var i = 0; i < 4; i++) {
			mixed.add(op(OpType.DELETE, "mixed-" + i));
		}
		for (final var next : mixed) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(next).status(), next::toString);
		}

		final var requests = stub.objectRequests();
		assertEquals(mixed.size(), requests.size());
		requests.forEach(request -> assertEquals(request.listener(), request.sourceNic(), () -> "on a Path of its NIC: " + request));
		for (final var method : List.of("PUT", "GET", "DELETE")) {
			assertEquals(Set.of(0, 1), requests.stream().filter(request -> method.equals(request.method())).map(Request::sourceNic).collect(Collectors.toSet()), method);
		}
		assertTokensOnTheirNics(endpoints);
		assertTrue(requests.stream().filter(request -> "DELETE".equals(request.method())).allMatch(request -> request.token() == null));
		final var rows = closedRows(run.driver());
		assertEquals(mixed.size(), totals(rows).get(0), rows::toString);
	}

	@Test
	void seedAndCleanupWithOneThreadUseEveryNicAndNoLaneHoldsMoreThanItsBound() throws Exception {
		final var endpoints = stub.listeners(4);
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 1), endpoints, NICS, 1));
		for (var i = 0; i < 4; i++) {
			assertEquals(1, run.pool().admission().limit(i), "the shared mode holds one attempt per NIC");
			assertEquals(2, run.nic(i).pool.maxBuffersPerClass(), "P + 1 in the shared mode");
		}

		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "seed-" + i)).status());
		}
		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.DELETE, "seed-" + i)).status());
		}

		final var requests = stub.objectRequests();
		assertEquals(16, requests.size());
		for (final var method : List.of("PUT", "DELETE")) {
			assertEquals(Set.of(0, 1, 2, 3), requests.stream().filter(request -> method.equals(request.method())).map(Request::sourceNic).collect(Collectors.toSet()), method);
		}
		ADDRESSES.forEach(source -> assertEquals(1, stub.peakConcurrency(source), source));
		for (var i = 0; i < 4; i++) {
			assertEquals(1, run.nic(i).pool.liveBuffers(), "each NIC holds its one buffer");
		}
	}

	@Test
	void aPipelineStepsTwoRdmaContextsKeepTheirLanesAndPoolsApart() throws Exception {
		final var endpoints = stub.listeners(2);
		final var factory = stub.new Factory();
		final var stepId = "rdma-nic-pipeline-" + System.nanoTime();
		final var outcomes = runScenario(endpoints, factory, stepId, """
						PipelineLoad
							.config({"load": {"step": {"limit": {"time": "30s"}}}})
							.append({"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 2}}}})
							.append({
								"load": {"op": {"type": "read"}},
								"storage": {"net": {"interfaces": "lo-c,lo-d"}, "driver": {"limit": {"concurrency": 4}}}
							})
							.run();
						""");

		assertEquals(List.of(8L, 0L), outcomes.get("CREATE-2"), () -> "outcomes: " + outcomes);
		assertEquals(0L, outcomes.get("READ-4").get(1), () -> "outcomes: " + outcomes);
		final var drivers = factory.drivers.stream().filter(driver -> driver.openRdmaResources() != null).toList();
		assertEquals(2, drivers.size(), "one started driver per context");
		final var writer = drivers.get(0).openRdmaResources();
		final var reader = drivers.get(1).openRdmaResources();
		assertEquals(List.of("lo-a", "lo-b"), writer.nics().stream().map(nic -> nic.name).toList());
		assertEquals(List.of("lo-c", "lo-d"), reader.nics().stream().map(nic -> nic.name).toList());
		assertNotSame(writer.lanes().get(0), reader.lanes().get(0));
		assertNotSame(writer.nics().get(0).pool, reader.nics().get(0).pool);
		assertEquals(2, writer.nics().get(0).pool.maxBuffersPerClass(), "the writer's share of 1, and one more");
		assertEquals(3, reader.nics().get(0).pool.maxBuffersPerClass(), "the reader's share of 2, and one more");
		assertEquals(factory.directMemoryLimitBytes / 2 / 2, reader.nics().get(0).pool.levels().budgetBytes(), "each driver splits its own budget");

		final var puts = stub.objectRequests().stream().filter(request -> "PUT".equals(request.method())).toList();
		assertEquals(8, puts.size(), () -> "requests: " + stub.objectRequests());
		assertEquals(Set.of(0, 1), puts.stream().map(Request::sourceNic).collect(Collectors.toSet()));
		puts.forEach(put -> assertEquals(put.sourceNic(), put.tokenNic(), put.toString()));
		final var gets = stub.objectRequests().stream().filter(request -> "GET".equals(request.method())).toList();
		gets.forEach(get -> {
			assertTrue(Set.of(2, 3).contains(get.sourceNic()), get.toString());
			assertEquals(get.sourceNic(), get.tokenNic(), get.toString());
		});
		writer.lanes().forEach(lane -> assertTrue(lane.exited()));
		reader.lanes().forEach(lane -> assertTrue(lane.exited()));
		writer.nics().forEach(nic -> assertTrue(nic.closed()));
		reader.nics().forEach(nic -> assertTrue(nic.closed()));
	}

	/** Runs {@code scenario} as an engine scenario; returns each load's successes and failures. */
	private static Map<String, List<Long>> runScenario(final List<String> endpoints, final RdmaNicLoopback.Factory factory, final String stepId,
					final String scenario) throws Exception {
		final var root = RdmaNicLoopback.root(rdma(false, true, 1), endpoints, NICS.subList(0, 2), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		config.val("run-id", 779L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-limit-count", 8);
		config.val("item-data-size", "4KB");
		config.val("item-output-path", BUCKET);
		config.val("item-output-delay", 0);
		config.val("item-output-queue-limit", 1_000);
		config.val("output-color", false);
		config.val("storage-driver-type", RdmaNicLoopback.DRIVER_TYPE);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		extensions.add(factory);
		final Map<String, List<Long>> outcomes = new ConcurrentHashMap<>();
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			public void unregister(final MetricsContext metricsCtx) {
				super.unregister(metricsCtx);
				if (!(metricsCtx instanceof DistributedMetricsContext) && stepId.equals(metricsCtx.loadStepId())) {
					final var snapshot = metricsCtx.lastSnapshot();
					outcomes.put(metricsCtx.opType() + "-" + metricsCtx.concurrencyLimit(),
									List.of(snapshot.successSnapshot().count(), snapshot.failsSnapshot().count()));
				}
			}
		}) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("RDMA NIC-set scenario canary", scenario, engine, 779L).run();
		}
		return outcomes;
	}

	private void assertTokensOnTheirNics(final List<String> endpoints) {
		final var withTokens = stub.objectRequests().stream().filter(request -> request.token() != null).toList();
		assertFalse(withTokens.isEmpty());
		withTokens.forEach(request -> {
			assertEquals(request.sourceNic(), request.tokenNic(), () -> "the token names the request's NIC: " + request);
			assertEquals(endpoints.get(request.listener()), request.host(), "the configured endpoint is the authority");
		});
		stub.requests().stream().filter(request -> !request.isObjectRequest())
						.forEach(request -> assertEquals(KERNEL_SOURCE, request.source(), () -> "a control request: " + request));
	}

	@SuppressWarnings("unchecked")
	private static Operation<DataItem> rangedRead(final String name, final List<Range> fixed, final int randomRanges) {
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(0, OpType.READ, new DataItemImpl(name, 0, SIZE), null, BUCKET, CREDENTIAL,
						fixed, randomRanges);
	}

	/**
	 * D87 on RDMA: once the only Path of a NIC refuses connections, that NIC takes one trial at a
	 * time and none while its Path backs off, so the other NIC carries the writes.
	 */
	@Test
	void aNicWhosePathRefusesTakesOnlyTrialsAndTheOtherNicCarriesTheWrites() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, 2, 4);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "before")).status());
		stub.stop(1);
		awaitRefused(endpoints.get(1));
		waitFor(() -> run.pool().state().get(1).open() == 0, "the dead NIC's idle connections are dropped");

		final int writes = 100;
		final int window = 8;
		var submitted = 0;
		var succeeded = 0;
		for (; submitted < window; submitted++) {
			assertTrue(run.driver().put(op(OpType.CREATE, "write-" + submitted)));
		}
		for (var done = 0; done < writes; done++) {
			if (run.results().await().status() == Operation.Status.SUCC) {
				succeeded++;
			}
			if (submitted < writes) {
				assertTrue(run.driver().put(op(OpType.CREATE, "write-" + submitted++)));
			}
		}
		run.awaitIdle();
		assertTrue(run.pool().admission().isFailing(1));
		assertFalse(run.pool().admission().isFailing(0));

		final var rows = closedRows(run.driver());
		final long dead = rows.rows().stream().filter(row -> row.nic() != null && row.nic().iface().equals(NICS.get(1)))
						.mapToLong(NicPathRows.Row::attempts).sum();
		// Its share in flight when it failed, then a trial per back-off interval of at least 100 ms.
		assertTrue(dead >= 1 && dead <= 16, () -> "attempts on the dead NIC: " + dead + ", rows: " + rows);
		final int failed = writes - succeeded;
		assertTrue(failed <= dead, () -> "failed writes: " + failed + ", rows: " + rows);
	}

	private Run started(final List<String> endpoints, final int nics, final int concurrency) throws Exception {
		return started(RdmaNicLoopback.root(rdma(false, true, 1), endpoints, NICS.subList(0, nics), concurrency));
	}

	private Run started(final Config root) throws Exception {
		final var driver = stub.driver(root);
		resources.add(driver);
		return RdmaNicLoopback.started(driver);
	}
}
