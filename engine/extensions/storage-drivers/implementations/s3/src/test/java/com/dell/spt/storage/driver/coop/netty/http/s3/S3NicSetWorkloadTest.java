package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.KERNEL_SOURCE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.address;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nics;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.port;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.rowsOf;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.stoppedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.storage;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.nic.NicPathsArtifact;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.RangeRun;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Run;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Workload canaries of the NIC-set route (item 19, K2): each workload that ticket 03's matrix
 * carries on {@code s3} succeeds through the real drivers on {@link NicSetLoopback}'s loopback
 * NICs. Each test checks the payloads, that every attempt used a Path of its NIC (the endpoint
 * paired with it, from its NIC's address, with the configured endpoint as Host) and that the
 * connections are back in their pools at the end. Control requests keep the kernel route.
 */
@Timeout(60)
final class S3NicSetWorkloadTest {

	private static final Operation.Status SUCC = Operation.Status.SUCC;
	private static final long OBJECT_SIZE = 10;
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
		awaitNoNicIoThreads();
	}

	@Test
	void getsAndPutsSpreadByShareAndReuseEachPathsConnections() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 4, false);

		final var puts = execute(run, ops(OpType.CREATE, "obj-", 16));
		final var gets = execute(run, ops(OpType.READ, "obj-", 16));

		puts.forEach(result -> assertEquals(SUCC, result.status(), result::toString));
		for (final var result : gets) {
			assertEquals(SUCC, result.status(), result::toString);
			assertEquals(ITEM_SIZE, ((DataOperation<?>) result).countBytesDone());
		}
		IntStream.range(0, 16).forEach(i -> assertEquals(ITEM_SIZE, stub.object("/bucket/obj-" + i).length));
		final var requests = stub.objectRequests();
		assertEquals(32, requests.size(), () -> "requests: " + requests);
		assertOnPaths(run.pool(), endpoints, requests);
		for (final var nic : nics(2)) {
			final var source = address(nic);
			assertTrue(count(requests, source, "PUT") > 0 && count(requests, source, "GET") > 0, () -> nic + " carried both");
			assertTrue(stub.peakConcurrency(source) <= 2, () -> nic + " stayed within its share: " + stub.peakConcurrency(source));
			final var connections = connections(requests, source);
			assertTrue(connections.size() <= 2, () -> nic + " reused its share of connections: " + connections);
		}
		assertReleased(run);

		// Each attempt counted once, on the Path that carried it, with the plan's columns.
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(32L, 32L, 0L, 0L, 0L, 32L * ITEM_SIZE), totals(rows), rows::toString);
		for (final var row : rowsOf(rows, NicPathRows.Kind.PATH)) {
			final var nic = row.nic();
			final var endpoint = row.endpoint();
			assertEquals(address(nic.iface()), nic.ipv4());
			assertEquals(0, nic.numaNode());
			assertEquals("2", nic.permitShare(), "P/N of the concurrency limit");
			assertEquals(port(endpoint.endpoint()), endpoint.port());
			assertEquals(requests.stream().filter(request -> request.source().equals(nic.ipv4())
							&& request.host().equals(endpoint.endpoint())).count(), row.attempts(), row::toString);
		}
		assertEquals(2, rowsOf(rows, NicPathRows.Kind.NIC).size());
		assertEquals(1, rowsOf(rows, NicPathRows.Kind.NONE).size());
		for (final var row : rows.rows()) {
			if (row.kind() != NicPathRows.Kind.PATH) {
				assertEquals(0, row.attempts(), row::toString);
			}
		}
	}

	@Test
	void integrityVerifiedGetsPassOnEveryPathAndACorruptedObjectFails() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 2, true);

		execute(run, ops(OpType.CREATE, "verified-", 4)).forEach(result -> assertEquals(SUCC, result.status()));
		execute(run, ops(OpType.READ, "verified-", 4)).forEach(result -> assertEquals(SUCC, result.status(), result::toString));

		final var gets = stub.objectRequests().stream().filter(request -> "GET".equals(request.method())).toList();
		assertEquals(Set.of(0, 1), gets.stream().map(Request::listener).collect(Collectors.toSet()), "verified on both Paths");
		stub.object("/bucket/verified-0")[0] ^= 1;
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, run.execute(dataOp(OpType.READ, "verified-0")).status(),
						"the verification ran");
		assertOnPaths(run.pool(), endpoints, stub.objectRequests());
		assertReleased(run);
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(9L, 8L, 1L, 0L, 0L, 8L * ITEM_SIZE), totals(rows), "the corrupted GET failed with no bytes: " + rows);
	}

	@Test
	void rangeReadsReceiveTheBytesOfEachRangeAtAFixedAndAtChosenOffsets() throws Exception {
		stub.absentObjectSize = OBJECT_SIZE;
		final var endpoints = stub.listeners(2);

		final var fixed = rangeRun(endpoints, new RangeReadPolicy(3, 2L, 1));
		final var fixedRanges = readRanges(fixed, "fixed-", 4);
		assertEquals(List.of(4L, 4L, 0L, 0L, 0L, 12L), totals(stoppedRows(fixed.driver, fixed)), "each range's bytes");
		final var chosen = rangeRun(endpoints, new RangeReadPolicy(3, null, 1));
		final var chosenRanges = readRanges(chosen, "chosen-", 8);
		assertEquals(List.of(8L, 8L, 0L, 0L, 0L, 24L), totals(stoppedRows(chosen.driver, chosen)));

		assertEquals(Set.of("bytes=2-4"), Set.copyOf(fixedRanges));
		assertTrue(Set.copyOf(chosenRanges).size() > 1, () -> "the offsets vary: " + chosenRanges);
		final var requests = stub.objectRequests();
		assertEquals(12, requests.size(), () -> "requests: " + requests);
		for (final var request : requests) {
			assertEquals(address(nics(2).get(request.listener())), request.source(), request::toString);
			assertEquals(endpoints.get(request.listener()), request.host());
		}
		assertEquals(Set.of(0, 1), requests.stream().map(Request::listener).collect(Collectors.toSet()));
		for (final var run : List.of(fixed, chosen)) {
			assertTrue(run.runtime.snapshot().reconciled(), () -> "reconciled: " + run.runtime.snapshot());
			assertEquals(0, run.pool().pendingCount());
		}
	}

	@Test
	void multipartUploadsSendEveryRequestOnAPathAndAbortOnTheKernelRoute() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 2, false);

		final var upload = multipart("uploaded", 5);
		assertTrue(run.driver().put(upload));
		assertEquals(SUCC, awaitUpload(run, upload).status());

		assertEquals(5L * ITEM_SIZE, stub.object("/bucket/uploaded").length);
		final var requests = stub.objectRequests();
		assertEquals(List.of("POST init", "PUT part", "PUT part", "PUT part", "PUT part", "PUT part", "POST complete"),
						requests.stream().map(S3NicSetWorkloadTest::phase).toList());
		assertOnPaths(run.pool(), endpoints, requests);
		assertEquals(Set.of(0, 1), requests.stream().filter(request -> request.query().contains("partNumber="))
						.map(Request::listener).collect(Collectors.toSet()), "the parts went over both NICs");

		// The second upload's completion fails; the abort that follows is a control request.
		final int completion = requests.size() + 7;
		stub.failObjectRequest = n -> n == completion;
		final var failing = multipart("aborted", 5);
		assertTrue(run.driver().put(failing));
		assertNotEquals(SUCC, awaitUpload(run, failing).status());
		waitFor(() -> stub.requests().stream().anyMatch(S3NicSetWorkloadTest::isAbort), "the failed upload was aborted");
		final var abort = stub.requests().stream().filter(S3NicSetWorkloadTest::isAbort).findFirst().orElseThrow();
		assertEquals(KERNEL_SOURCE, abort.source(), "the abort keeps the kernel route");
		assertEquals("/bucket/aborted", abort.path());
		final var attempts = stub.objectRequests().stream().filter(request -> !isAbort(request)).toList();
		assertEquals(14, attempts.size(), () -> "requests: " + attempts);
		assertOnPaths(run.pool(), endpoints, attempts);
		assertReleased(run);
		// Initiations and completions carry no payload; the failed completion fails; the abort is not counted.
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(14L, 13L, 1L, 0L, 0L, 10L * ITEM_SIZE), totals(rows), rows::toString);
	}

	@Test
	void aMixedStepsGetsPutsDeletesAndHeadsStayWithinTheShares() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 4, false);
		execute(run, ops(OpType.CREATE, "mixed-", 8)).forEach(result -> assertEquals(SUCC, result.status()));

		final List<DataOperation<DataItem>> mixed = new ArrayList<>();
		for (var i = 0; i < 2; i++) {
			mixed.add(dataOp(OpType.READ, "mixed-" + i));
			mixed.add(dataOp(OpType.CREATE, "added-" + i));
			mixed.add(dataOp(OpType.STAT, "mixed-" + (2 + i)));
			mixed.add(dataOp(OpType.DELETE, "mixed-" + (4 + i)));
		}
		final var results = execute(run, mixed);

		final Map<OpType, Integer> succeeded = new HashMap<>();
		for (final var result : results) {
			assertEquals(SUCC, result.status(), result::toString);
			succeeded.merge(result.type(), 1, Integer::sum);
		}
		assertEquals(Map.of(OpType.READ, 2, OpType.CREATE, 2, OpType.STAT, 2, OpType.DELETE, 2), succeeded);
		assertEquals(ITEM_SIZE, stub.object("/bucket/added-1").length);
		assertEquals(null, stub.object("/bucket/mixed-4"));
		final var requests = stub.objectRequests();
		assertEquals(Map.of("PUT", 10L, "GET", 2L, "HEAD", 2L, "DELETE", 2L),
						requests.stream().collect(Collectors.groupingBy(Request::method, Collectors.counting())));
		assertOnPaths(run.pool(), endpoints, requests);
		for (final var nic : nics(2)) {
			assertTrue(stub.peakConcurrency(address(nic)) <= 2, () -> nic + " stayed within its share");
		}
		assertReleased(run);
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void batchDeletesAndListOperationsGoOnPaths() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 2, false);
		execute(run, ops(OpType.CREATE, "batch-", 4)).forEach(result -> assertEquals(SUCC, result.status()));

		final var batches = List.of(batchDelete("batch-0", "batch-1"), batchDelete("batch-2", "batch-3"));
		final var list = new ListOperationImpl<>(0, OpType.LIST, new PathItemImpl("batch-"), CREDENTIAL);
		list.srcPath("/bucket");
		final List<Operation<?>> ops = new ArrayList<>(batches);
		ops.add(list);
		final List<Operation<?>> results = execute(run, (List) ops);

		results.forEach(result -> assertEquals(SUCC, result.status(), result::toString));
		IntStream.range(0, 4).forEach(i -> assertEquals(null, stub.object("/bucket/batch-" + i)));
		final var listed = results.stream().filter(ListOperation.class::isInstance).findFirst().orElseThrow();
		assertNotNull(listed);
		// Both the batch deletes and the LIST operation address the bucket itself.
		final var bucketRequests = stub.requests().stream().filter(request -> !request.isObjectRequest())
						.filter(request -> !KERNEL_SOURCE.equals(request.source())).toList();
		assertEquals(List.of("GET list", "POST delete", "POST delete"), bucketRequests.stream()
						.map(request -> request.method() + (request.query().contains("list-type") ? " list" : " " + request.query()))
						.sorted().toList());
		assertOnPaths(run.pool(), endpoints, bucketRequests);
		assertOnPaths(run.pool(), endpoints, stub.objectRequests());
		assertReleased(run);
	}

	@Test
	void taggingUpdatesGoOnPaths() throws Exception {
		final var endpoints = stub.listeners(2);
		final var storage = storage(endpoints, nics(2), 2);
		storage.val("object-tagging-enabled", true);
		storage.val("object-tagging-tags", Map.of("canary", "nic-set"));
		final var driver = NicSetLoopback.driver(storage, false);
		resources.add(driver);
		final var run = NicSetLoopback.started(driver);

		execute(run, ops(OpType.UPDATE, "tagged-", 4)).forEach(result -> assertEquals(SUCC, result.status(), result::toString));

		final var requests = stub.objectRequests();
		assertEquals(4, requests.size(), () -> "requests: " + requests);
		requests.forEach(request -> assertEquals("PUT tagging", request.method() + " " + request.query()));
		IntStream.range(0, 4).forEach(i -> assertTrue(stub.tags("/bucket/tagged-" + i).contains("<Key>canary</Key>")));
		assertOnPaths(run.pool(), endpoints, requests);
		assertEquals(Set.of(0, 1), requests.stream().map(Request::listener).collect(Collectors.toSet()));
		assertReleased(run);
	}

	@Test
	void targetBucketsChooseTheBucketNotTheConnection() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = started(endpoints, nics(2), 2, false);

		final List<DataOperation<DataItem>> ops = new ArrayList<>();
		for (var i = 0; i < 8; i++) {
			ops.add(new DataOperationImpl<>(0, OpType.CREATE, new DataItemImpl("spread-" + i, i, ITEM_SIZE), null,
							"/bucket-" + (i % 2), CREDENTIAL, null, 0));
		}
		execute(run, ops).forEach(result -> assertEquals(SUCC, result.status(), result::toString));

		IntStream.range(0, 8).forEach(i -> assertEquals(ITEM_SIZE, stub.object("/bucket-" + (i % 2) + "/spread-" + i).length));
		final var requests = stub.objectRequests();
		assertEquals(8, requests.size());
		assertOnPaths(run.pool(), endpoints, requests);
		assertEquals(Set.of(0, 1), requests.stream().map(Request::listener).collect(Collectors.toSet()));
		assertReleased(run);
	}

	@Test
	void seedAndCleanupInTheSharedModeUseEveryNicAndListOnTheKernelRoute() throws Exception {
		final var endpoints = stub.listeners(4);
		final var run = started(endpoints, nics(4), 1, false);

		// The listing parses each key as a generated id, so the seeded names are numbers.
		execute(run, ops(OpType.CREATE, "", 8)).forEach(result -> assertEquals(SUCC, result.status()));
		@SuppressWarnings("unchecked")
		final var items = (ItemFactory<DataItem>) ItemType.getItemFactory(ItemType.DATA);
		// A complete listing ends with a null.
		final var listed = run.driver().list(items, "/bucket", null, 10, null, 100).stream().filter(Objects::nonNull).toList();
		final List<DataOperation<DataItem>> deletes = listed.stream()
						.map(item -> (DataOperation<DataItem>) new DataOperationImpl<>(0, OpType.DELETE, item, null, "/bucket",
										CREDENTIAL, null, 0))
						.toList();
		execute(run, deletes).forEach(result -> assertEquals(SUCC, result.status(), result::toString));

		assertEquals(8, listed.size(), () -> "listed: " + listed);
		IntStream.range(0, 8).forEach(i -> assertEquals(null, stub.object("/bucket/" + i)));
		final var listing = stub.requests().stream()
						.filter(request -> "GET".equals(request.method()) && !request.isObjectRequest()).toList();
		assertEquals(1, listing.size(), () -> "requests: " + stub.requests());
		assertEquals(KERNEL_SOURCE, listing.get(0).source(), "the listing keeps the kernel route");
		final var requests = stub.objectRequests();
		assertEquals(16, requests.size());
		assertOnPaths(run.pool(), endpoints, requests);
		for (final var method : List.of("PUT", "DELETE")) {
			assertEquals(Set.copyOf(NicSetLoopback.ADDRESSES), requests.stream().filter(request -> method.equals(request.method()))
							.map(Request::source).collect(Collectors.toSet()), method + " used every NIC");
		}
		NicSetLoopback.ADDRESSES.forEach(source -> assertEquals(1, stub.peakConcurrency(source), source));
		assertReleased(run);
	}

	/**
	 * A pipeline step as an engine scenario: each of its two contexts gets its own driver, Path Pools
	 * and permits, on different NICs and with different limits.
	 *
	 * <p>The reading context's operations are not asserted: its generator can take the handoff empty,
	 * before the first write ends, and then ends with no reads on either route
	 * ({@code .scratch/pipeline-handoff-race}). The weighted canary below has two contexts carry
	 * traffic at once.
	 */
	@Test
	void aPipelineStepsContextsGetTheirOwnDriversPathPoolsAndPermits() throws Exception {
		final var endpoints = stub.listeners(2);
		final var factory = new NicSetLoopback.Factory();
		final var stepId = "nic-set-pipeline-" + System.nanoTime();
		final var outcomes = runScenario(endpoints, factory, stepId, """
						PipelineLoad
							.config({"load": {"step": {"limit": {"time": "30s"}}}})
							.append({"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 2}}}})
							.append({
								"load": {"op": {"type": "read"}},
								"item": {"data": {"verify": true}},
								"storage": {"net": {"interfaces": "lo-c,lo-d"}, "driver": {"limit": {"concurrency": 4}}}
							})
							.run();
						""");

		assertEquals(List.of(8L, 0L), outcomes.get("CREATE-2"), () -> "outcomes: " + outcomes);
		assertEquals(0L, outcomes.get("READ-4").get(1), () -> "outcomes: " + outcomes);
		final var pools = stepPools(factory);
		assertEquals(2, pools.size(), "one started driver per context");
		final var writer = pools.get(0);
		final var reader = pools.get(1);
		assertSeparate(writer, List.of("lo-a", "lo-b"), List.of(1, 1), reader, List.of("lo-c", "lo-d"), List.of(2, 2));
		reader.state().forEach(nic -> assertTrue(nic.paths().stream().allMatch(path -> path.connectsStarted() > 0),
						() -> "the reader pre-opened its own connections: " + nic));
		final var puts = stub.objectRequests().stream().filter(request -> "PUT".equals(request.method())).toList();
		assertEquals(8, puts.size(), () -> "requests: " + stub.objectRequests());
		assertOnPaths(writer, endpoints, puts);
		assertEquals(Set.of(address("lo-a"), address("lo-b")), puts.stream().map(Request::source).collect(Collectors.toSet()));
		final var gets = stub.objectRequests().stream().filter(request -> "GET".equals(request.method())).toList();
		if (!gets.isEmpty()) {
			assertOnPaths(reader, endpoints, gets);
		}
		List.of("lo-a", "lo-b").forEach(nic -> assertTrue(stub.peakConcurrency(address(nic)) <= 1, nic));
		List.of("lo-c", "lo-d").forEach(nic -> assertTrue(stub.peakConcurrency(address(nic)) <= 2, nic));

		// One file for the step: each context's rows on its own NICs, then the end record.
		final var file = nicPathsFile(stepId);
		final var rows = csv(file);
		final var end = rows.get(rows.size() - 1);
		assertEquals("end", end.get("row"));
		assertEquals("true", end.get("complete"));
		assertEquals("", end.get("missing_contexts"));
		assertEquals("0", end.get("unresolved"));
		final var body = rows.subList(0, rows.size() - 1);
		assertTrue(body.stream().allMatch(row -> stepId.equals(row.get("step_id")) && "1".equals(row.get("schema_version"))));
		assertEquals(Map.of("0", Set.of("lo-a", "lo-b"), "1", Set.of("lo-c", "lo-d")), body.stream()
						.filter(row -> !"none".equals(row.get("row")))
						.collect(Collectors.groupingBy(row -> row.get("context_index"),
										Collectors.mapping(row -> row.get("interface"), Collectors.toSet()))));
		assertEquals(8L, sum(body, "0", "succeeded"));
		assertEquals(8L * 1024, sum(body, "0", "bytes"));
		assertEquals(outcomes.get("READ-4").get(0), sum(body, "1", "succeeded"));
		Files.delete(file);

		// And one report: the two drivers agree on their threads, not on their NICs (D64).
		final var setFile = nicSetFile(stepId);
		final var report = MAPPER.readTree(setFile.toFile());
		assertEquals(1, report.get("schema_version").asInt());
		assertEquals(stepId, report.get("step_id").asText());
		assertEquals(1, report.get("driver_threads").asInt());
		assertEquals(1, report.get("event_loops_per_nic").asInt());
		assertFalse(report.has("nics"), () -> "report: " + report);
		assertEquals(4, report.get("event_loops").asInt());
		final var contexts = report.get("contexts");
		assertEquals(2, contexts.size(), () -> "report: " + report);
		assertEquals(List.of("lo-a", "lo-b"), names(contexts.get(0).get("nics")));
		assertEquals(List.of("lo-c", "lo-d"), names(contexts.get(1).get("nics")));
		assertEquals(1, contexts.get(1).get("context_index").asInt());
		assertEquals(2, contexts.get(1).get("event_loops").asInt());
		Files.delete(setFile);
	}

	/**
	 * A step whose NIC set cannot be planned still writes its file: the end record alone, naming the
	 * context that never handed over its counts (item 18).
	 */
	@Test
	void aStepThatFailsToResolveAnEndpointWritesOnlyTheEndRecord() throws Exception {
		final var stepId = "nic-set-unresolved-" + System.nanoTime();
		final var outcomes = runScenario(List.of(stub.listeners(1).get(0), "nic-paths.invalid:9000"),
						new NicSetLoopback.Factory(), stepId, """
										Load.config({"load": {"step": {"limit": {"time": "30s"}}}}).run();
										""");

		assertTrue(stub.objectRequests().isEmpty(), () -> "outcomes: " + outcomes + ", requests: " + stub.objectRequests());
		final var file = nicPathsFile(stepId);
		final var rows = csv(file);
		assertEquals(1, rows.size(), () -> "rows: " + rows);
		final var end = rows.get(0);
		assertEquals("end", end.get("row"));
		assertEquals("false", end.get("complete"));
		assertEquals("0", end.get("missing_contexts"));
		assertEquals("0", end.get("attempts"));
		Files.delete(file);

		final var setFile = nicSetFile(stepId);
		final var report = MAPPER.readTree(setFile.toFile());
		assertEquals(1, report.get("driver_threads").asInt());
		assertEquals(List.of(), names(report.get("nics")), "a NIC set that never opened has no NICs");
		assertEquals(0, report.get("event_loops").asInt());
		assertFalse(report.has("contexts"));
		Files.delete(setFile);
	}

	/** A step without a NIC set writes no per-path file (item 18). */
	@Test
	void aStepWithoutANicSetWritesNoNicPathsFile() throws Exception {
		final var stepId = "nic-set-none-" + System.nanoTime();
		final var outcomes = runScenario(stub.listeners(1), null, stepId, """
						Load.config({"load": {"step": {"limit": {"time": "30s"}}}}).run();
						""");

		assertEquals(Map.of("CREATE-1", List.of(8L, 0L)), outcomes);
		assertFalse(Files.exists(nicPathsFile(stepId)));
		assertFalse(Files.exists(nicSetFile(stepId)));
	}

	/**
	 * A weighted step's two contexts write at once, each through its own driver's Path Pools on its
	 * own NICs and within its own permits.
	 */
	@Test
	void aWeightedStepsContextsWriteAtOnceThroughTheirOwnPathPoolsAndPermits() throws Exception {
		final var endpoints = stub.listeners(2);
		final var factory = new NicSetLoopback.Factory();
		final var outcomes = runScenario(endpoints, factory, """
						WeightedLoad
							.config({"load": {"step": {"limit": {"time": "30s"}}}})
							.append({"storage": {"net": {"interfaces": "lo-a,lo-b"}, "driver": {"limit": {"concurrency": 2}}}})
							.append({"storage": {"net": {"interfaces": "lo-c,lo-d"}, "driver": {"limit": {"concurrency": 4}}}})
							.run();
						""");

		assertEquals(Map.of("CREATE-2", List.of(8L, 0L), "CREATE-4", List.of(8L, 0L)), outcomes);
		final var pools = stepPools(factory);
		assertEquals(2, pools.size(), "one started driver per context");
		final var first = pools.get(0);
		final var second = pools.get(1);
		assertSeparate(first, List.of("lo-a", "lo-b"), List.of(1, 1), second, List.of("lo-c", "lo-d"), List.of(2, 2));
		final var requests = stub.objectRequests();
		assertEquals(16, requests.size(), () -> "requests: " + requests);
		final var firstPuts = requests.stream().filter(request -> Set.of(address("lo-a"), address("lo-b"))
						.contains(request.source())).toList();
		final var secondPuts = requests.stream().filter(request -> !firstPuts.contains(request)).toList();
		assertEquals(8, firstPuts.size(), () -> "requests: " + requests);
		assertOnPaths(first, endpoints, firstPuts);
		assertOnPaths(second, endpoints, secondPuts);
		NicSetLoopback.ADDRESSES.forEach(source -> assertTrue(requests.stream().anyMatch(request -> source.equals(request.source())),
						() -> source + " carried writes"));
		List.of("lo-a", "lo-b").forEach(nic -> assertTrue(stub.peakConcurrency(address(nic)) <= 1, nic));
		List.of("lo-c", "lo-d").forEach(nic -> assertTrue(stub.peakConcurrency(address(nic)) <= 2, nic));
	}

	/**
	 * Runs {@code scenario} as the engine runs one, with 8 operations of 1 KB per context on the
	 * fixture's driver type, and returns each local context's successes and failures by operation
	 * type and concurrency limit, such as {@code CREATE-2}. A step keeps one terminal entry, of the
	 * context that ended last, so the counts are taken as each context unregisters.
	 */
	private static Map<String, List<Long>> runScenario(final List<String> endpoints, final NicSetLoopback.Factory factory,
					final String scenario) throws Exception {
		return runScenario(endpoints, factory, "nic-set-scenario-" + System.nanoTime(), scenario);
	}

	/** As above with step id {@code stepId}; without a {@code factory}, on the shipped {@code s3} driver and no NIC set. */
	private static Map<String, List<Long>> runScenario(final List<String> endpoints, final NicSetLoopback.Factory factory,
					final String stepId, final String scenario) throws Exception {
		final var root = NicSetLoopback.root(endpoints, factory == null ? List.of() : nics(2), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		config.val("run-id", 778L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-limit-count", 8);
		config.val("load-op-weight", 1);
		config.val("item-data-size", "1KB");
		config.val("item-output-path", "/bucket");
		config.val("item-output-delay", 0);
		config.val("item-output-queue-limit", 1_000);
		config.val("output-color", false);
		config.val("storage-driver-type", factory == null ? "s3" : NicSetLoopback.DRIVER_TYPE);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		if (factory != null) {
			extensions.add(factory);
		}
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
			new RunImpl("NIC-set scenario canary", scenario, engine, 778L).run();
		}
		return outcomes;
	}

	private static Path nicPathsFile(final String stepId) throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(Loggers.NIC_PATHS.getName(), stepId));
	}

	private static Path nicSetFile(final String stepId) throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(Loggers.NIC_SET.getName(), stepId));
	}

	private static List<String> names(final JsonNode array) {
		final List<String> out = new ArrayList<>();
		array.forEach(node -> out.add(node.asText()));
		return out;
	}

	/** The file's records by column; no field of these files is quoted. */
	private static List<Map<String, String>> csv(final Path file) throws IOException {
		final var lines = Files.readAllLines(file);
		assertEquals(NicPathsArtifact.HEADER, lines.get(0));
		final var header = lines.get(0).split(",", -1);
		final List<Map<String, String>> records = new ArrayList<>();
		for (final var line : lines.subList(1, lines.size())) {
			final var fields = line.split(",", -1);
			assertEquals(header.length, fields.length, line);
			final Map<String, String> record = new HashMap<>();
			IntStream.range(0, header.length).forEach(i -> record.put(header[i], fields[i]));
			records.add(record);
		}
		return records;
	}

	private static long sum(final List<Map<String, String>> rows, final String context, final String column) {
		return rows.stream().filter(row -> context.equals(row.get("context_index")))
						.mapToLong(row -> Long.parseLong(row.get(column))).sum();
	}

	/**
	 * The Path Pools of the drivers the step started, in creation order. The step client also creates
	 * drivers for its own checks and closes them unstarted, so they never connect.
	 */
	private static List<NicSetConnectionPool> stepPools(final NicSetLoopback.Factory factory) {
		return factory.drivers.stream().map(S3NicSetDriver::connectionPool).filter(Objects::nonNull)
						.filter(pool -> pool.state().stream().flatMap(nic -> nic.paths().stream())
										.anyMatch(path -> path.connectsStarted() > 0))
						.toList();
	}

	/** Two pools apart, each on its NICs with its admission limits, and each with nothing left counted. */
	private static void assertSeparate(final NicSetConnectionPool first, final List<String> firstNics,
					final List<Integer> firstLimits, final NicSetConnectionPool second, final List<String> secondNics,
					final List<Integer> secondLimits) {
		assertNotSame(first, second);
		assertEquals(firstNics, first.state().stream().map(NicSetConnectionPool.NicState::nic).toList());
		assertEquals(secondNics, second.state().stream().map(NicSetConnectionPool.NicState::nic).toList());
		assertEquals(firstLimits, limits(first));
		assertEquals(secondLimits, limits(second));
		for (final var pool : List.of(first, second)) {
			assertEquals(List.of(0, 0), counts(pool));
			assertEquals(0, pool.pendingCount());
		}
	}

	private Run started(final List<String> endpoints, final List<String> interfaces, final int concurrency,
					final boolean verify) throws Exception {
		final var driver = NicSetLoopback.driver(storage(endpoints, interfaces, concurrency), verify);
		resources.add(driver);
		return NicSetLoopback.started(driver);
	}

	private RangeRun rangeRun(final List<String> endpoints, final RangeReadPolicy policy) throws Exception {
		final var run = new RangeRun(storage(endpoints, nics(2), 2), policy, OBJECT_SIZE);
		resources.add(run);
		return run;
	}

	/** Reads {@code count} objects and returns each read's range as a request header, in order. */
	private static List<String> readRanges(final RangeRun run, final String prefix, final int count) throws Exception {
		final List<String> ranges = new ArrayList<>();
		for (var i = 0; i < count; i++) {
			run.read(prefix + i);
			final var result = run.outcome();
			assertEquals(SUCC, result.status(), result::toString);
			final var range = result.selection().range();
			assertEquals(range.length(), result.countBytesDone(), "every byte of the range was received");
			ranges.add(range.requestHeader());
		}
		return ranges;
	}

	/** Puts every operation at once and waits for as many results. */
	private static List<Operation<?>> execute(final Run run, final List<? extends Operation<DataItem>> ops)
					throws InterruptedException {
		for (final var op : ops) {
			assertTrue(run.driver().put(op));
		}
		final List<Operation<?>> results = new ArrayList<>();
		for (var i = 0; i < ops.size(); i++) {
			results.add(run.results().await());
		}
		return results;
	}

	private static List<DataOperation<DataItem>> ops(final OpType type, final String prefix, final int count) {
		return IntStream.range(0, count).mapToObj(i -> dataOp(type, prefix + i)).toList();
	}

	private static CompositeDataOperation<DataItem> multipart(final String name, final int parts) {
		return new CompositeDataOperationImpl<>(0, OpType.CREATE, new DataItemImpl(name, 7, (long) parts * ITEM_SIZE), null,
						"/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
	}

	/** The upload's own result, once its completion was answered and nothing is in flight. */
	private static Operation<?> awaitUpload(final Run run, final CompositeDataOperation<DataItem> upload)
					throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		Operation<?> last = null;
		while (System.nanoTime() < deadline) {
			final var result = run.results().poll();
			if (result != null) {
				last = result;
			} else if (last instanceof CompositeDataOperation<?> composite && composite.allSubOperationsDone()
							&& last.item().name().equals(upload.item().name()) && run.driver().activeOpCount() == 0) {
				return last;
			}
		}
		throw new AssertionError("the upload did not finish: " + last);
	}

	private static DeleteRequestOperation batchDelete(final String... keys) {
		final List<DeleteTarget> targets = new ArrayList<>();
		for (final var key : keys) {
			targets.add(new DeleteTarget(new IntegrityManifestDataItem("bucket", key, 0, null)));
		}
		return new DeleteRequestOperationImpl(0, new DeleteRequest("bucket", CREDENTIAL, targets));
	}

	private static String phase(final Request request) {
		final var query = request.query() == null ? "" : request.query();
		return request.method() + (query.contains("uploads") ? " init" : query.contains("partNumber=") ? " part" : " complete");
	}

	private static boolean isAbort(final Request request) {
		return "DELETE".equals(request.method()) && request.query() != null && request.query().contains("uploadId=");
	}

	/**
	 * Each request went to the endpoint paired with a NIC of {@code pool}, from that NIC's address,
	 * with the configured endpoint as Host.
	 */
	private static void assertOnPaths(final NicSetConnectionPool pool, final List<String> endpoints, final List<Request> requests) {
		final Map<String, String> nicOfEndpoint = new HashMap<>();
		pool.state().forEach(nic -> nic.paths().forEach(path -> nicOfEndpoint.put(path.endpoint(), nic.nic())));
		assertTrue(!requests.isEmpty());
		for (final var request : requests) {
			final var endpoint = endpoints.get(request.listener());
			final var nic = nicOfEndpoint.get(endpoint);
			assertNotNull(nic, () -> endpoint + " has a Path on " + pool.state());
			assertEquals(address(nic), request.source(), () -> request + " left through " + nic);
			assertEquals(endpoint, request.host(), request::toString);
		}
	}

	/** Every attempt ended, its NIC's room and permit came back, and every connection is idle. */
	private static void assertReleased(final Run run) throws InterruptedException {
		final var pool = run.pool();
		waitFor(() -> run.driver().activeOpCount() == 0 && pool.pendingCount() == 0
						&& counts(pool).stream().allMatch(count -> count == 0)
						&& pool.state().stream().allMatch(nic -> nic.open() == nic.idle()),
						"released: " + pool.state() + ", counts " + counts(pool));
		for (final var nic : pool.state()) {
			assertTrue(nic.open() <= nic.openLimit(), nic::toString);
		}
	}

	private static long count(final List<Request> requests, final String source, final String method) {
		return requests.stream().filter(request -> source.equals(request.source()) && method.equals(request.method())).count();
	}

	private static Set<Integer> connections(final List<Request> requests, final String source) {
		return requests.stream().filter(request -> source.equals(request.source())).map(Request::port)
						.collect(Collectors.toCollection(HashSet::new));
	}

	private static List<Integer> counts(final NicSetConnectionPool pool) {
		final var admission = pool.admission();
		return IntStream.range(0, admission.size()).map(admission::count).boxed().toList();
	}

	private static List<Integer> limits(final NicSetConnectionPool pool) {
		final var admission = pool.admission();
		return IntStream.range(0, admission.size()).map(admission::limit).boxed().toList();
	}
}
