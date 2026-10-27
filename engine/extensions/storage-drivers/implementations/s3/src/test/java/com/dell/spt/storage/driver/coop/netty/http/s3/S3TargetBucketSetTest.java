package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.dell.spt.base.data.SeedDataInput;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Netty S3 driver addresses the member of a target bucket set that each operation names: new
 * objects the member their builder selected, existing objects their recorded bucket, multipart
 * requests their parent's member. Every endpoint-selection mode routes the same paths.
 */
final class S3TargetBucketSetTest {

	private static final int ITEM_SIZE = EndpointSelectionTestSupport.ITEM_SIZE;
	private static final List<String> MEMBERS = TargetBucketSet.memberPaths("/bench/prefix", 4);

	private record Captured(String method, String path, String query) {}

	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private HttpServer server;
	private volatile int completeStatus = 200;

	@AfterEach
	void tearDown() throws Exception {
		for (final var resource : resources) {
			resource.close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
	}

	@Test
	void objectRequestsAddressTheMemberTheirOperationNamesWithDefaultSelection() throws Exception {
		objectRequestsAddressTheMemberTheirOperationNames("default");
	}

	@Test
	void objectRequestsAddressTheMemberTheirOperationNamesWithRoundRobin() throws Exception {
		objectRequestsAddressTheMemberTheirOperationNames("round-robin");
	}

	@Test
	void objectRequestsAddressTheMemberTheirOperationNamesWithPerRequestDns() throws Exception {
		objectRequestsAddressTheMemberTheirOperationNames("per-request-dns");
	}

	private void objectRequestsAddressTheMemberTheirOperationNames(final String mode) throws Exception {
		final var driver = driver(mode);
		final var created = new ArrayList<Operation<?>>();
		for (long id = 0; id < 8; id++) {
			final var op = build(OpType.CREATE, new DataItemImpl("k" + id, id, ITEM_SIZE), Long.MAX_VALUE);
			assertTrue(driver.driver.put(op));
			final var result = driver.results.await();
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			created.add(result);
		}
		for (final var opType : List.of(OpType.READ, OpType.DELETE)) {
			for (final var result : created) {
				final var item = (DataItem) result.item();
				assertTrue(driver.driver.put(build(opType, new DataItemImpl(item.name(), item.offset(), ITEM_SIZE),
								Long.MAX_VALUE)));
				assertEquals(Operation.Status.SUCC, driver.results.await().status());
			}
		}

		for (long id = 0; id < 8; id++) {
			final var object = "/bench-" + (id % 4) + "/prefix/k" + id;
			assertEquals(1, count("PUT", object), object + " " + requests);
			assertEquals(1, count("GET", object), object + " " + requests);
			assertEquals(1, count("DELETE", object), object + " " + requests);
		}
		final Map<String, Long> bucketChecks = requests.stream()
						.filter(r -> "HEAD".equals(r.method()))
						.collect(Collectors.groupingBy(Captured::path, Collectors.counting()));
		assertEquals(Map.of("/bench-0", 1L, "/bench-1", 1L, "/bench-2", 1L, "/bench-3", 1L), bucketChecks,
						"each member is checked once, on first use; the base never");
	}

	@Test
	void multipartRequestsAddressTheirParentsMemberWithDefaultSelection() throws Exception {
		multipartRequestsAddressTheirParentsMember("default");
	}

	@Test
	void multipartRequestsAddressTheirParentsMemberWithRoundRobin() throws Exception {
		multipartRequestsAddressTheirParentsMember("round-robin");
	}

	@Test
	void multipartRequestsAddressTheirParentsMemberWithPerRequestDns() throws Exception {
		multipartRequestsAddressTheirParentsMember("per-request-dns");
	}

	private void multipartRequestsAddressTheirParentsMember(final String mode) throws Exception {
		final var driver = driver(mode);
		final var op = build(OpType.CREATE, new DataItemImpl("big", 7, 3L * ITEM_SIZE), ITEM_SIZE);
		assertInstanceOf(CompositeDataOperation.class, op);

		assertTrue(driver.driver.put(op));
		awaitRequest("POST", "uploadId");

		final var object = "/bench-3/prefix/big";
		assertTrue(requests.stream().anyMatch(r -> "POST".equals(r.method()) && object.equals(r.path())
						&& r.query() != null && r.query().contains("uploads")), requests::toString);
		assertEquals(3, requests.stream().filter(r -> "PUT".equals(r.method()) && object.equals(r.path())
						&& r.query() != null && r.query().contains("partNumber")).count(), requests::toString);
		assertTrue(requests.stream().allMatch(r -> r.path().startsWith("/bench-3")), requests::toString);
	}

	@Test
	void anAbortedUploadIsAbortedInItsMember() throws Exception {
		completeStatus = 500;
		final var driver = driver("default");

		assertTrue(driver.driver.put(build(OpType.CREATE, new DataItemImpl("big", 6, 3L * ITEM_SIZE), ITEM_SIZE)));
		awaitRequest("DELETE", "uploadId");

		assertTrue(requests.stream().anyMatch(r -> "DELETE".equals(r.method()) && "/bench-2/prefix/big".equals(r.path())
						&& r.query() != null && r.query().contains("uploadId")), requests::toString);
		assertTrue(requests.stream().allMatch(r -> r.path().startsWith("/bench-2")), requests::toString);
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static Operation<DataItem> build(final OpType opType, final DataItem item, final long sizeThreshold)
					throws IOException {
		final var builder = new DataOperationsBuilderImpl(0);
		builder.opType(opType);
		builder.credentialInput(new ConstantValueInputImpl<>(EndpointSelectionTestSupport.CREDENTIAL));
		builder.sizeThreshold(sizeThreshold);
		if (OpType.CREATE.equals(opType)) {
			builder.outputPathInput(new ConstantValueInputImpl<>("/bench/prefix"));
			builder.targetBucketMembers(MEMBERS, 0);
		}
		return (Operation<DataItem>) builder.buildOp(item);
	}

	private long count(final String method, final String path) {
		return requests.stream().filter(r -> method.equals(r.method()) && path.equals(r.path()) && r.query() == null)
						.count();
	}

	private void awaitRequest(final String method, final String queryPart) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			if (requests.stream().anyMatch(r -> method.equals(r.method()) && r.query() != null
							&& r.query().contains(queryPart) && !(method.equals("POST") && r.query().contains("uploads")))) {
				return;
			}
			Thread.sleep(10);
		}
		throw new AssertionError("no " + method + " ?" + queryPart + " request: " + requests);
	}

	private record Run(StorageDriver<DataItem, Operation<DataItem>> driver, EndpointSelectionTestSupport.Results results) {}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private Run driver(final String mode) throws Exception {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
		final int port = server.getAddress().getPort();
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 2);
		config.val("storage-net-timeoutMilliSec", 5_000);
		switch (mode) {
		case "round-robin" -> {
			config.val("storage-net-node-addrs", List.of("127.0.0.1:" + port));
			config.val("storage-net-endpoint-selection", "round-robin");
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		}
		case "per-request-dns" -> {
			// localhost resolves to the loopback address without a DNS query.
			config.val("storage-net-node-addrs", List.of("localhost"));
			config.val("storage-net-node-port", port);
			config.val("storage-net-endpoint-selection", "per-request-dns");
			config.val("storage-net-endpoint-dns-timeoutMilliSec", 1_000);
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1_000);
		}
		default -> config.val("storage-net-node-port", port);
		}
		final var created = new S3StorageDriverExtension().create(
						"target-bucket-set", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), false, 16);
		final var driver = (StorageDriver<DataItem, Operation<DataItem>>) created;
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
		driver.start();
		return new Run(driver, results);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var method = exchange.getRequestMethod();
			final var query = uri.getRawQuery();
			requests.add(new Captured(method, uri.getRawPath(), query));
			if ("POST".equals(method) && query != null && !query.contains("uploads") && completeStatus != 200) {
				exchange.sendResponseHeaders(completeStatus, -1);
				return;
			}
			byte[] body = new byte[0];
			if ("POST".equals(method) && query != null && query.contains("uploads")) {
				body = ("<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>"
								+ "<UploadId>upload-1</UploadId></InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
			} else if ("POST".equals(method)) {
				body = "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>"
								.getBytes(StandardCharsets.UTF_8);
			} else if ("GET".equals(method)) {
				body = new byte[ITEM_SIZE];
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			}
			final var status = "DELETE".equals(method) ? 204 : 200;
			exchange.sendResponseHeaders(status, "HEAD".equals(method) || body.length == 0 ? -1 : body.length);
			if (body.length > 0 && !"HEAD".equals(method)) {
				exchange.getResponseBody().write(body);
			}
		}
	}
}
