package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.metrics.range.RangeReadSnapshot;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

class S3RangeStorageDriverTest {
	private record Request(String method, String path, String range, String authorization) {}

	private record Response(int status, String contentRange, String body) {
		static Response disconnect() {
			return new Response(0, null, "");
		}
	}

	private static final class Fixture implements AutoCloseable {
		final HttpServer server;
		final ExecutorService serverThreads = Executors.newFixedThreadPool(2);
		final ScheduledExecutorService retryThread = Executors.newSingleThreadScheduledExecutor();
		final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
		final BlockingQueue<Operation.Status> terminal = new LinkedBlockingQueue<>();
		final BlockingQueue<RangeReadOperation<DataItem>> results = new LinkedBlockingQueue<>();
		final AtomicInteger requestCount = new AtomicInteger();
		final S3RangeStorageDriver driver;
		final RangeReadRuntime<DataItem> runtime;
		final RangeReadPolicy policy;

		@SuppressWarnings("unchecked")
		Fixture(RangeReadPolicy policy, boolean retry, IntFunction<Response> responses) throws Exception {
			this.policy = policy;
			server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
			server.setExecutor(serverThreads);
			server.createContext("/", exchange -> {
				try (exchange) {
					String range = exchange.getRequestHeaders().getFirst("Range");
					requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(), range,
									exchange.getRequestHeaders().getFirst("Authorization")));
					Response response = responses.apply(requestCount.incrementAndGet());
					// Close the accepted connection before any response headers reach the driver.
					if (response.status() == 0)
						return;
					if (response.contentRange() != null)
						exchange.getResponseHeaders().set("Content-Range", response.contentRange().startsWith("echo")
										? "bytes " + range.substring(6) + "/" + (response.contentRange().equals("echo")
														? "*"
														: response.contentRange().substring(5))
										: response.contentRange());
					byte[] body = response.body().getBytes(StandardCharsets.US_ASCII);
					exchange.sendResponseHeaders(response.status(), body.length);
					exchange.getResponseBody().write(body);
				}
			});
			server.start();
			var config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
			config.val("storage-net-node-port", server.getAddress().getPort());
			config.val("storage-net-timeoutMilliSec", 2000);
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-threads", 1);
			config.val("storage-net-http-headers", Map.of());
			driver = (S3RangeStorageDriver) new S3StorageDriverExtension<DataItem, RangeReadOperation<DataItem>, S3StorageDriver<DataItem, RangeReadOperation<DataItem>>>()
							.createRangeRead("range-loopback", new com.dell.spt.base.data.SeedDataInput(1, 1024, 1, true), config.configVal("storage"), 4, policy);
			runtime = driver.rangeReadRuntime();
			LoadGenerator<DataItem, RangeReadOperation<DataItem>> generator = mock(LoadGenerator.class);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				RangeReadOperation<DataItem> op = call.getArgument(0);
				RangeReadAttempt attempt = call.getArgument(1);
				return op.circulation().claimRetryQueue(attempt) && runtime.admission().put(op);
			});
			driver.operationResultOutput(runtime.bind(generator, driver.operationLifecycle(), retry, 1,
							(delay, task) -> retryThread.schedule(task, delay, TimeUnit.MILLISECONDS),
							op -> terminal.add(op.status()), results::add));
			driver.start();
		}

		RangeReadOperation<DataItem> read(String name, long size) {
			var op = new RangeReadOperation<DataItem>(0, new DataItemImpl(name, 0, size), "/bucket", "/bucket", null, policy);
			assertTrue(runtime.tracker().generatorBuffered(op));
			assertTrue(runtime.admission().put(op));
			return op;
		}

		Operation.Status outcome() throws Exception {
			var status = terminal.poll(5, TimeUnit.SECONDS);
			assertNotNull(status, () -> "No terminal result: " + runtime.snapshot());
			if (status == Operation.Status.SUCC)
				assertNotNull(results.poll(5, TimeUnit.SECONDS));
			assertEquals(0, driver.activeRangeTransports());
			return status;
		}

		RangeReadSnapshot snapshot() {
			return runtime.snapshot();
		}

		public void close() throws Exception {
			try {
				runtime.closeAdmission();
				driver.closeAdmission();
				driver.close();
				runtime.close();
				assertEquals(0, driver.activeRangeTransports());
				assertNull(driver.terminalFailure());
				assertNull(runtime.failure());
			} finally {
				retryThread.shutdownNow();
				server.stop(0);
				serverThreads.shutdownNow();
				assertTrue(retryThread.awaitTermination(5, TimeUnit.SECONDS));
				assertTrue(serverThreads.awaitTermination(5, TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void exactSignedGetPreservesWholeObjectMetadataAndSendsFixedOutOfBoundsUnchanged() throws Exception {
		var policy = new RangeReadPolicy(3, 2L, 1);
		try (var f = new Fixture(policy, false, n -> n == 1
						? new Response(206, "bytes 2-4/8", "abc")
						: new Response(416, null, "error"))) {
			var first = f.read("key~literal", 10);
			assertEquals(Operation.Status.SUCC, f.outcome());
			assertEquals(10, first.item().size());
			assertEquals("/bucket/key~literal", first.item().name());
			assertEquals(3, first.countBytesDone());
			f.read("small", 1);
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, f.outcome());
			assertEquals(2, f.requests.size());
			for (Request request : f.requests) {
				assertEquals("GET", request.method());
				assertEquals("bytes=2-4", request.range());
				assertTrue(request.authorization().startsWith("AWS4-HMAC-SHA256 "));
			}
			assertEquals("/bucket/key~literal", f.requests.peek().path());
			assertEquals(2, f.snapshot().requestsSent());
			assertEquals(1, f.snapshot().logical().accepted());
			assertEquals(1, f.snapshot().logical().failed());
			assertTrue(f.snapshot().logical().reconciled());
		}
	}

	@Test
	void mutableObjectsUseResponseStructureWithoutRefreshingInventoryOrReselecting() throws Exception {
		record Mutation(String name, Response response, Operation.Status expected) {}
		var mutations = new Mutation[]{
				new Mutation("growth", new Response(206, "echo/16", "abc"), Operation.Status.SUCC),
				new Mutation("shrink-with-valid-span", new Response(206, "echo/5", "abc"), Operation.Status.SUCC),
				new Mutation("shrink-out-of-bounds", new Response(416, null, "error"), Operation.Status.RESP_FAIL_CLIENT),
				new Mutation("deletion", new Response(404, null, "missing"), Operation.Status.RESP_FAIL_NOT_FOUND),
				new Mutation("same-size-replacement", new Response(206, "echo/8", "xyz"), Operation.Status.SUCC)
		};
		// Alignment leaves one legal random selection, making the wire assertion deterministic.
		for (var policy : new RangeReadPolicy[]{new RangeReadPolicy(3, 2L, 1), new RangeReadPolicy(3, null, 8)}) {
			for (var mutation : mutations) {
				try (var f = new Fixture(policy, false,
								n -> n == 1 ? mutation.response() : new Response(206, "echo/8", "abc"))) {
					var op = f.read(mutation.name(), 8);
					assertEquals(mutation.expected(), f.outcome(), mutation.name());
					assertEquals(8, op.item().size(), "Response size must not replace inventory size");
					assertEquals(1, f.requestCount.get(), "No metadata probe or fallback GET");
					var request = f.requests.poll();
					assertNotNull(request);
					assertEquals("GET", request.method());
					assertEquals("/bucket/" + mutation.name(), request.path());
					assertEquals(policy.equals(new RangeReadPolicy(3, 2L, 1)) ? "bytes=2-4" : "bytes=0-2", request.range());
					boolean success = mutation.expected() == Operation.Status.SUCC;
					assertEquals(success ? 3 : 0, f.snapshot().successfulBytes());
					assertEquals(success ? 0 : 1, f.snapshot().httpFailures());
					assertEquals(0, f.snapshot().responseValidationFailures());
					f.read("after-mutation", 8);
					assertEquals(Operation.Status.SUCC, f.outcome());
					assertEquals(2, f.requestCount.get());
					assertEquals(2, f.snapshot().requestsSent());
					assertEquals(success ? 2 : 1, f.snapshot().logical().accepted());
					assertEquals(success ? 0 : 1, f.snapshot().logical().failed());
					assertTrue(f.snapshot().reconciled());
				}
			}
		}
	}

	@Test
	void randomRetryRetainsRangeAndOneLogicalOutcome() throws Exception {
		try (var f = new Fixture(new RangeReadPolicy(3, null, 1), true,
						n -> n == 1 ? new Response(503, null, "error") : new Response(206, "echo", "abc"))) {
			f.read("retry", 10);
			assertEquals(Operation.Status.SUCC, f.outcome());
			var first = f.requests.poll();
			var retry = f.requests.poll();
			assertEquals(first.range(), retry.range());
			assertEquals(2, f.snapshot().requestsSent());
			assertEquals(1, f.snapshot().httpAttemptFailures());
			assertEquals(0, f.snapshot().logical().failed());
			assertEquals(1, f.snapshot().logical().accepted());
			assertEquals(3, f.snapshot().successfulBytes());
			assertTrue(f.snapshot().logical().reconciled());
		}
	}

	@Test
	void localSelectionFailureSendsNoRequest() throws Exception {
		try (var f = new Fixture(new RangeReadPolicy(3, null, 1), false,
						n -> new Response(500, null, "unexpected"))) {
			f.read("too-small", 1);
			assertNotEquals(Operation.Status.SUCC, f.outcome());
			assertEquals(0, f.requestCount.get());
			assertEquals(0, f.snapshot().requestsSent());
			assertEquals(1, f.snapshot().localSelectionErrors());
			assertTrue(f.snapshot().logical().reconciled());
		}
	}

	@Test
	void rejectedResponseReleasesTransportForNextSuccessfulOperation() throws Exception {
		try (var f = new Fixture(new RangeReadPolicy(3, 2L, 1), false,
						n -> n == 1 ? new Response(200, null, "abc") : new Response(206, "echo", "abc"))) {
			f.read("ignored-range", 10);
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, f.outcome());
			f.read("next", 10);
			assertEquals(Operation.Status.SUCC, f.outcome());
			assertEquals(1, f.snapshot().responseValidationFailures());
			assertEquals(1, f.snapshot().logical().accepted());
			assertEquals(2, f.snapshot().requestsSent());
			assertTrue(f.snapshot().logical().reconciled());
		}
	}

	@Test
	void connectionLossReleasesTransportAndRetryRetainsSelectedRange() throws Exception {
		for (boolean retry : new boolean[]{false, true
		}) {
			try (var f = new Fixture(new RangeReadPolicy(3, null, 1), retry,
							n -> n == 1 ? Response.disconnect() : new Response(206, "echo/10", "abc"))) {
				var op = f.read("connection-loss", 10);
				assertEquals(retry ? Operation.Status.SUCC : Operation.Status.FAIL_IO, f.outcome());
				assertEquals(10, op.item().size());
				var first = f.requests.poll();
				assertNotNull(first);
				if (retry) {
					var second = f.requests.poll();
					assertNotNull(second);
					assertEquals(first.method(), second.method());
					assertEquals(first.path(), second.path());
					assertEquals(first.range(), second.range(), "Retry must retain the selected range");
					assertTrue(second.authorization().startsWith("AWS4-HMAC-SHA256 "));
				}
				f.read("after-connection-loss", 10);
				assertEquals(Operation.Status.SUCC, f.outcome());
				assertEquals(retry ? 3 : 2, f.requestCount.get());
				assertEquals(retry ? 3 : 2, f.snapshot().requestsSent());
				assertEquals(1, f.snapshot().transportAttemptFailures());
				assertEquals(retry ? 0 : 1, f.snapshot().transportFailures());
				assertEquals(retry ? 0 : 1, f.snapshot().logical().failed());
				assertEquals(retry ? 2 : 1, f.snapshot().logical().accepted());
				assertEquals(retry ? 6 : 3, f.snapshot().successfulBytes());
				assertEquals(0, f.snapshot().responseValidationFailures());
				assertEquals(0, f.snapshot().httpFailures());
				assertTrue(f.snapshot().reconciled());
			}
		}
	}

	@Test
	void timeoutReleasesPermitAndAllowsNextRead() throws Exception {
		var release = new CountDownLatch(1);
		try (var f = new Fixture(new RangeReadPolicy(3, 2L, 1), false, n -> {
			if (n == 1) {
				try {
					release.await(4, TimeUnit.SECONDS);
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
				}
			}
			return new Response(206, "echo", "abc");
		})) {
			try {
				f.read("timeout", 10);
				assertEquals(Operation.Status.FAIL_TIMEOUT, f.outcome());
				f.read("next", 10);
				assertEquals(Operation.Status.SUCC, f.outcome());
				assertEquals(1, f.snapshot().transportFailures());
				assertTrue(f.snapshot().logical().reconciled());
			} finally {
				release.countDown();
			}
		}
	}

	@Test
	void drainFencesQueuedWorkAndRetainsUnresolvedDispatchedAttempt() throws Exception {
		var release = new CountDownLatch(1);
		try (var f = new Fixture(new RangeReadPolicy(3, 2L, 1), false, n -> {
			try {
				release.await(4, TimeUnit.SECONDS);
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			}
			return new Response(206, "echo", "abc");
		})) {
			try {
				f.read("dispatched", 10);
				assertNotNull(f.requests.poll(3, TimeUnit.SECONDS));
				f.read("queued", 10);
				f.runtime.closeAdmission();
				f.driver.closeAdmission();
				f.driver.recoverQueuedOperations();
				f.runtime.tracker().enforceTerminalDeadline(System.nanoTime());
				f.runtime.tracker().resolveOutstandingAsUnresolved();
				assertEquals(1, f.snapshot().logical().unattempted());
				assertEquals(1, f.snapshot().logical().unresolved());
				assertEquals(1, f.snapshot().requestsSent());
				assertTrue(f.snapshot().logical().reconciled());
			} finally {
				release.countDown();
			}
		}
	}

	@Test
	void ordinaryFactoryRemainsOrdinaryAndInvalidRangeOptionsFailBeforeResources() throws Exception {
		var config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1").configVal("storage");
		var factory = new S3StorageDriverExtension<DataItem, RangeReadOperation<DataItem>, S3StorageDriver<DataItem, RangeReadOperation<DataItem>>>();
		var input = new com.dell.spt.base.data.SeedDataInput(1, 1024, 1, true);
		try (var ordinary = factory.create("ordinary-factory", input, config, false, 4)) {
			assertFalse(ordinary instanceof com.dell.spt.base.storage.driver.range.RangeReadDriverSupport);
		}
		var policy = new RangeReadPolicy(3, 2L, 1);
		assertThrows(com.dell.spt.base.config.IllegalConfigurationException.class,
						() -> factory.createRangeRead("invalid-timeout", null, config, 4, policy));
		config.val("net-timeoutMilliSec", 1000);
		config.val("net-http-read-metadata-only", true);
		assertThrows(com.dell.spt.base.config.IllegalConfigurationException.class,
						() -> factory.createRangeRead("metadata-only", null, config, 4, policy));
		config.val("net-http-read-metadata-only", false);
		config.val("object-tagging-enabled", true);
		assertThrows(com.dell.spt.base.config.IllegalConfigurationException.class,
						() -> factory.createRangeRead("tagging", null, config, 4, policy));
	}

}
