package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.nic.Ipv4;
import com.dell.spt.base.nic.Ipv4Prefix;
import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.nic.NicPathCounting;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.nic.NicBinding;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

/**
 * Loopback NICs and an in-memory S3 stub for tests of the real S3 drivers with a NIC set. Four
 * loopback addresses stand in for four NICs: the fixture inventory gives each its own address and
 * port, and the source-address binding replaces the device binding, which loopback cannot show.
 * The stub records each request's source address, keeps the objects written to it and can hold
 * requests before it answers them.
 */
final class NicSetLoopback implements AutoCloseable {

	static final List<String> NICS = List.of("lo-a", "lo-b", "lo-c", "lo-d");
	static final List<String> ADDRESSES = List.of("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5");
	static final String KERNEL_SOURCE = "127.0.0.1";
	/** The fixture resolver answers it with the listeners' address. */
	static final String HOSTNAME = "s3-b.example";
	static final String BUCKET = "/bucket";
	private static final long HOLD_SECONDS = 30;
	private static final Pattern KEY = Pattern.compile("<Key>([^<]*)</Key>");

	private static final NicInventory.Sysctls SYSCTLS = new NicInventory.Sysctls(1, 2, 2);
	private static final NicInventory INVENTORY = new NicInventory(
					List.of(nic(0), nic(1), nic(2), nic(3)),
					List.of(),
					NICS.stream().map(NicSetLoopback::route).toList(),
					SYSCTLS);
	static final NicSetSupport.Environment ENVIRONMENT = new NicSetSupport.Environment(
					() -> INVENTORY,
					host -> HOSTNAME.equals(host) ? Ipv4.parse(KERNEL_SOURCE) : Ipv4.parse(host),
					NicBinding.SOURCE_ADDRESS);

	/** One request as the stub received it; {@code source} and {@code port} name its connection. */
	record Request(int listener, String method, String path, String query, String host, String source, int port, String range) {

		/** A request on an object key in any bucket; the bucket check and listings are on the bucket itself. */
		boolean isObjectRequest() {
			return path.indexOf('/', 1) > 0;
		}
	}

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Request> requests = new CopyOnWriteArrayList<>();
	private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
	/** Each upload's parts by number; parts of one upload arrive on several listeners at once. */
	private final Map<String, Map<Integer, byte[]>> uploads = new ConcurrentHashMap<>();
	private final Map<String, String> tags = new ConcurrentHashMap<>();
	private final AtomicInteger uploadIds = new AtomicInteger();
	private final Map<String, AtomicInteger> active = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> peaks = new ConcurrentHashMap<>();
	private final AtomicInteger objectRequestCount = new AtomicInteger();
	private volatile CountDownLatch gate;
	/** The size the stub gives an object nobody wrote, whose reads return zeros. */
	volatile long absentObjectSize = EndpointSelectionTestSupport.ITEM_SIZE;
	/** Which object requests, counted from 1, the stub answers with 503. */
	volatile IntPredicate failObjectRequest = n -> false;

	/** The storage configuration of a driver with these endpoints and NICs. */
	static Config storage(final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		return root(endpoints, interfaces, concurrency).configVal("storage");
	}

	/** The whole configuration around {@link #storage}, for engine scenarios. */
	static Config root(final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		final Config root = S3StorageDriverTest.baseConfig(false, 4, false, null, KERNEL_SOURCE);
		root.val("storage-net-transport", "epoll");
		root.val("storage-net-interfaces", interfaces);
		root.val("storage-net-node-addrs", endpoints);
		root.val("storage-net-timeoutMilliSec", 2_000);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-driver-limit-concurrency", concurrency);
		root.val("storage-driver-threads", 1);
		return root;
	}

	/** A plain-driver test run: the driver and the results it published. */
	record Run(S3NicSetDriver<DataItem, Operation<DataItem>> driver, EndpointSelectionTestSupport.Results results) {

		Operation<?> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}

		NicSetConnectionPool pool() {
			return driver.connectionPool();
		}
	}

	/** A plain driver on the fixture environment; the caller starts and closes it. */
	static S3NicSetDriver<DataItem, Operation<DataItem>> driver(final Config storage, final boolean verify) throws Exception {
		return S3NicSetDriver.create("nic-set-test", new SeedDataInput(1, EndpointSelectionTestSupport.ITEM_SIZE, 1, true),
						storage, verify, 16, NicSetSettings.fromStorage(storage).orElseThrow(), ENVIRONMENT);
	}

	/** Starts {@code driver} with a result collector. */
	static Run started(final S3NicSetDriver<DataItem, Operation<DataItem>> driver) {
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	/** The driver type of {@link Factory}. */
	static final String DRIVER_TYPE = "s3-nic-loopback";

	/**
	 * The {@code s3} NIC-set drivers on the fixture environment, for engine scenarios, in which the
	 * shipped factory would resolve its NICs on the host. It keeps every driver it creates.
	 */
	static final class Factory
					implements NicSetDriverFactory<DataItem, Operation<DataItem>, S3NicSetDriver<DataItem, Operation<DataItem>>> {

		final List<S3NicSetDriver<DataItem, Operation<DataItem>>> drivers = new CopyOnWriteArrayList<>();

		@Override
		public String id() {
			return DRIVER_TYPE;
		}

		@Override
		public S3NicSetDriver<DataItem, Operation<DataItem>> create(final String stepId, final DataInput dataInput,
						final Config storageConfig, final boolean verifyFlag, final int batchSize)
						throws IllegalConfigurationException, InterruptedException {
			final S3NicSetDriver<DataItem, Operation<DataItem>> driver = S3NicSetDriver.create(stepId, dataInput, storageConfig,
							verifyFlag, batchSize, NicSetSettings.fromStorage(storageConfig).orElseThrow(), ENVIRONMENT);
			drivers.add(driver);
			return driver;
		}

		/** The S3 extension on the class path brings the schema and the defaults. */
		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}
	}

	/** Drives the range driver through its runtime, as the range endpoint-selection tests do. */
	static final class RangeRun implements AutoCloseable {

		final S3RangeNicSetDriver driver;
		final RangeReadRuntime<DataItem> runtime;
		private final RangeReadPolicy policy;
		private final long objectSize;
		private final ScheduledExecutorService retryThread = Executors.newSingleThreadScheduledExecutor();
		private final BlockingQueue<Operation.Status> terminal = new LinkedBlockingQueue<>();
		private final BlockingQueue<RangeReadOperation<DataItem>> results = new LinkedBlockingQueue<>();
		/** Runs before a failed attempt's retry is queued, on the thread that settled the attempt. */
		volatile Runnable beforeRetry = () -> {};
		private boolean closed;

		RangeRun(final Config storage, final RangeReadPolicy policy, final long objectSize) throws Exception {
			this(storage, policy, objectSize, false);
		}

		/** With {@code retry}, a failed attempt is retried once, as the range step's retry limit allows. */
		@SuppressWarnings("unchecked")
		RangeRun(final Config storage, final RangeReadPolicy policy, final long objectSize, final boolean retry) throws Exception {
			this.policy = policy;
			this.objectSize = objectSize;
			driver = S3RangeNicSetDriver.create("nic-set-range", new SeedDataInput(1, EndpointSelectionTestSupport.ITEM_SIZE, 1, true),
							storage, 4, policy, NicSetSettings.fromStorage(storage).orElseThrow(), ENVIRONMENT);
			runtime = driver.rangeReadRuntime();
			final LoadGenerator<DataItem, RangeReadOperation<DataItem>> generator = mock(LoadGenerator.class);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				final RangeReadOperation<DataItem> op = call.getArgument(0);
				final RangeReadAttempt attempt = call.getArgument(1);
				beforeRetry.run();
				return op.circulation().claimRetryQueue(attempt) && runtime.admission().put(op);
			});
			driver.operationResultOutput(runtime.bind(generator, driver.operationLifecycle(), retry, 1,
							(delay, task) -> retryThread.schedule(task, delay, TimeUnit.MILLISECONDS),
							op -> terminal.add(op.status()), results::add));
			driver.start();
		}

		RangeReadOperation<DataItem> read(final String name) {
			final var op = new RangeReadOperation<DataItem>(0, new DataItemImpl(name, 0, objectSize), BUCKET, BUCKET, null, policy);
			assertTrue(runtime.tracker().generatorBuffered(op));
			assertTrue(runtime.admission().put(op));
			return op;
		}

		RangeReadOperation<DataItem> outcome() throws InterruptedException {
			final var status = terminal.poll(EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(status, () -> "No terminal result: " + runtime.snapshot());
			final var result = results.poll(EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "Every determinate outcome reaches the trace callback");
			assertEquals(status, result.status());
			return result;
		}

		/** The next terminal status within {@code millis}, or null. */
		Operation.Status pollTerminal(final long millis) throws InterruptedException {
			return terminal.poll(millis, TimeUnit.MILLISECONDS);
		}

		NicSetConnectionPool pool() {
			return driver.connectionPool();
		}

		@Override
		public void close() throws Exception {
			if (closed) {
				return;
			}
			closed = true;
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
			}
		}
	}

	/** The first {@code count} NICs. */
	static List<String> nics(final int count) {
		return NICS.subList(0, count);
	}

	/** The source address of NIC {@code name}. */
	static String address(final String name) {
		return ADDRESSES.get(NICS.indexOf(name));
	}

	/** Starts {@code count} listeners and returns their endpoints, in order. */
	List<String> listeners(final int count) throws IOException {
		final List<String> endpoints = new ArrayList<>();
		for (var i = 0; i < count; i++) {
			final var index = servers.size();
			final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			server.createContext("/", exchange -> handle(index, exchange));
			server.setExecutor(serverThreads);
			server.start();
			servers.add(server);
			endpoints.add(KERNEL_SOURCE + ":" + server.getAddress().getPort());
		}
		return endpoints;
	}

	/** Stops listener {@code index}; its connections close. */
	void stop(final int index) {
		servers.get(index).stop(0);
	}

	List<Request> requests() {
		return List.copyOf(requests);
	}

	/** The requests on object keys; the bucket check and listings are on the bucket itself. */
	List<Request> objectRequests() {
		return requests.stream().filter(Request::isObjectRequest).toList();
	}

	/** The stored object itself, not a copy, so a test can corrupt it in place. */
	byte[] object(final String path) {
		return objects.get(path);
	}

	/** The tagging document last written to {@code path}. */
	String tags(final String path) {
		return tags.get(path);
	}

	/**
	 * From now on, every object request waits after it is recorded until {@link #release()}. Bucket
	 * requests, the first-use check among them, are answered at once.
	 */
	void hold() {
		gate = new CountDownLatch(1);
	}

	/** Answers the held requests, and every later one at once. */
	void release() {
		final var held = gate;
		gate = null;
		if (held != null) {
			held.countDown();
		}
	}

	/** The most requests from {@code source} the stub was answering at once. */
	int peakConcurrency(final String source) {
		final var peak = peaks.get(source);
		return peak == null ? 0 : peak.get();
	}

	@Override
	public void close() {
		release();
		servers.forEach(server -> server.stop(0));
		serverThreads.shutdownNow();
	}

	private void handle(final int listener, final HttpExchange exchange) throws IOException {
		try (exchange) {
			final var body = exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var remote = exchange.getRemoteAddress();
			final var source = remote.getAddress().getHostAddress();
			final var request = new Request(listener, exchange.getRequestMethod(), uri.getRawPath(), uri.getRawQuery(),
							exchange.getRequestHeaders().getFirst("Host"), source, remote.getPort(),
							exchange.getRequestHeaders().getFirst("Range"));
			final var concurrent = active.computeIfAbsent(source, unused -> new AtomicInteger()).incrementAndGet();
			peaks.computeIfAbsent(source, unused -> new AtomicInteger()).accumulateAndGet(concurrent, Math::max);
			try {
				requests.add(request);
				if (request.isObjectRequest() && failObjectRequest.test(objectRequestCount.incrementAndGet())) {
					final var error = "error".getBytes(StandardCharsets.US_ASCII);
					exchange.sendResponseHeaders(503, error.length);
					exchange.getResponseBody().write(error);
					return;
				}
				final var held = gate;
				if (held != null && request.isObjectRequest() && !held.await(HOLD_SECONDS, TimeUnit.SECONDS)) {
					exchange.sendResponseHeaders(503, -1);
					return;
				}
				answer(request, body, exchange);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				active.get(source).decrementAndGet();
			}
		}
	}

	private void answer(final Request request, final byte[] body, final HttpExchange exchange) throws IOException {
		final var query = request.query() == null ? "" : request.query();
		final var path = request.path();
		switch (request.method()) {
		case "PUT" -> {
			if ("tagging".equals(query)) {
				tags.put(path, new String(body, StandardCharsets.UTF_8));
			} else if (query.contains("partNumber=")) {
				uploads.computeIfAbsent(param(query, "uploadId"), unused -> new ConcurrentSkipListMap<>())
								.put(Integer.parseInt(param(query, "partNumber")), body);
			} else {
				objects.put(path, body);
			}
			exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			exchange.sendResponseHeaders(200, -1);
		}
		case "POST" -> {
			if ("delete".equals(query)) {
				respond(exchange, 200, deleteObjects(path, new String(body, StandardCharsets.UTF_8)));
			} else if (query.contains("uploads")) {
				final var uploadId = "upload-" + uploadIds.incrementAndGet();
				uploads.put(uploadId, new ConcurrentSkipListMap<>());
				respond(exchange, 200, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key><UploadId>"
								+ uploadId + "</UploadId></InitiateMultipartUploadResult>");
			} else {
				final var parts = uploads.remove(param(query, "uploadId"));
				final var whole = new ByteArrayOutputStream();
				parts.values().forEach(whole::writeBytes);
				objects.put(path, whole.toByteArray());
				respond(exchange, 200, "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>");
			}
		}
		case "DELETE" -> {
			if (query.contains("uploadId=")) {
				uploads.remove(param(query, "uploadId"));
			} else {
				objects.remove(path);
			}
			exchange.sendResponseHeaders(204, -1);
		}
		case "HEAD" -> {
			final var object = objects.get(path);
			if (request.isObjectRequest() && object == null) {
				exchange.sendResponseHeaders(404, -1);
			} else {
				exchange.getResponseHeaders().set("Content-Length", String.valueOf(object == null ? 0 : object.length));
				exchange.sendResponseHeaders(200, -1);
			}
		}
		case "GET" -> {
			if (!request.isObjectRequest()) {
				respond(exchange, 200, listing(path));
				return;
			}
			final var object = objects.get(path);
			final var size = object == null ? absentObjectSize : object.length;
			if (request.range() != null) {
				final var bounds = request.range().substring("bytes=".length()).split("-", -1);
				final var first = Integer.parseInt(bounds[0]);
				final var last = Integer.parseInt(bounds[1]);
				exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + size);
				exchange.sendResponseHeaders(206, last - first + 1);
				exchange.getResponseBody().write(object == null ? new byte[last - first + 1] : Arrays.copyOfRange(object, first, last + 1));
				return;
			}
			final var content = object == null ? new byte[(int) size] : object;
			exchange.sendResponseHeaders(200, content.length == 0 ? -1 : content.length);
			exchange.getResponseBody().write(content);
		}
		default -> exchange.sendResponseHeaders(405, -1);
		}
	}

	/** Answers a multi-object delete: removes each listed key and reports it deleted. */
	private String deleteObjects(final String bucket, final String body) {
		final var xml = new StringBuilder("<DeleteResult>");
		final var keys = KEY.matcher(body);
		while (keys.find()) {
			objects.remove(bucket + "/" + keys.group(1));
			xml.append("<Deleted><Key>").append(keys.group(1)).append("</Key></Deleted>");
		}
		return xml.append("</DeleteResult>").toString();
	}

	private String listing(final String bucket) {
		final var xml = new StringBuilder("<ListBucketResult><IsTruncated>false</IsTruncated>");
		objects.forEach((path, content) -> {
			if (path.startsWith(bucket + "/")) {
				xml.append("<Contents><Key>").append(path.substring(bucket.length() + 1)).append("</Key><Size>")
								.append(content.length).append("</Size></Contents>");
			}
		});
		return xml.append("</ListBucketResult>").toString();
	}

	private static void respond(final HttpExchange exchange, final int status, final String xml) throws IOException {
		final var bytes = xml.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
	}

	private static String param(final String query, final String name) {
		for (final var pair : query.split("&", -1)) {
			if (pair.startsWith(name + "=")) {
				return pair.substring(name.length() + 1);
			}
		}
		throw new IllegalArgumentException("no " + name + " in " + query);
	}

	static int port(final String endpoint) {
		return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
	}

	static int unusedPort() throws IOException {
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	/** A stopped listener's port can accept briefly after the close; wait until it refuses. */
	static void awaitRefused(final String endpoint) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (System.nanoTime() < deadline) {
			try (final var socket = new Socket()) {
				socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port(endpoint)), 1_000);
			} catch (final ConnectException e) {
				return;
			} catch (final IOException e) {
				// retry
			}
			Thread.sleep(10);
		}
		fail(endpoint + " still accepts connections");
	}

	static void waitFor(final BooleanSupplier condition, final String what) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), what);
	}

	static List<String> nicIoThreads(final String nic) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.map(Thread::getName)
						.filter(name -> name.startsWith("ioWorker-" + nic))
						.toList();
	}

	static void awaitNoNicIoThreads() throws InterruptedException {
		waitFor(() -> NICS.stream().allMatch(nic -> nicIoThreads(nic).isEmpty()),
						"the NICs' event loops stopped: " + NICS.stream().flatMap(nic -> nicIoThreads(nic).stream()).toList());
	}

	/** The terminal rows {@code driver} gives once {@code stop} has closed it. */
	static NicPathRows stoppedRows(final NicPathCounting driver, final AutoCloseable stop) throws Exception {
		stop.close();
		final var rows = driver.nicPathRows();
		assertNotNull(rows, "a stopped NIC-set driver gives its rows");
		assertTrue(rows.complete(), "every update had stopped");
		return rows;
	}

	/** The sums over {@code rows}: attempts, succeeded, failed, interrupted, unresolved and bytes. */
	static List<Long> totals(final NicPathRows rows) {
		final var sums = new long[6];
		for (final var row : rows.rows()) {
			sums[0] += row.attempts();
			sums[1] += row.succeeded();
			sums[2] += row.failed();
			sums[3] += row.interrupted();
			sums[4] += row.unresolved();
			sums[5] += row.bytes();
		}
		return Arrays.stream(sums).boxed().toList();
	}

	/** The rows of kind {@code kind}. */
	static List<NicPathRows.Row> rowsOf(final NicPathRows rows, final NicPathRows.Kind kind) {
		return rows.rows().stream().filter(row -> row.kind() == kind).toList();
	}

	private static NicInventory.Nic nic(final int index) {
		return new NicInventory.Nic(NICS.get(index), NicInventory.Kind.PHYSICAL, null, true,
						List.of(Ipv4Prefix.parse(ADDRESSES.get(index) + "/8")), 25_000, 1_500, 0, "0000:01:00." + index, List.of(),
						null, null, null, List.of(), SYSCTLS);
	}

	private static NicInventory.Route route(final String nic) {
		return new NicInventory.Route(nic, Ipv4.parse("127.0.0.0"), Ipv4.mask(8), 0, false);
	}
}
