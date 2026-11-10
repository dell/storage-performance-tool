package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.nic.Ipv4;
import com.dell.spt.base.nic.Ipv4Prefix;
import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.storage.driver.coop.netty.nic.NicBinding;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * Loopback NICs and an RDMA-capable S3 stub for tests of the real S3-RDMA driver with a NIC set. Four
 * loopback addresses stand in for four NICs, each with its own {@code mlx5} device and a RoCE v2 GID
 * in the fixture inventory, and each device's fake transport has its own DC target number. The stub
 * emulates the one-sided transfer through the fake that the token names, and records each request's
 * source address and the NIC its token names.
 */
final class RdmaNicLoopback implements AutoCloseable {

	static final List<String> NICS = List.of("lo-a", "lo-b", "lo-c", "lo-d");
	static final List<String> ADDRESSES = List.of("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5");
	static final List<String> DEVICES = List.of("mlx5_a", "mlx5_b", "mlx5_c", "mlx5_d");
	static final String KERNEL_SOURCE = "127.0.0.1";
	static final String BUCKET = "/bucket";
	static final long THRESHOLD = 1024;
	static final int SIZE = 4096;
	static final String DRIVER_TYPE = "s3-rdma-nic-loopback";
	static final Credential CREDENTIAL = Credential.getInstance("access", "secret");
	static final byte SERVER_GET_FILL = 0x5a;
	/** Byte offset of a device's DC target number from its index. */
	private static final int DCTN_BASE = 0x100;
	private static final long HOLD_SECONDS = 30;

	/** Leaves pool placement out unless a test asks for it, whatever this host has. */
	private static final Path NO_NUMA_NODES = Path.of("no-numa-nodes-in-this-fixture");
	private static final NicInventory.Sysctls SYSCTLS = new NicInventory.Sysctls(1, 2, 2);
	private static final NicInventory INVENTORY = new NicInventory(
					IntStream.range(0, NICS.size()).mapToObj(RdmaNicLoopback::nic).toList(),
					DEVICES.stream().map(RdmaNicLoopback::device).toList(),
					NICS.stream().map(RdmaNicLoopback::route).toList(),
					SYSCTLS);

	/** One request as the stub received it; {@code tokenNic} is -1 without a token. */
	record Request(int listener, String method, String path, String query, String host, String source, int port,
					String range, String token, int tokenNic, int bodyLength) {

		/** A request on an object key; the bucket check is on the bucket itself. */
		boolean isObjectRequest() {
			return path.indexOf('/', 1) > 0;
		}

		/** The index of the NIC whose address sent it, or -1. */
		int sourceNic() {
			return ADDRESSES.indexOf(source);
		}
	}

	private final List<HttpServer> servers = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final List<Request> requests = new CopyOnWriteArrayList<>();
	private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
	private final Map<String, Map<Integer, byte[]>> uploads = new ConcurrentHashMap<>();
	private final AtomicInteger uploadIds = new AtomicInteger();
	private final Map<String, AtomicInteger> active = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> peaks = new ConcurrentHashMap<>();
	private final AtomicInteger objectRequestCount = new AtomicInteger();
	/** Every transport this fixture made, in the order the driver asked for them. */
	final List<NicTransport> transports = new CopyOnWriteArrayList<>();
	private final AtomicInteger opensInFlight = new AtomicInteger();
	/** Whether two contexts ever opened at once. */
	volatile boolean opensOverlapped;
	/** Which object requests, counted from 1, the stub answers with 503. */
	volatile IntPredicate failObjectRequest = n -> false;
	/** Whether the stub answers every part upload with 503, over HTTP or RDMA. */
	volatile boolean failParts;
	/** Which requests with a token the stub declines (reply 501): a GET body then comes over HTTP. */
	volatile Predicate<Request> decline = request -> false;
	/** The object requests that wait for {@link #release()}. */
	private volatile Predicate<Request> held = request -> false;
	private volatile CountDownLatch gate = new CountDownLatch(0);
	/** Answered on object responses, such as integrity metadata. */
	volatile Map<String, String> objectResponseHeaders = Map.of();
	/** The device whose open fails. */
	volatile String failOpen;
	/** Runs before each registration, on every transport: a test's way into a pre-warm. */
	volatile Runnable beforeRegistration = () -> {};
	/** Runs as each connection binds, to fail a connect. */
	volatile Runnable beforeConnect = () -> {};
	/** Makes the stub close each connection after its response, so the next request connects. */
	volatile boolean closeConnections;
	/** Which listeners drop every object request without answering: the connection closes. */
	volatile IntPredicate dropOnListener = listener -> false;
	private final NicSetSupport.Environment environment = new NicSetSupport.Environment(
					() -> INVENTORY, host -> Ipv4.parse(host), (connecting, iface, address) -> {
						beforeConnect.run();
						NicBinding.SOURCE_ADDRESS.apply(connecting, iface, address);
					});

	/** A device's fake: its token names its DC target; the fixture finds it by that number. */
	final class NicTransport extends FakeRdmaTransport {

		NicTransport(final RdmaConfig config) {
			super(config);
			setBeforeRegister(() -> beforeRegistration.run());
		}

		@Override
		public void initStrict(final String device, final String localIp) {
			if (opensInFlight.incrementAndGet() > 1) {
				opensOverlapped = true;
			}
			try {
				setDctn(DCTN_BASE + DEVICES.indexOf(device));
				setFailInit(device.equals(failOpen));
				super.initStrict(device, localIp);
			} finally {
				opensInFlight.decrementAndGet();
			}
		}

		int nicIndex() {
			return DEVICES.indexOf(getStrictDevice());
		}
	}

	/** The whole configuration of a driver with these endpoints and NICs. */
	static Config root(final RdmaConfig rdma, final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		final Config root = S3RdmaStorageDriverTestSupport.config(rdma, BUCKET, endpoints, 9020);
		root.val("storage-net-transport", "epoll");
		root.val("storage-net-interfaces", interfaces);
		root.val("storage-net-http-headers", Map.of());
		root.val("storage-net-timeoutMilliSec", 10_000);
		root.val("storage-driver-limit-concurrency", concurrency);
		root.val("storage-driver-threads", 1);
		root.val("storage-driver-limit-queue-input", 1_000);
		root.val("load-batch-size", 16);
		return root;
	}

	static RdmaConfig rdma(final boolean fallback, final boolean pool, final int copyThreads) {
		return rdma(fallback, pool, copyThreads, 30_000);
	}

	static RdmaConfig rdma(final boolean fallback, final boolean pool, final int copyThreads, final long timeoutMs) {
		return new RdmaConfig(true, THRESHOLD, fallback, "auto", "", "WARN", timeoutMs, false, pool, copyThreads);
	}

	static DataInput dataInput() {
		try {
			return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/** A driver on the fixture environment and this fixture's transports; the caller starts and closes it. */
	S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver(final Config root) throws Exception {
		final Config storage = root.configVal("storage");
		final S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver = S3RdmaNicSetDriver.create("rdma-nic-test", dataInput(), storage, false,
						root.intVal("load-batch-size"), NicSetSettings.fromStorage(storage).orElseThrow(), environment, recording());
		driver.numaNodes = NO_NUMA_NODES;
		return driver;
	}

	/** Starts {@code driver} with a result collector. */
	static Run started(final S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver) {
		final var results = new Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	/** A driver test run: the driver and the results it published. */
	record Run(S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver, Results results) {

		Operation<DataItem> execute(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			return results.await();
		}

		/** Runs the operation and waits for its attempt to end, for tests of the NIC rotation. */
		Operation<DataItem> executeAlone(final Operation<DataItem> op) throws Exception {
			final var result = execute(op);
			awaitIdle();
			return result;
		}

		void awaitIdle() throws InterruptedException {
			final var admission = pool().admission();
			waitFor(() -> driver.activeOpCount() == 0
							&& IntStream.range(0, admission.size()).allMatch(nic -> admission.count(nic) == 0), "every attempt ended");
		}

		NicSetConnectionPool pool() {
			return driver.connectionPool();
		}

		RdmaNic nic(final int index) {
			return driver.openRdmaResources().nics().get(index);
		}

		RdmaPreparationLane lane(final int index) {
			return driver.openRdmaResources().lanes().get(index);
		}
	}

	/** The fixture's driver factory, for engine scenarios. */
	final class Factory implements NicSetDriverFactory<DataItem, Operation<DataItem>, S3RdmaNicSetDriver<DataItem, Operation<DataItem>>> {

		final List<S3RdmaNicSetDriver<DataItem, Operation<DataItem>>> drivers = new CopyOnWriteArrayList<>();
		/** The direct-memory limit the drivers see; a default well above any fixture's need. */
		volatile long directMemoryLimitBytes = 64L << 30;

		@Override
		public String id() {
			return DRIVER_TYPE;
		}

		@Override
		public S3RdmaNicSetDriver<DataItem, Operation<DataItem>> create(final String stepId, final DataInput dataInput,
						final Config storageConfig, final boolean verifyFlag, final int batchSize)
						throws IllegalConfigurationException, InterruptedException {
			final S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver = S3RdmaNicSetDriver.create(stepId, dataInput, storageConfig,
							verifyFlag, batchSize, NicSetSettings.fromStorage(storageConfig).orElseThrow(), environment, recording());
			driver.directMemoryLimitBytes = directMemoryLimitBytes;
			driver.numaNodes = NO_NUMA_NODES;
			drivers.add(driver);
			return driver;
		}

		/** The S3-RDMA extension on the class path brings the schema and the defaults. */
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

	/** Collects published operation results. */
	static final class Results implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<DataItem>> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}

		Operation<DataItem> await() throws InterruptedException {
			final var result = results.poll(15, TimeUnit.SECONDS);
			assertNotNull(result, "no result published");
			return result;
		}

		Operation<DataItem> poll(final long millis) throws InterruptedException {
			return results.poll(millis, TimeUnit.MILLISECONDS);
		}
	}

	@SuppressWarnings("unchecked")
	static Operation<DataItem> op(final OpType type, final String name) {
		return op(type, name, SIZE);
	}

	@SuppressWarnings("unchecked")
	static Operation<DataItem> op(final OpType type, final String name, final long size) {
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(0, type, new DataItemImpl(name, name.hashCode() & 0xFFFF, size),
						null, BUCKET, CREDENTIAL, null, 0);
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

	void stop(final int index) {
		servers.get(index).stop(0);
	}

	/** Waits until {@code endpoint}, a stopped listener's, refuses connections. */
	static void awaitRefused(final String endpoint) throws InterruptedException {
		final int port = Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (System.nanoTime() < deadline) {
			try (final var socket = new Socket()) {
				socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 1_000);
			} catch (final ConnectException e) {
				return;
			} catch (final IOException e) {
				// retry
			}
			Thread.sleep(10);
		}
		throw new AssertionError(endpoint + " still accepts connections");
	}

	/**
	 * Stops listener {@code index} and leaves its port silent: a listening socket whose accept queue
	 * is full, so the kernel drops each new SYN and a connect waits for its timeout. Closing the
	 * result frees the port. As {@code NicSetLoopback.blackhole} in the {@code s3} tests.
	 */
	AutoCloseable blackhole(final int index) throws IOException, InterruptedException {
		final var address = servers.get(index).getAddress();
		servers.get(index).stop(0);
		final var listener = ServerSocketChannel.open();
		final List<SocketChannel> queued = new ArrayList<>();
		final AutoCloseable silence = () -> {
			for (final var channel : queued) {
				channel.close();
			}
			listener.close();
		};
		try {
			listener.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (true) {
				try {
					// A backlog of 1 queues two connections; the kernel drops the SYNs of any more.
					listener.bind(address, 1);
					break;
				} catch (final BindException e) {
					// The stopped server closes its socket on its own thread.
					if (System.nanoTime() > deadline) {
						throw e;
					}
					Thread.sleep(10);
				}
			}
			while (queued.size() < 8) {
				final var channel = SocketChannel.open();
				queued.add(channel);
				channel.configureBlocking(false);
				if (!channel.connect(address) && !finishesWithin(channel, 200)) {
					return silence;
				}
			}
			throw new IOException("the accept queue of " + address + " never filled");
		} catch (final IOException | InterruptedException | RuntimeException e) {
			try {
				silence.close();
			} catch (final Exception closing) {
				e.addSuppressed(closing);
			}
			throw e;
		}
	}

	/** Whether this host drops a SYN that finds the accept queue full, which {@link #blackhole} needs, rather than resetting it. */
	static boolean dropsSynsOnAFullAcceptQueue() {
		try {
			return "0".equals(Files.readString(Path.of("/proc/sys/net/ipv4/tcp_abort_on_overflow")).trim());
		} catch (final IOException e) {
			return false;
		}
	}

	private static boolean finishesWithin(final SocketChannel channel, final long millis)
					throws IOException, InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
		while (System.nanoTime() < deadline) {
			if (channel.finishConnect()) {
				return true;
			}
			Thread.sleep(5);
		}
		return false;
	}

	List<Request> requests() {
		return List.copyOf(requests);
	}

	List<Request> objectRequests() {
		return requests.stream().filter(Request::isObjectRequest).toList();
	}

	/** The multipart uploads started and neither completed nor aborted. */
	int openUploads() {
		return uploads.size();
	}

	byte[] object(final String path) {
		return objects.get(path);
	}

	void putObject(final String path, final byte[] content) {
		objects.put(path, content);
	}

	/** From now on, every object request waits until {@link #release()}. */
	void hold() {
		hold(request -> true);
	}

	/** From now on, the object requests that match wait until {@link #release()}. */
	void hold(final Predicate<Request> which) {
		gate = new CountDownLatch(1);
		held = which;
	}

	void release() {
		held = request -> false;
		gate.countDown();
	}

	/** The most requests from {@code source} the stub was answering at once. */
	int peakConcurrency(final String source) {
		final var peak = peaks.get(source);
		return peak == null ? 0 : peak.get();
	}

	/** The transport of NIC {@code index}, of the driver opened last. */
	NicTransport transport(final int index) {
		for (var i = transports.size() - 1; i >= 0; i--) {
			if (transports.get(i).nicIndex() == index) {
				return transports.get(i);
			}
		}
		throw new AssertionError("no transport of NIC " + index);
	}

	/** Applied to each transport as it is made, for what must hold before a lane starts. */
	java.util.function.Consumer<FakeRdmaTransport> transportSetup = transport -> {};

	Function<RdmaConfig, RdmaTransport> recording() {
		return config -> {
			final var transport = new NicTransport(config);
			transportSetup.accept(transport);
			transports.add(transport);
			return transport;
		};
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
			final var token = exchange.getRequestHeaders().getFirst(S3RdmaStorageDriver.RDMA_TOKEN_HEADER);
			final var request = new Request(listener, exchange.getRequestMethod(), uri.getRawPath(), uri.getRawQuery(),
							exchange.getRequestHeaders().getFirst("Host"), source, remote.getPort(),
							exchange.getRequestHeaders().getFirst("Range"), token, tokenNic(token), body.length);
			if (closeConnections) {
				exchange.getResponseHeaders().set("Connection", "close");
			}
			final var concurrent = active.computeIfAbsent(source, unused -> new AtomicInteger()).incrementAndGet();
			peaks.computeIfAbsent(source, unused -> new AtomicInteger()).accumulateAndGet(concurrent, Math::max);
			try {
				requests.add(request);
				if (request.isObjectRequest() && (failObjectRequest.test(objectRequestCount.incrementAndGet())
								|| (failParts && "PUT".equals(request.method()) && request.query() != null
												&& request.query().contains("partNumber=")))) {
					exchange.sendResponseHeaders(503, -1);
					return;
				}
				if (request.isObjectRequest() && dropOnListener.test(listener)) {
					exchange.close();
					return;
				}
				final var currentGate = gate;
				if (request.isObjectRequest() && held.test(request) && !currentGate.await(HOLD_SECONDS, TimeUnit.SECONDS)) {
					exchange.sendResponseHeaders(503, -1);
					return;
				}
				if (token != null) {
					answerRdma(request, exchange);
				} else {
					answer(request, body, exchange);
				}
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				active.get(source).decrementAndGet();
			}
		}
	}

	private int tokenNic(final String token) {
		return token == null ? -1 : Integer.parseInt(token.split(":", -1)[4], 16) - DCTN_BASE;
	}

	/** The one-sided transfer, through the registered buffer the token names on the NIC it names. */
	private void answerRdma(final Request request, final HttpExchange exchange) throws IOException {
		final var query = request.query() == null ? "" : request.query();
		final var path = request.path();
		if (decline.test(request)) {
			exchange.getResponseHeaders().set(RdmaReplyContract.REPLY_HEADER, "501");
			if ("GET".equals(request.method())) {
				final var content = content(path);
				exchange.sendResponseHeaders(200, content.length);
				exchange.getResponseBody().write(content);
			} else {
				exchange.sendResponseHeaders(200, -1);
			}
			return;
		}
		final var fields = request.token().split(":", -1);
		final var clientMemory = transport(request.tokenNic()).getRegisteredBuffer(Long.parseLong(fields[2], 16)).duplicate();
		final int size = Integer.parseInt(fields[1], 16);
		clientMemory.clear().limit(size);
		exchange.getResponseHeaders().set(RdmaReplyContract.REPLY_HEADER, "200");
		if ("PUT".equals(request.method())) {
			final var payload = new byte[size];
			clientMemory.get(payload);
			if (query.contains("partNumber=")) {
				uploads.computeIfAbsent(param(query, "uploadId"), unused -> new ConcurrentSkipListMap<>())
								.put(Integer.parseInt(param(query, "partNumber")), payload);
				exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			} else {
				objects.put(path, payload);
			}
			exchange.sendResponseHeaders(200, -1);
			return;
		}
		clientMemory.put(content(path), 0, size);
		exchange.getResponseHeaders().set(RdmaReplyContract.BYTES_TRANSFERRED_HEADER, Integer.toString(size));
		objectResponseHeaders.forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
		exchange.sendResponseHeaders(200, -1);
	}

	/** The object at {@code path}, or the server's fill for one nobody wrote. */
	private byte[] content(final String path) {
		final var object = objects.get(path);
		if (object != null) {
			return object;
		}
		final var fill = new byte[SIZE];
		Arrays.fill(fill, SERVER_GET_FILL);
		return fill;
	}

	private void answer(final Request request, final byte[] body, final HttpExchange exchange) throws IOException {
		final var query = request.query() == null ? "" : request.query();
		final var path = request.path();
		switch (request.method()) {
		case "PUT" -> {
			if (query.contains("partNumber=")) {
				uploads.computeIfAbsent(param(query, "uploadId"), unused -> new ConcurrentSkipListMap<>())
								.put(Integer.parseInt(param(query, "partNumber")), body);
			} else {
				objects.put(path, body);
			}
			exchange.getResponseHeaders().set("ETag", "\"etag-" + requests.size() + "\"");
			exchange.sendResponseHeaders(200, -1);
		}
		case "POST" -> {
			if (query.contains("uploads")) {
				final var uploadId = "upload-" + uploadIds.incrementAndGet();
				uploads.put(uploadId, new ConcurrentSkipListMap<>());
				respond(exchange, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key><UploadId>" + uploadId
								+ "</UploadId></InitiateMultipartUploadResult>");
			} else {
				final var parts = uploads.remove(param(query, "uploadId"));
				final var whole = new ByteArrayOutputStream();
				parts.values().forEach(whole::writeBytes);
				objects.put(path, whole.toByteArray());
				respond(exchange, "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>");
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
			exchange.getResponseHeaders().set("Content-Length", "0");
			exchange.sendResponseHeaders(200, -1);
		}
		case "GET" -> {
			if (!request.isObjectRequest()) {
				respond(exchange, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
				return;
			}
			final var content = content(path);
			if (request.range() != null) {
				final var bounds = request.range().substring("bytes=".length()).split("-", -1);
				final var first = Integer.parseInt(bounds[0]);
				final var last = Integer.parseInt(bounds[1]);
				exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
				exchange.sendResponseHeaders(206, last - first + 1);
				exchange.getResponseBody().write(Arrays.copyOfRange(content, first, last + 1));
				return;
			}
			exchange.sendResponseHeaders(200, content.length == 0 ? -1 : content.length);
			exchange.getResponseBody().write(content);
		}
		default -> exchange.sendResponseHeaders(405, -1);
		}
	}

	private static void respond(final HttpExchange exchange, final String xml) throws IOException {
		final var bytes = xml.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(200, bytes.length);
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

	static void waitFor(final BooleanSupplier condition, final String what) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5);
		}
		assertTrue(condition.getAsBoolean(), what);
	}

	/** The terminal rows of {@code driver}, closed by this call. */
	static NicPathRows closedRows(final S3RdmaNicSetDriver<DataItem, Operation<DataItem>> driver) throws IOException {
		driver.close();
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

	/** Alive lanes of step {@code stepId}. */
	static List<String> laneThreads(final String stepId) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(Thread::isAlive)
						.map(Thread::getName)
						.filter(name -> name.startsWith(stepId + "-rdma-lane-"))
						.toList();
	}

	/** Fills {@code size} bytes the way a PUT of the item reads it, for comparison with what the stub received. */
	static byte[] expected(final String name, final int size) {
		try (final var input = dataInput()) {
			final var item = new DataItemImpl(name, name.hashCode() & 0xFFFF, size);
			item.dataInput(input);
			final var buffer = ByteBuffer.allocate(size);
			while (buffer.hasRemaining()) {
				item.read(buffer);
			}
			return buffer.array();
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static NicInventory.Nic nic(final int index) {
		final var address = Ipv4.parse(ADDRESSES.get(index));
		return new NicInventory.Nic(NICS.get(index), NicInventory.Kind.PHYSICAL, null, true,
						List.of(Ipv4Prefix.parse(ADDRESSES.get(index) + "/8")), 25_000, 1_500, index % 2, "0000:01:00." + index, List.of(),
						null, null, DEVICES.get(index), List.of(new NicInventory.RoceGid(DEVICES.get(index), 1, 3, address, true)),
						SYSCTLS);
	}

	private static NicInventory.RdmaDevice device(final String name) {
		return new NicInventory.RdmaDevice(name, "mlx5_core", List.of(new NicInventory.RdmaPort(1, "ACTIVE", "Ethernet")), true, 4096);
	}

	private static NicInventory.Route route(final String nic) {
		return new NicInventory.Route(nic, Ipv4.parse("127.0.0.0"), Ipv4.mask(8), 0, false);
	}
}
