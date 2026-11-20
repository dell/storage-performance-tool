package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The driver's workers start an operation's request timing only on its first submission, so a
 * multipart upload's parent keeps its initiation's request times through its completion and
 * reports the whole upload; the timing reset that the Netty drivers need before a completion must
 * not reach it. A retried part is a new submission, timed from its own start.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsMultipartTimingTest {

	private static final int PART_SIZE = 1024;
	private static final int PARTS = 3;
	private static final long RESULT_TIMEOUT_SECONDS = 20;
	private static final String DRAINED = "s3-aws-multipart-timing-log-capture-drained";

	private final MultipartStub stub = new MultipartStub();
	private final ErrorsCapture errors = new ErrorsCapture();
	private final LinkedBlockingQueue<Operation<?>> results = new LinkedBlockingQueue<>();
	private S3AwsStorageDriver<DataItem, Operation<DataItem>> driver;

	@BeforeEach
	void start(@TempDir final Path home) throws Exception {
		errors.attach();
		driver = driver(home, stub.port());
		driver.operationResultOutput(new ResultOutput(results));
		driver.start();
	}

	@AfterEach
	void stop() throws Exception {
		try {
			if (driver != null) {
				driver.close();
			}
		} finally {
			stub.close();
			errors.detach();
		}
	}

	@Test
	void anUploadsParentKeepsItsInitiationsRequestTimesThroughItsCompletion() throws Exception {
		errors.drained();
		final var start = System.nanoTime();
		final var upload = upload("whole");
		final var wallMicros = (System.nanoTime() - start) / 1_000;

		final var composites = upload.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
		assertEquals(2, composites.size(), "the initiation and the completion: " + upload);
		final var initiation = composites.get(0);
		final var parent = composites.get(1);
		assertEquals(Operation.Status.SUCC, parent.status(), parent::toString);
		assertTrue(initiation.reqTimeStart() > 0);
		assertEquals(initiation.reqTimeStart(), parent.reqTimeStart(), "the upload is timed from its initiation");
		assertEquals(initiation.reqTimeDone(), parent.reqTimeDone());
		for (final var part : parts(upload)) {
			assertTrue(parent.respTimeStart() > part.respTimeDone(), () -> "parent " + times(parent) + ", part " + times(part));
		}
		assertTrue(parent.duration() > 0 && parent.duration() <= wallMicros,
						() -> "duration " + parent.duration() + " µs against " + wallMicros + " µs of wall time");
		assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
	}

	@Test
	void aPartThatFailsOnceIsRetriedAndTheUploadCompletes() throws Exception {
		stub.failPart = n -> n == 1;
		errors.drained();
		final var upload = upload("retried");

		final var parent = upload.get(upload.size() - 1);
		assertEquals(Operation.Status.SUCC, parent.status(), upload::toString);
		final var parts = parts(upload);
		assertEquals(PARTS + 1, parts.size(), upload::toString);
		final var failed = parts.stream().filter(part -> part.status() != Operation.Status.SUCC).toList();
		assertEquals(1, failed.size(), upload::toString);
		final var first = failed.get(0);
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, first.status(), first::toString);
		final var retried = parts.stream().filter(part -> part.partNumber() == first.partNumber() && part.status() == Operation.Status.SUCC)
						.toList();
		assertEquals(1, retried.size(), upload::toString);
		final var last = retried.get(0);
		assertEquals(1, last.retryCount());
		assertTrue(last.reqTimeStart() > first.respTimeDone() && last.reqTimeStart() <= last.reqTimeDone()
						&& last.reqTimeDone() <= last.respTimeStart() && last.respTimeStart() <= last.respTimeDone(),
						() -> "retry " + times(last) + ", failed " + times(first));
		assertEquals(PARTS + 1, stub.partRequests.get(), "one retry, sent");
		assertEquals(List.of(), stub.aborts);
		assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
	}

	/** Every result of the upload of {@code name}, in order, up to the parent's last. */
	private List<Operation<?>> upload(final String name) throws Exception {
		final DataItem item = new DataItemImpl(name, 0, (long) PARTS * PART_SIZE);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
		assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, item, null, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0, PART_SIZE)));
		final List<Operation<?>> upload = new ArrayList<>();
		while (true) {
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, () -> "the upload did not finish: " + upload);
			assertTrue(result.item().name().endsWith("/" + name), result::toString);
			upload.add(result);
			if (result instanceof CompositeDataOperation<?> composite
							&& (composite.allSubOperationsDone() || result.status() != Operation.Status.SUCC)) {
				return upload;
			}
		}
	}

	private static List<PartialDataOperation<?>> parts(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
	}

	private static String times(final Operation<?> result) {
		return result.getClass().getSimpleName() + " " + result.status() + " [" + result.reqTimeStart() + ", "
						+ result.reqTimeDone() + ", " + result.respTimeStart() + ", " + result.respTimeDone() + "]";
	}

	@SuppressWarnings("unchecked")
	private static S3AwsStorageDriver<DataItem, Operation<DataItem>> driver(final Path home, final int port) throws Exception {
		final var storage = S3AwsMergedConfig.create(home, port, Map.of()).configVal("storage");
		return (S3AwsStorageDriver<DataItem, Operation<DataItem>>) new S3AwsStorageDriverFactory<DataItem, Operation<DataItem>>()
						.create("s3-aws-multipart-timing",
										DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false), storage, false, 16);
	}

	/**
	 * A loopback S3 that answers multipart uploads: their initiation, parts, completion and abort,
	 * and every other request with an empty 200. Each initiation gets an upload ID of its own, from
	 * {@code upload-1}. It answers with {@link #failureStatus} the parts {@link #failPart} picks,
	 * counted from 1 over every part request it receives, and every part of the uploads
	 * {@link #failUpload} picks by ID. The default 400 is one the client does not retry itself.
	 */
	static final class MultipartStub implements AutoCloseable {

		private final HttpServer server;
		private final ExecutorService threads = Executors.newCachedThreadPool();
		final AtomicInteger partRequests = new AtomicInteger();
		final AtomicInteger initiations = new AtomicInteger();
		final AtomicInteger completions = new AtomicInteger();
		final List<String> aborts = new CopyOnWriteArrayList<>();
		/** The upload IDs initiated and neither completed nor aborted. */
		final Set<String> openUploads = ConcurrentHashMap.newKeySet();
		volatile IntPredicate failPart = n -> false;
		volatile Predicate<String> failUpload = uploadId -> false;
		volatile boolean failCompletion;
		volatile int failureStatus = 400;

		MultipartStub() {
			try {
				server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			} catch (final IOException e) {
				throw new IllegalStateException(e);
			}
			server.setExecutor(threads);
			server.createContext("/", this::handle);
			server.start();
		}

		int port() {
			return server.getAddress().getPort();
		}

		private void handle(final HttpExchange exchange) throws IOException {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				final var method = exchange.getRequestMethod();
				final var query = exchange.getRequestURI().getRawQuery() == null ? "" : exchange.getRequestURI().getRawQuery();
				exchange.getResponseHeaders().set("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
				if ("PUT".equals(method) && query.contains("partNumber=")) {
					final var n = partRequests.incrementAndGet();
					if (failPart.test(n) || failUpload.test(uploadId(query))) {
						final var status = failureStatus;
						respond(exchange, status, "<Error><Code>" + errorCode(status) + "</Code><Message>scripted</Message></Error>");
						return;
					}
					exchange.getResponseHeaders().set("ETag", "\"etag-" + n + "\"");
					empty(exchange, 200);
				} else if ("POST".equals(method) && query.contains("uploads")) {
					final var uploadId = "upload-" + initiations.incrementAndGet();
					openUploads.add(uploadId);
					respond(exchange, 200, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>"
									+ "<UploadId>" + uploadId + "</UploadId></InitiateMultipartUploadResult>");
				} else if ("POST".equals(method) && query.contains("uploadId=") && failCompletion) {
					final var status = failureStatus;
					respond(exchange, status, "<Error><Code>" + errorCode(status) + "</Code><Message>scripted</Message></Error>");
				} else if ("POST".equals(method) && query.contains("uploadId=")) {
					completions.incrementAndGet();
					openUploads.remove(uploadId(query));
					respond(exchange, 200, "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>"
									+ "<ETag>\"done\"</ETag></CompleteMultipartUploadResult>");
				} else if ("DELETE".equals(method) && query.contains("uploadId=")) {
					aborts.add(exchange.getRequestURI().toString());
					openUploads.remove(uploadId(query));
					exchange.sendResponseHeaders(204, -1);
				} else if ("PUT".equals(method) && exchange.getRequestHeaders().containsKey("x-amz-copy-source")) {
					respond(exchange, 200, "<CopyObjectResult><ETag>\"copied\"</ETag></CopyObjectResult>");
				} else {
					empty(exchange, 200);
				}
			}
		}

		private static String uploadId(final String query) {
			for (final var param : query.split("&")) {
				if (param.startsWith("uploadId=")) {
					return param.substring("uploadId=".length());
				}
			}
			return "";
		}

		private static String errorCode(final int status) {
			return switch (status) {
			case 403 -> "AccessDenied";
			case 500 -> "InternalError";
			case 503 -> "SlowDown";
			default -> status >= 500 ? "ServiceUnavailable" : "InvalidArgument";
			};
		}

		private static void empty(final HttpExchange exchange, final int status) throws IOException {
			exchange.getResponseHeaders().set("Content-Length", "0");
			exchange.sendResponseHeaders(status, -1);
		}

		private static void respond(final HttpExchange exchange, final int status, final String xml) throws IOException {
			final var bytes = xml.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(status, bytes.length);
			exchange.getResponseBody().write(bytes);
		}

		@Override
		public void close() {
			server.stop(0);
			threads.shutdownNow();
		}
	}

	/** The lines of the Errors logger, at DEBUG and above, while it is attached. */
	private static final class ErrorsCapture extends AbstractAppender {

		private final List<String> lines = new CopyOnWriteArrayList<>();
		private volatile CountDownLatch drain = new CountDownLatch(1);
		/** How many lines earlier calls of {@link #drained()} returned; only the test thread uses it. */
		private int consumed;
		private org.apache.logging.log4j.core.Logger logger;
		private Level level;

		private ErrorsCapture() {
			super("s3-aws-multipart-timing-capture", null, null, true, Property.EMPTY_ARRAY);
		}

		void attach() {
			logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.ERR.getName());
			level = logger.getLevel();
			start();
			logger.addAppender(this);
			logger.setLevel(Level.DEBUG);
		}

		void detach() {
			if (logger != null) {
				logger.setLevel(level);
				logger.removeAppender(this);
				stop();
			}
		}

		@Override
		public void append(final LogEvent event) {
			final var message = event.getMessage().getFormattedMessage();
			if (DRAINED.equals(message)) {
				drain.countDown();
			} else {
				lines.add(event.getLevel() + " " + message);
			}
		}

		/** The lines logged since the last call, once a marker logged after them has arrived. */
		List<String> drained() throws InterruptedException {
			final var marker = new CountDownLatch(1);
			drain = marker;
			Loggers.ERR.debug(DRAINED);
			assertTrue(marker.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS), "the Errors logger drained");
			final var drained = List.copyOf(lines.subList(consumed, lines.size()));
			consumed += drained.size();
			return drained;
		}
	}

	private static final class ResultOutput implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<?>> results;

		ResultOutput(final LinkedBlockingQueue<Operation<?>> results) {
			this.results = results;
		}

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
	}
}
