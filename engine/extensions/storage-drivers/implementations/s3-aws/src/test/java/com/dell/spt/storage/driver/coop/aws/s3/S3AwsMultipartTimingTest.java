package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * A multipart upload's parent makes two requests on one operation, its initiation and then its
 * completion or abort. The driver's workers start an operation's request timing only on its first
 * submission, so the driver times the completion or abort itself, from cleared timing: the
 * parent's result is that request, including any retries the client makes for it, as on the Netty
 * drivers, and not the whole upload. A retried part is a new submission, timed from its own start.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsMultipartTimingTest {

	private static final int PART_SIZE = 1024;
	private static final int PARTS = 3;
	private static final long PART_DELAY_MILLIS = 300;
	private static final long COMPLETION_DELAY_MILLIS = 200;
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

	/**
	 * Parts take {@link #PART_DELAY_MILLIS} and the completion {@link #COMPLETION_DELAY_MILLIS}: the
	 * parent's result is the completion, which starts once every part has ended, so its duration is
	 * the completion's, whatever the number of parts. Before, it ran from the initiation, through the
	 * parts.
	 */
	@Test
	void anUploadsParentIsTimedByItsCompletionWhateverItsPartCount() throws Exception {
		stub.partDelayMillis = PART_DELAY_MILLIS;
		stub.completionDelayMillis = COMPLETION_DELAY_MILLIS;
		for (final var parts : List.of(2, 6)) {
			errors.drained();
			final var upload = upload("whole-" + parts, parts);

			final var composites = composites(upload);
			assertEquals(2, composites.size(), "the initiation and the completion: " + upload);
			final var initiation = composites.get(0);
			final var parent = composites.get(1);
			assertEquals(Operation.Status.SUCC, parent.status(), parent::toString);
			assertEquals(parts, parts(upload).size(), upload::toString);
			assertTimedInOrder(initiation);
			assertTimedInOrder(parent);
			assertTrue(parent.reqTimeStart() > initiation.respTimeDone(),
							() -> "parent " + times(parent) + ", initiation " + times(initiation));
			for (final var part : parts(upload)) {
				assertTimedInOrder(part);
				assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "parent " + times(parent) + ", part " + times(part));
			}
			// The completion's own time: at least its delay, and less than a part's on top of it, which
			// the whole upload takes at least.
			assertTrue(parent.duration() >= COMPLETION_DELAY_MILLIS * 1_000
							&& parent.duration() < (COMPLETION_DELAY_MILLIS + PART_DELAY_MILLIS) * 1_000,
							() -> parts + " parts: parent " + parent.duration() + " µs, " + times(parent));
			assertTrue(parent.latency() >= 0 && parent.latency() <= parent.duration(), () -> times(parent));
			assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
		}
		assertEquals(2, stub.completions.get());
	}

	/**
	 * Every part is refused, so the part runs out of the driver's retries and the upload is aborted:
	 * the parent's result is the abort, which starts once the part's last attempt has ended.
	 */
	@Test
	void anAbortedUploadsParentIsTimedByItsAbortWhenAllPartsFail() throws Exception {
		stub.failPart = n -> true;
		errors.drained();
		final var upload = upload("aborted", 1);

		final var composites = composites(upload);
		assertEquals(2, composites.size(), "the initiation and the abort: " + upload);
		final var initiation = composites.get(0);
		final var parent = composites.get(1);
		// The aborted upload ends with its part's failure, not the abort's own outcome.
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, parent.status(), parent::toString);
		assertEquals(1, stub.aborts.size(), stub.aborts::toString);
		assertEquals(0, stub.completionRequests.get());
		final var parts = parts(upload);
		assertEquals(1 + 3, parts.size(), "the part and its three retries: " + upload);
		assertTimedInOrder(parent);
		assertTrue(parent.reqTimeStart() > initiation.respTimeDone(), () -> "abort " + times(parent) + ", initiation " + times(initiation));
		for (final var part : parts) {
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, part.status(), part::toString);
			assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "abort " + times(parent) + ", part " + times(part));
		}
		assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
	}

	/** A completion that is refused fails the upload once, and the upload is aborted. */
	@Test
	void aFailedCompletionFailsTheUploadOnceAndAbortsIt() throws Exception {
		stub.failCompletion = true;
		errors.drained();
		final var upload = upload("refused", PARTS);

		final var composites = composites(upload);
		assertEquals(2, composites.size(), "the initiation and the completion: " + upload);
		final var parent = composites.get(1);
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, parent.status(), parent::toString);
		assertEquals(1, stub.completionRequests.get());
		assertEquals(0, stub.completions.get());
		assertEquals(1, stub.aborts.size(), stub.aborts::toString);
		assertEquals(Set.of(), stub.openUploads);
		// No duration from a cleared start: the parent's times are its completion's, or none.
		assertTrue(parent.duration() >= 0 && parent.duration() < TimeUnit.SECONDS.toMicros(RESULT_TIMEOUT_SECONDS), () -> times(parent));
		assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
		assertNull(results.poll(200, TimeUnit.MILLISECONDS), "no second result of the upload");
	}

	/**
	 * The first completion request is answered 503, which the client retries itself: the upload
	 * completes once, and the parent's one result covers both requests and the back-off between.
	 */
	@Test
	void aCompletionTheClientRetriesIsOneUploadTimedWithItsRetry() throws Exception {
		stub.failCompletionRequest = n -> n == 1;
		stub.failureStatus = 503;
		stub.completionDelayMillis = COMPLETION_DELAY_MILLIS;
		errors.drained();
		final var upload = upload("retried-completion", PARTS);

		final var composites = composites(upload);
		assertEquals(2, composites.size(), "the initiation and the completion: " + upload);
		final var parent = composites.get(1);
		assertEquals(Operation.Status.SUCC, parent.status(), parent::toString);
		assertEquals(2, stub.completionRequests.get(), "the client sent the completion again");
		assertEquals(1, stub.completions.get());
		assertEquals(List.of(), stub.aborts);
		assertTimedInOrder(parent);
		for (final var part : parts(upload)) {
			assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "parent " + times(parent) + ", part " + times(part));
		}
		assertTrue(parent.duration() >= 2 * COMPLETION_DELAY_MILLIS * 1_000, () -> "both completion requests: " + times(parent));
		assertEquals(List.of(), errors.drained().stream().filter(line -> line.contains("IllegalStateException")).toList());
		assertNull(results.poll(200, TimeUnit.MILLISECONDS), "no second result of the upload");
	}

	/**
	 * Admission closes while the upload's last part is in flight, so its completion is never sent. The
	 * parent's timing is reset for a completion that does not happen, and whatever result of it is
	 * published is not a success, with no duration from the cleared start.
	 */
	@Test
	void aCompletionRefusedByTheClosedAdmissionGateIsNotTimed() throws Exception {
		final var gate = new CountDownLatch(1);
		stub.partGate = gate;
		stub.partGateFrom = PARTS;
		final var start = System.nanoTime();
		final DataItem item = new DataItemImpl("refused-gate", 0, (long) PARTS * PART_SIZE);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
		assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, item, null, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0, PART_SIZE)));
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (stub.partRequests.get() < PARTS && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(PARTS, stub.partRequests.get(), "the last part is in flight");
		driver.closeAdmission();
		gate.countDown();
		// The last part ends as a success once admission is closed, so the completion is refused.
		final List<Operation<?>> published = new ArrayList<>();
		while (parts(published).stream().filter(part -> part.status() == Operation.Status.SUCC).count() < PARTS) {
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, () -> "every part succeeded: " + published);
			published.add(result);
		}
		Thread.sleep(200);
		driver.close();
		final var wallMicros = (System.nanoTime() - start) / 1_000;
		driver = null;

		assertEquals(0, stub.completionRequests.get(), "the completion is not sent");
		results.drainTo(published);
		final var composites = composites(published);
		// The closed gate refuses the completion, so this route publishes the initiation only, and no
		// parent result whose timing could be wrong.
		assertEquals(1, composites.size(), () -> "the initiation alone: " + published);
		for (final var parent : composites.subList(1, composites.size())) {
			assertTrue(parent.status() != Operation.Status.SUCC, () -> "a parent not completed: " + times(parent));
			assertTrue(parent.duration() >= 0 && parent.duration() <= wallMicros,
							() -> "duration " + parent.duration() + " µs against " + wallMicros + " µs of wall time: " + times(parent));
			assertTrue(parent.latency() >= 0 && parent.latency() <= wallMicros, () -> times(parent));
		}
	}

	@Test
	void aPartThatFailsOnceIsRetriedAndTheUploadCompletes() throws Exception {
		stub.failPart = n -> n == 1;
		errors.drained();
		final var upload = upload("retried", PARTS);

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

	/** Every result of the upload of {@code name} in {@code parts} parts, in order, up to the parent's last. */
	private List<Operation<?>> upload(final String name, final int parts) throws Exception {
		final DataItem item = new DataItemImpl(name, 0, (long) parts * PART_SIZE);
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

	private static List<Operation<?>> composites(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
	}

	/** {@code result}'s four times are set and in order. */
	private static void assertTimedInOrder(final Operation<?> result) {
		assertTrue(result.reqTimeStart() > 0 && result.reqTimeStart() <= result.reqTimeDone()
						&& result.reqTimeDone() <= result.respTimeStart() && result.respTimeStart() <= result.respTimeDone(),
						() -> times(result));
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
		/** The most uploads open at once. */
		final AtomicInteger maxOpenUploads = new AtomicInteger();
		/** The upload ID of each completion request, in order. */
		final List<String> completedUploads = new CopyOnWriteArrayList<>();
		/** The requests whose upload ID is {@code null}, by method and URI. */
		final List<String> nullUploadIdRequests = new CopyOnWriteArrayList<>();
		volatile IntPredicate failPart = n -> false;
		volatile Predicate<String> failUpload = uploadId -> false;
		volatile boolean failCompletion;
		volatile int failureStatus = 400;
		/** Completion requests received, counted from 1, whether answered or failed. */
		final AtomicInteger completionRequests = new AtomicInteger();
		/** The completion requests answered with {@link #failureStatus}, by their number. */
		volatile IntPredicate failCompletionRequest = n -> false;
		/** How long each part request waits before it is answered. */
		volatile long partDelayMillis;
		/** How long each completion request waits before it is answered. */
		volatile long completionDelayMillis;
		/** Part requests numbered from {@link #partGateFrom} wait for this gate before they are answered. */
		volatile CountDownLatch partGate = new CountDownLatch(0);
		volatile int partGateFrom = 1;

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
				if ("null".equals(uploadId(query))) {
					nullUploadIdRequests.add(method + " " + exchange.getRequestURI());
				}
				exchange.getResponseHeaders().set("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
				if ("PUT".equals(method) && query.contains("partNumber=")) {
					final var n = partRequests.incrementAndGet();
					if (n >= partGateFrom) {
						awaitGate(partGate);
					}
					pause(partDelayMillis);
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
					maxOpenUploads.accumulateAndGet(openUploads.size(), Math::max);
					respond(exchange, 200, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>"
									+ "<UploadId>" + uploadId + "</UploadId></InitiateMultipartUploadResult>");
				} else if ("POST".equals(method) && query.contains("uploadId=")
								&& (failCompletionRequest.test(completionRequest()) || failCompletion)) {
					final var status = failureStatus;
					respond(exchange, status, "<Error><Code>" + errorCode(status) + "</Code><Message>scripted</Message></Error>");
				} else if ("POST".equals(method) && query.contains("uploadId=")) {
					completions.incrementAndGet();
					completedUploads.add(uploadId(query));
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

		/** Counts a completion request and waits out {@link #completionDelayMillis}; returns its number. */
		private int completionRequest() {
			final var n = completionRequests.incrementAndGet();
			pause(completionDelayMillis);
			return n;
		}

		private static void pause(final long millis) {
			if (millis > 0) {
				try {
					Thread.sleep(millis);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}

		private static void awaitGate(final CountDownLatch gate) {
			try {
				gate.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
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
