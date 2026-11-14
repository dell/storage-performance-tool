package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nics;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.storage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.logging.Loggers;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A multipart upload's parent makes two exchanges on one operation, its initiation and then its
 * completion or abort, and a retried part makes another on its part. Each exchange starts from
 * cleared timing, so none logs a false "Request is finished … after the response is started" on
 * the Errors logger, and each result carries its own exchange's times. A composite READ's
 * finalization sends no request, so its result keeps the timing of its initial pass. Each case
 * runs on the default route and on a NIC set, over {@link NicSetLoopback}'s stub.
 */
@Timeout(60)
final class S3MultipartRequestTimingTest {

	private static final Operation.Status SUCC = Operation.Status.SUCC;
	private static final int PARTS = 5;
	/** {@code CoopStorageDriverBase.MAX_PART_RETRIES}. */
	private static final int PART_RETRIES = 3;
	private static final String FALSE_LINE = "Request is finished";
	private static final String DRAINED = "multipart-request-timing-log-capture-drained";

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final ErrorsCapture errors = new ErrorsCapture();

	/** A started driver and the results it published. */
	private record Run(S3StorageDriver<DataItem, Operation<DataItem>> driver, EndpointSelectionTestSupport.Results results) {}

	@BeforeEach
	void start() {
		errors.attach();
	}

	@AfterEach
	void stop() throws Exception {
		try {
			for (var i = resources.size() - 1; i >= 0; i--) {
				resources.get(i).close();
			}
			stub.close();
			awaitNoNicIoThreads();
		} finally {
			errors.detach();
		}
	}

	@Test
	void anUploadOnTheDefaultRouteLogsNoRequestStateFailureAndItsParentTimesItsCompletion() throws Exception {
		anUploadLogsNoRequestStateFailureAndItsParentTimesItsCompletion(defaultRoute());
	}

	@Test
	void anUploadOnANicSetLogsNoRequestStateFailureAndItsParentTimesItsCompletion() throws Exception {
		anUploadLogsNoRequestStateFailureAndItsParentTimesItsCompletion(nicSet());
	}

	@Test
	void aRetriedPartOnTheDefaultRouteLogsNoRequestStateFailureAndRecordsItsLastAttempt() throws Exception {
		aRetriedPartLogsNoRequestStateFailureAndRecordsItsLastAttempt(defaultRoute());
	}

	@Test
	void aRetriedPartOnANicSetLogsNoRequestStateFailureAndRecordsItsLastAttempt() throws Exception {
		aRetriedPartLogsNoRequestStateFailureAndRecordsItsLastAttempt(nicSet());
	}

	@Test
	void anAbortOnTheDefaultRouteLogsNoRequestStateFailureAndIsTimedByItsOwnExchange() throws Exception {
		anAbortLogsNoRequestStateFailureAndIsTimedByItsOwnExchange(defaultRoute());
	}

	@Test
	void anAbortOnANicSetLogsNoRequestStateFailureAndIsTimedByItsOwnExchange() throws Exception {
		anAbortLogsNoRequestStateFailureAndIsTimedByItsOwnExchange(nicSet());
	}

	@Test
	void aCompositeReadOnTheDefaultRouteKeepsItsInitialRequestStartAndLogsNothing() throws Exception {
		aCompositeReadKeepsItsInitialRequestStartAndLogsNothing(defaultRoute());
	}

	@Test
	void aCompositeReadOnANicSetKeepsItsInitialRequestStartAndLogsNothing() throws Exception {
		aCompositeReadKeepsItsInitialRequestStartAndLogsNothing(nicSet());
	}

	private void anUploadLogsNoRequestStateFailureAndItsParentTimesItsCompletion(final Run run) throws Exception {
		errors.drained();
		final var results = upload(run, "clean", PARTS);

		final var parent = finalResult(results);
		assertEquals(SUCC, parent.status(), parent::toString);
		final var parts = parts(results);
		assertEquals(PARTS, parts.size(), results::toString);
		assertTimedInOrder(parent);
		for (final var part : parts) {
			assertEquals(SUCC, part.status(), part::toString);
			assertTimedInOrder(part);
			// The parent's result is its completion's exchange, which starts once every part has ended.
			assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "parent " + times(parent) + ", part " + times(part));
		}
		// The Errors logger is shared by the whole test JVM, so only this defect's line is checked.
		assertEquals(List.of(), falseLines(errors.drained()));
	}

	private void aRetriedPartLogsNoRequestStateFailureAndRecordsItsLastAttempt(final Run run) throws Exception {
		// Object requests count from 1, the initiation first: the first part to arrive is answered 503.
		stub.failObjectRequest = n -> n == 2;
		errors.drained();
		final var results = upload(run, "retried", PARTS);

		final var parent = finalResult(results);
		assertEquals(SUCC, parent.status(), parent::toString);
		final var parts = parts(results);
		assertEquals(PARTS + 1, parts.size(), results::toString);
		final var failed = parts.stream().filter(part -> part.status() != SUCC).toList();
		assertEquals(1, failed.size(), results::toString);
		final var first = failed.get(0);
		assertEquals(0, first.retryCount());
		final var retried = parts.stream().filter(part -> part.partNumber() == first.partNumber() && part.status() == SUCC)
						.toList();
		assertEquals(1, retried.size(), results::toString);
		final var last = retried.get(0);
		assertEquals(1, last.retryCount());
		assertTimedInOrder(last);
		assertTrue(last.reqTimeStart() > first.respTimeDone(), () -> "retry " + times(last) + ", failed " + times(first));
		assertTimedInOrder(parent);
		for (final var part : parts) {
			assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "parent " + times(parent) + ", part " + times(part));
		}
		// The failed attempt's 503 and the part retry are warned of; a request-state failure is not.
		final var lines = errors.drained();
		assertTrue(lines.stream().anyMatch(line -> line.contains("part #" + first.partNumber() + " failed")), lines::toString);
		assertEquals(List.of(), lines.stream().filter(line -> line.contains("IllegalStateException")).toList());
	}

	private void anAbortLogsNoRequestStateFailureAndIsTimedByItsOwnExchange(final Run run) throws Exception {
		// Every part is answered 503, so the part runs out of retries and the upload is aborted. One
		// part, so the upload's abort is its last result.
		stub.failParts = true;
		errors.drained();
		final var results = upload(run, "aborted", 1);

		final var composites = results.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
		assertEquals(2, composites.size(), "the initiation and the abort: " + results);
		final var initiation = composites.get(0);
		final var parent = composites.get(1);
		assertEquals(SUCC, initiation.status(), initiation::toString);
		assertNotEquals(SUCC, parent.status(), parent::toString);
		assertEquals(1, stub.requests().stream().filter(S3MultipartRequestTimingTest::isAbort).count(),
						() -> "one abort was sent: " + stub.requests());
		final var parts = parts(results);
		assertEquals(1 + PART_RETRIES, parts.size(), results::toString);
		assertEquals(PART_RETRIES, parts.get(parts.size() - 1).retryCount(), results::toString);
		// The parent's result is the abort's exchange, which starts once the part's last attempt has ended.
		assertTimedInOrder(parent);
		assertTrue(parent.reqTimeStart() > initiation.respTimeDone(), () -> "abort " + times(parent) + ", initiation " + times(initiation));
		for (final var part : parts) {
			assertNotEquals(SUCC, part.status(), part::toString);
			assertTrue(parent.reqTimeStart() > part.respTimeDone(), () -> "abort " + times(parent) + ", part " + times(part));
		}
		// Each failed attempt's 503 and its retry are warned of; a request-state failure is not.
		final var lines = errors.drained();
		assertTrue(lines.stream().anyMatch(line -> line.contains("part #" + parts.get(0).partNumber() + " failed")), lines::toString);
		assertEquals(List.of(), lines.stream().filter(line -> line.contains("IllegalStateException")).toList());
	}

	private void aCompositeReadKeepsItsInitialRequestStartAndLogsNothing(final Run run) throws Exception {
		// A literal name: the initial pass's completion can rename the item to its path before the
		// test thread reads it.
		final var name = "read";
		stub.absentObjectSize = (long) PARTS * ITEM_SIZE;
		final var read = new CompositeDataOperationImpl<DataItem>(0, OpType.READ, new DataItemImpl(name, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		errors.drained();

		final var start = System.nanoTime();
		assertTrue(run.driver().put(read));
		final var results = await(run, name);
		final var wallMicros = (System.nanoTime() - start) / 1_000;

		final var composites = results.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
		assertEquals(2, composites.size(), "the initial pass and the finalization: " + results);
		final var initial = composites.get(0);
		final var parent = composites.get(1);
		assertEquals(SUCC, parent.status(), parent::toString);
		assertEquals(PARTS, parts(results).size(), results::toString);
		assertNotEquals(0, initial.reqTimeStart());
		assertEquals(initial.reqTimeStart(), parent.reqTimeStart(), "the read's duration runs from its initial pass");
		assertTrue(parent.duration() > 0 && parent.duration() <= wallMicros,
						() -> "duration " + parent.duration() + " µs against " + wallMicros + " µs of wall time");
		assertEquals(List.of(), errors.drained(), "the read logged nothing");
	}

	private Run defaultRoute() throws Exception {
		final var driver = new S3StorageDriver<DataItem, Operation<DataItem>>("multipart-timing-default",
						new SeedDataInput(1, ITEM_SIZE, 1, true), storage(stub.listeners(1), List.of(), 2), false, 16);
		return started(driver);
	}

	private Run nicSet() throws Exception {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
		return started(NicSetLoopback.driver(storage(stub.listeners(2), nics(2), 2), false));
	}

	private Run started(final S3StorageDriver<DataItem, Operation<DataItem>> driver) {
		resources.add(driver);
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput(results);
		driver.start();
		return new Run(driver, results);
	}

	private static List<Operation<?>> upload(final Run run, final String name, final int parts) throws InterruptedException {
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(name, 7, (long) parts * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		assertTrue(run.driver().put(upload));
		return await(run, name);
	}

	/**
	 * Every result of the composite operation on {@code name}, in order, up to its own final
	 * result, once nothing is in flight. An item name in a result is its full path.
	 */
	private static List<Operation<?>> await(final Run run, final String name) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		final List<Operation<?>> results = new ArrayList<>();
		while (System.nanoTime() < deadline) {
			final var result = run.results().poll();
			if (result != null) {
				assertTrue(result.item().name().endsWith("/" + name), result::toString);
				results.add(result);
			} else if (!results.isEmpty() && results.get(results.size() - 1) instanceof CompositeDataOperation<?> last
							&& (last.allSubOperationsDone() || last.status() != SUCC) && run.driver().activeOpCount() == 0) {
				return results;
			}
		}
		throw new AssertionError("the operation did not finish: " + results);
	}

	private static Operation<?> finalResult(final List<Operation<?>> results) {
		final var last = results.get(results.size() - 1);
		assertTrue(last instanceof CompositeDataOperation<?> composite && composite.allSubOperationsDone(), results::toString);
		return last;
	}

	private static List<PartialDataOperation<?>> parts(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
	}

	/** The false "Request is finished … after the response is started" lines among {@code lines}. */
	private static List<String> falseLines(final List<String> lines) {
		return lines.stream().filter(line -> line.contains(FALSE_LINE)).toList();
	}

	private static boolean isAbort(final NicSetLoopback.Request request) {
		return "DELETE".equals(request.method()) && request.query() != null && request.query().contains("uploadId=");
	}

	/** All four times of an exchange are set and in order. */
	private static void assertTimedInOrder(final Operation<?> result) {
		assertTrue(result.reqTimeStart() > 0 && result.reqTimeStart() <= result.reqTimeDone()
						&& result.reqTimeDone() <= result.respTimeStart() && result.respTimeStart() <= result.respTimeDone(),
						() -> times(result));
	}

	private static String times(final Operation<?> result) {
		return result.getClass().getSimpleName() + " " + result.status() + " [" + result.reqTimeStart() + ", "
						+ result.reqTimeDone() + ", " + result.respTimeStart() + ", " + result.respTimeDone() + "]";
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
			super("multipart-request-timing-capture", null, null, true, Property.EMPTY_ARRAY);
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

		/**
		 * The lines logged since the last call. A marker logged after them, on the same asynchronous
		 * logger, arrives once they all have.
		 */
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
}
