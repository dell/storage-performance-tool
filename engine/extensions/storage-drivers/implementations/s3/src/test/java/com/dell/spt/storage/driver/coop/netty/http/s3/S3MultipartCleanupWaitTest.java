package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.RESULT_TIMEOUT_SECONDS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nics;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The aborts that follow failed multipart completions, at the driver's stop and close. The stop waits
 * for them, within its own deadline, before the I/O shuts down; the close is a bounded backstop; and a
 * completion or abort that never had a connection settles its upload on any failure but an interrupt.
 * Each driver runs against the loopback stub with integrity metadata on, so its uploads get
 * {@code multipart.lifecycle.csv} records.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3MultipartCleanupWaitTest {

	/** A net timeout the tests' bounds stay well below, so a wait that runs it out fails them. */
	private static final int NET_TIMEOUT_MILLIS = 5_000;
	/** How long the stub holds the delayed abort before it answers. */
	private static final long ABORT_DELAY_MILLIS = 300;
	/** A short stop wait, in place of the shipped 5 s, for the tests of what follows it. */
	private static final long STOP_WAIT_MILLIS = 300;
	/** Room for a loaded host above an expected wait, still below {@link #NET_TIMEOUT_MILLIS}. */
	private static final long SLACK_MILLIS = 2_500;
	private static final int PARTS = 3;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final CountDownLatch released = new CountDownLatch(1);
	private AbstractAppender capture;

	@BeforeEach
	void setUp() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
		capture = new AbstractAppender("multipart-cleanup-capture", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				if (event.getLevel().isMoreSpecificThan(org.apache.logging.log4j.Level.WARN)) {
					warnings.add(event.getMessage().getFormattedMessage());
				}
			}
		};
		capture.start();
		errLogger().addAppender(capture);
		msgLogger().addAppender(capture);
	}

	@AfterEach
	void tearDown() throws Exception {
		released.countDown();
		try {
			for (var i = resources.size() - 1; i >= 0; i--) {
				resources.get(i).close();
			}
		} finally {
			errLogger().removeAppender(capture);
			msgLogger().removeAppender(capture);
			capture.stop();
			stub.close();
		}
	}

	// ---- the stop's wait ----

	/**
	 * The completion fails with 400 and the abort's answer comes 300 ms later; the driver is closed as
	 * soon as the upload's result is out. The stop reads the answer before the I/O shuts down, so the
	 * upload is recorded aborted, the close takes about the delay, not the net timeout, and nothing
	 * logs a response timeout.
	 */
	@Test
	void aDelayedAbortIsReadBeforeTheIoShutsDown() throws Exception {
		failCompletions();
		stub.abortDelayMillis = ABORT_DELAY_MILLIS;
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var key = key("delayed");

		upload(driver, key);
		final var closeMillis = closeMillis(driver);

		assertEquals(List.of("failed_aborted"), states(key), () -> lifecycle(key).toString());
		assertTrue(closeMillis < ABORT_DELAY_MILLIS + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		assertEquals(0, stub.openUploads(), "the target aborted the upload");
		assertTrue(warnings.stream().noneMatch(message -> message.contains("Response timeout")), warnings::toString);
	}

	/**
	 * The abort is never answered: the stop waits out its deadline, then the I/O shutdown closes the
	 * abort's connection, which ends its wait for the answer, and the upload is recorded orphaned. The
	 * close is not held past the deadline.
	 */
	@Test
	void anUnansweredAbortEndsAtTheStopDeadline() throws Exception {
		failCompletions();
		stub.abortsUnanswered = true;
		final var driver = probe(NET_TIMEOUT_MILLIS);
		driver.multipartCleanupStopWaitMillis(STOP_WAIT_MILLIS);
		final var key = key("unanswered");

		upload(driver, key);
		waitFor(() -> stub.requests().stream().anyMatch(NicSetLoopback::isAbort), "the abort reached the stub");
		final var closeMillis = closeMillis(driver);

		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort failed"), () -> lifecycle(key).toString());
		assertTrue(closeMillis < STOP_WAIT_MILLIS + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		final var stopWarn = "1 multipart abort(s) unanswered " + STOP_WAIT_MILLIS + " ms after they were sent";
		waitFor(() -> warnings.stream().anyMatch(message -> message.contains(stopWarn)), "the stop's WARN");
		assertEquals(1, warnings.stream().filter(message -> message.contains("multipart abort(s) unanswered")).count(), warnings::toString);
	}

	/**
	 * With {@code storage.net.timeoutMilliSec} 0 the abort's request has no timeout of its own. The
	 * stop's deadline still ends the wait, and the close returns after it.
	 */
	@Test
	void anUnansweredAbortWithoutANetTimeoutEndsAtTheStopDeadline() throws Exception {
		failCompletions();
		stub.abortsUnanswered = true;
		final var driver = probe(0);
		driver.multipartCleanupStopWaitMillis(STOP_WAIT_MILLIS);
		final var key = key("no-timeout");

		upload(driver, key);
		waitFor(() -> stub.requests().stream().anyMatch(NicSetLoopback::isAbort), "the abort reached the stub");
		final var closeMillis = closeMillis(driver);

		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(closeMillis < STOP_WAIT_MILLIS + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
	}

	/**
	 * Two drivers, as a worker's two contexts, each with an abort never answered, are stopped one after
	 * the other, the way a step slice stops its contexts inside the entry node's RMI call. Each abort's
	 * wait counts from when it was scheduled, so the second stop does not wait its own full deadline
	 * again: the two stops together take about one deadline, not two.
	 */
	@Test
	void contextsStoppedInTurnShareOneDeadlineForTheirPendingAborts() throws Exception {
		final long stopWaitMillis = 3_000;
		failCompletions();
		stub.abortsUnanswered = true;
		final var first = probe(0);
		final var second = probe(0);
		first.multipartCleanupStopWaitMillis(stopWaitMillis);
		second.multipartCleanupStopWaitMillis(stopWaitMillis);
		final var firstKey = key("first-context");
		final var secondKey = key("second-context");

		upload(first, firstKey);
		upload(second, secondKey);
		waitFor(() -> stub.requests().stream().filter(NicSetLoopback::isAbort).count() == 2, "both aborts reached the stub");
		final long start = System.nanoTime();
		first.stop();
		final var firstMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		second.stop();
		final var bothMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		assertTrue(firstMillis >= stopWaitMillis - SLACK_MILLIS, () -> "the first stop took " + firstMillis + " ms");
		assertTrue(bothMillis < stopWaitMillis + SLACK_MILLIS, () -> "the two stops took " + bothMillis + " ms");
		assertEquals(List.of("failed_orphaned"), states(firstKey), () -> lifecycle(firstKey).toString());
		assertEquals(List.of("failed_orphaned"), states(secondKey), () -> lifecycle(secondKey).toString());
	}

	/**
	 * A whole step with {@code storage.net.timeoutMilliSec} 0: its one upload's completion fails and the
	 * abort is never answered. The step ends after the stop's shipped deadline and writes its results;
	 * before, it never ended.
	 */
	@Test
	void aStepWithoutANetTimeoutEndsAfterTheStopDeadline() throws Exception {
		failCompletions();
		stub.abortsUnanswered = true;
		final var start = System.nanoTime();

		final var outcomes = S3NicSetWorkloadTest.runScenario(stub.listeners(1), null, "multipart-cleanup-step-" + System.nanoTime(), """
						Load.config({
							"load": {"step": {"limit": {"time": "60s"}}, "op": {"limit": {"count": 1}}},
							"storage": {"net": {"timeoutMilliSec": 0}},
							"item": {"data": {"size": "3KB", "ranges": {"threshold": "1KB"}}}
						}).run();
						""");
		final var stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		assertEquals(List.of(0L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(1, stub.requests().stream().filter(NicSetLoopback::isAbort).count(), () -> "requests: " + stub.requests());
		assertTrue(stepMillis >= S3StorageDriver.MULTIPART_CLEANUP_STOP_WAIT_MILLIS, () -> "the step took " + stepMillis + " ms");
		assertTrue(stepMillis < S3StorageDriver.MULTIPART_CLEANUP_STOP_WAIT_MILLIS + 30_000, () -> "the step took " + stepMillis + " ms");
	}

	/** A cleanup task that fails does not stop the wait, and the I/O still shuts down. */
	@Test
	void aFailedCleanupTaskDoesNotSkipTheIoShutdown() throws Exception {
		failCompletions();
		final var driver = probe(NET_TIMEOUT_MILLIS);
		driver.abort = (op, status) -> {
			throw new IllegalStateException("cleanup task failure");
		};

		upload(driver, key("failed-task"));
		driver.stop();

		assertEquals(Boolean.TRUE, driver.ioStopped(), "the I/O shut down");
		driver.close();
		assertTrue(driver.isClosed());
		waitFor(() -> warnings.stream().anyMatch(message -> message.contains("cleanup task failure")), "the failure's WARN");
		assertEquals(1, warnings.stream().filter(message -> message.contains("cleanup task failure")).count(), warnings::toString);
		assertEquals(0, driver.pendingMultipartCleanupTasks());
	}

	// ---- the close's backstop ----

	/**
	 * An abort still running when the close begins (here held in the test's abort past the stop's
	 * deadline) holds the close at most twice the net timeout plus the margin. Its upload is recorded
	 * orphaned once, with one WARN for the count, and a record the task writes later is dropped.
	 */
	@Test
	void anAbortPendingAtCloseIsRecordedOrphanedAfterTheBound() throws Exception {
		failCompletions();
		final var netTimeout = 500;
		final var driver = probe(netTimeout);
		driver.multipartCleanupStopWaitMillis(0);
		final var aborted = new CountDownLatch(1);
		driver.abort = (op, status) -> {
			awaitReleased();
			driver.recordLate(op);
			aborted.countDown();
		};
		final var key = key("close-bound");

		upload(driver, key);
		final var closeMillis = closeMillis(driver);
		final var bound = 2L * netTimeout + S3StorageDriver.MULTIPART_CLEANUP_CLOSE_MARGIN_MILLIS;

		assertEquals(bound, driver.multipartCleanupCloseBoundMillis());
		assertTrue(closeMillis >= bound - 50 && closeMillis < bound + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort pending at close"), () -> lifecycle(key).toString());
		waitFor(() -> warnings.stream().anyMatch(message -> message.contains("still pending at close")), "the close's WARN");
		assertEquals(1, warnings.stream().filter(message -> message.contains("1 multipart abort(s) still pending at close"))
						.count(), warnings::toString);
		released.countDown();
		assertTrue(aborted.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		assertEquals(1, lifecycle(key).size(), () -> "the late record was dropped: " + lifecycle(key));
	}

	/** With {@code storage.net.timeoutMilliSec} 0, the close's bound is the fixed cap. */
	@Test
	void withoutANetTimeoutTheCloseWaitsTheCap() throws Exception {
		failCompletions();
		final var driver = probe(0);
		driver.multipartCleanupStopWaitMillis(0);
		final var cap = 1_000L;
		driver.multipartCleanupCloseCapMillis(cap);
		driver.abort = (op, status) -> awaitReleased();
		final var key = key("close-cap");

		upload(driver, key);
		final var closeMillis = closeMillis(driver);

		assertEquals(cap, driver.multipartCleanupCloseBoundMillis());
		assertTrue(closeMillis >= cap - 50 && closeMillis < cap + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertEquals(60_000, S3StorageDriver.MULTIPART_CLEANUP_CLOSE_CAP_MILLIS);
	}

	/** An interrupt ends the close's wait at once; the upload is recorded orphaned and the close finishes. */
	@Test
	void anInterruptEndsTheCloseWait() throws Exception {
		failCompletions();
		final var driver = probe(NET_TIMEOUT_MILLIS);
		driver.multipartCleanupStopWaitMillis(0);
		driver.abort = (op, status) -> awaitReleased();
		final var closing = new CountDownLatch(1);
		driver.afterClosing = closing::countDown;
		final var key = key("interrupted");
		upload(driver, key);

		final var interrupted = new boolean[1];
		final var closer = new Thread(() -> {
			try {
				driver.close();
			} catch (final IOException e) {
				throw new IllegalStateException(e);
			}
			interrupted[0] = Thread.currentThread().isInterrupted();
		}, "closer");
		closer.start();
		assertTrue(closing.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		closer.interrupt();
		closer.join(SLACK_MILLIS);

		assertFalse(closer.isAlive(), "the close returned");
		assertTrue(interrupted[0], "the close kept the interrupt");
		assertTrue(driver.isClosed());
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
	}

	/**
	 * An abort scheduled once the close has marked the driver closing (driven from the close itself,
	 * right after the mark) is not sent: its upload is recorded orphaned at once, and nothing is left
	 * for the close to wait on.
	 */
	@Test
	void anAbortScheduledAfterTheCloseBeganIsRecordedWithoutBeingSent() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var key = key("late");
		final var late = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		late.put(S3Api.KEY_UPLOAD_ID, "upload-late");
		final var pendingAfterSchedule = new AtomicInteger(-1);
		driver.afterClosing = () -> {
			driver.scheduleFailedCompletionAbort(late, Operation.Status.RESP_FAIL_CLIENT);
			pendingAfterSchedule.set(driver.pendingMultipartCleanupTasks());
		};

		driver.close();

		assertEquals(0, pendingAfterSchedule.get(), "nothing left to wait on");
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort not sent: the driver is closing"), () -> lifecycle(key).toString());
		assertTrue(stub.requests().stream().noneMatch(NicSetLoopback::isAbort), () -> "requests: " + stub.requests());
	}

	// ---- requests that never had a connection ----

	/** A completion whose lease throws, so it fails with FAIL_UNKNOWN and no connection, aborts its upload. */
	@Test
	void anUnconnectedCompletionThatFailsUnknownAbortsItsUpload() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		// The completion's lease, the first after the last part's.
		driver.failLease = once(lease -> partRequests() == PARTS);
		final var key = key("unknown-completion");

		final var result = upload(driver, key);

		assertEquals(Operation.Status.FAIL_UNKNOWN, result.status());
		waitFor(() -> !lifecycle(key).isEmpty(), "the upload's record");
		assertEquals(List.of("failed_aborted"), states(key), () -> lifecycle(key).toString());
		assertTrue(stub.objectRequests().stream().noneMatch(S3MultipartCleanupWaitTest::isCompletion), "the completion was not sent");
		assertEquals(1, stub.requests().stream().filter(NicSetLoopback::isAbort).count(), () -> "requests: " + stub.requests());
		assertEquals(0, stub.openUploads());
	}

	/** An abort whose lease throws, so it fails with FAIL_UNKNOWN and no connection, records its upload orphaned. */
	@Test
	void anUnconnectedAbortThatFailsUnknownRecordsItsUploadOrphaned() throws Exception {
		stub.failParts = true;
		final var driver = probe(NET_TIMEOUT_MILLIS);
		// The abort's lease, after part 1 and its three retries.
		driver.failLease = once(lease -> partRequests() == 4);
		final var key = key("unknown-abort");

		upload(driver, key);
		waitFor(() -> !lifecycle(key).isEmpty(), "the upload's record");

		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort failed: FAIL_UNKNOWN"), () -> lifecycle(key).toString());
		assertTrue(stub.requests().stream().noneMatch(NicSetLoopback::isAbort), () -> "requests: " + stub.requests());
		assertEquals(1, stub.openUploads(), "nothing aborted the upload");
	}

	/**
	 * A completion whose lease is interrupted never completes: the interrupt goes back to the dispatch,
	 * and the operation stays with the stop. Nothing aborts its upload or records it, even at close.
	 */
	@Test
	void anInterruptedCompletionLeaseNeitherAbortsNorRecords() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var interrupted = new CountDownLatch(1);
		driver.leaseFailure = () -> {
			interrupted.countDown();
			return new InterruptedException("lease interrupted");
		};
		driver.failLease = once(lease -> partRequests() == PARTS);
		final var key = key("interrupted-lease");
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(driver.put(upload));
		assertTrue(interrupted.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		driver.close();

		assertEquals(List.of(), lifecycle(key));
		assertTrue(stub.requests().stream().noneMatch(NicSetLoopback::isAbort), () -> "requests: " + stub.requests());
		assertEquals(1, stub.openUploads());
	}

	// ---- an upload whose completion or abort could not be queued ----

	/**
	 * A 1-slot child queue held full by another operation: the upload's completion cannot be queued
	 * within the enqueue timeout, so the upload is published as a {@code FAIL_TIMEOUT} failure and the
	 * driver aborts it outside the queue. The abort reaches the target, the upload is recorded {@code
	 * failed_aborted} once, and the stop has nothing left to wait for. The driver is not started: the
	 * test crosses dispatch and completes the requests itself, as the coop base's tests do.
	 */
	@Test
	void anUploadWhoseCompletionCannotBeQueuedIsAbortedOutsideTheQueue() throws Exception {
		final var previous = System.getProperty(ENQUEUE_TIMEOUT_PROPERTY);
		System.setProperty(ENQUEUE_TIMEOUT_PROPERTY, "100");
		try {
			final var driver = unstartedProbe(1);
			final var key = key("completion-not-queued");
			final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, ITEM_SIZE),
							null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
			final var uploadId = initiateOnTheStub(key);
			upload.put(S3Api.KEY_UPLOAD_ID, uploadId);
			assertEquals(1, stub.openUploads());

			// The initiation is answered and the only part is queued, then dispatched.
			assertTrue(driver.operationLifecycle().driverQueued(op(upload)));
			assertTrue(driver.claimDispatch(op(upload)));
			upload.status(Operation.Status.SUCC);
			assertTrue(driver.completed(op(upload)));
			final var part = upload.subOperations().get(0);
			assertTrue(driver.childQueue().remove(op(part)), () -> "the part was queued: " + driver.childQueue());
			assertTrue(driver.claimDispatch(op(part)));
			// Another operation takes the queue's only slot, so the completion finds no room.
			assertTrue(driver.childQueue().offer(new OperationImpl<>(0, OpType.CREATE, new DataItemImpl("filler", 0, 1),
							null, "/bucket", CREDENTIAL)));

			part.status(Operation.Status.SUCC);
			upload.markSubTaskCompleted();
			assertTrue(driver.completed(op(part)));

			final var result = awaitFailedUpload(results, key);
			assertEquals(Operation.Status.FAIL_TIMEOUT, result.status());
			waitFor(() -> stub.openUploads() == 0, "the target aborted the upload");
			assertEquals(List.of("failed_aborted"), states(key), () -> lifecycle(key).toString());
			assertTrue(lifecycle(key).get(0).contains(
							"completion not queued: timed out enqueueing MPU completion operation after 100 ms"),
							() -> lifecycle(key).toString());
			final var aborts = stub.requests().stream().filter(NicSetLoopback::isAbort).toList();
			assertEquals(1, aborts.size(), () -> "requests: " + stub.requests());
			assertEquals(uploadId, NicSetLoopback.param(aborts.get(0).query(), "uploadId"));
			waitFor(() -> driver.pendingMultipartCleanupTasks() == 0, "the abort task ended");
			assertTrue(stub.objectRequests().stream().noneMatch(S3MultipartCleanupWaitTest::isCompletion),
							"the completion was not sent");
		} finally {
			if (previous == null) {
				System.clearProperty(ENQUEUE_TIMEOUT_PROPERTY);
			} else {
				System.setProperty(ENQUEUE_TIMEOUT_PROPERTY, previous);
			}
		}
	}

	/**
	 * An upload whose abort could not be queued is aborted outside the queue: its record keeps the
	 * part's failure and says why the abort was not queued.
	 */
	@Test
	void anUploadWhoseAbortCannotBeQueuedIsAbortedOutsideTheQueue() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var key = key("abort-not-queued");
		final var upload = initiatedUpload(key);
		upload.put("mpuFailure", Operation.Status.FAIL_TIMEOUT.name());

		driver.recordUploadLeftOpen(op(upload), true, "timed out enqueueing MPU abort operation after 500 ms");
		driver.stop();

		assertEquals(0, driver.pendingMultipartCleanupTasks(), "the stop waited for the abort");
		assertEquals(0, stub.openUploads(), "the target aborted the upload");
		assertEquals(List.of("failed_aborted"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("multipart part failed: FAIL_TIMEOUT; abort not queued: "
						+ "timed out enqueueing MPU abort operation after 500 ms"), () -> lifecycle(key).toString());
		assertEquals(1, stub.requests().stream().filter(NicSetLoopback::isAbort).count(), () -> "requests: " + stub.requests());
	}

	/** The target refuses the abort: the upload is recorded orphaned, with the reason and the failure. */
	@Test
	void anAbortOutsideTheQueueThatTheTargetRefusesRecordsTheUploadOrphaned() throws Exception {
		stub.failRequest = NicSetLoopback::isAbort;
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var key = key("abort-refused");
		final var upload = initiatedUpload(key);

		driver.recordUploadLeftOpen(op(upload), false, "timed out enqueueing MPU completion operation after 500 ms");
		driver.stop();

		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("completion not queued: timed out enqueueing MPU completion operation "
						+ "after 500 ms; abort failed"), () -> lifecycle(key).toString());
		assertEquals(1, stub.openUploads());
	}

	/**
	 * An abort outside the queue still pending when the close's bound runs out is recorded orphaned,
	 * "pending at close", and the late record of the abort is dropped as a second one.
	 */
	@Test
	void anAbortOutsideTheQueuePendingAtCloseRecordsTheUploadOrphaned() throws Exception {
		final var driver = probe(0);
		driver.multipartCleanupStopWaitMillis(0);
		driver.multipartCleanupCloseCapMillis(300);
		final var started = new CountDownLatch(1);
		final var ended = new CountDownLatch(1);
		driver.uploadAbort = (op, reason) -> {
			started.countDown();
			awaitReleased();
			driver.recordLateUploadAbort(op, reason);
			ended.countDown();
		};
		final var key = key("off-queue-pending");
		final var upload = initiatedUpload(key);

		driver.recordUploadLeftOpen(op(upload), true, "timed out enqueueing MPU abort operation after 500 ms");
		assertTrue(started.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		driver.close();

		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort not queued: timed out enqueueing MPU abort operation after 500 ms; "
						+ "abort pending at close"), () -> lifecycle(key).toString());
		released.countDown();
		assertTrue(ended.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		Thread.sleep(200);
		assertEquals(1, lifecycle(key).size(), () -> "the late record was dropped: " + lifecycle(key));
	}

	/** Once the close has marked the driver closing, the upload is recorded orphaned without an abort. */
	@Test
	void anUploadLeftOpenOnceTheDriverIsClosingIsRecordedWithoutAnAbort() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		final var key = key("off-queue-closing");
		final var upload = initiatedUpload(key);
		final var pendingAfter = new AtomicInteger(-1);
		driver.afterClosing = () -> {
			driver.recordUploadLeftOpen(op(upload), false, "timed out enqueueing MPU completion operation after 500 ms");
			pendingAfter.set(driver.pendingMultipartCleanupTasks());
		};

		driver.close();

		assertEquals(0, pendingAfter.get(), "nothing left to wait on");
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("completion not queued: timed out enqueueing MPU completion operation "
						+ "after 500 ms; abort not sent: the driver is closing"), () -> lifecycle(key).toString());
		assertTrue(stub.requests().stream().noneMatch(NicSetLoopback::isAbort), () -> "requests: " + stub.requests());
	}

	/**
	 * The executor refuses the abort's task: the hook does not throw, nothing is left for the stop to
	 * wait out, and the upload is recorded orphaned.
	 */
	@Test
	void anAbortTaskTheExecutorRefusesRecordsTheUploadOrphaned() throws Exception {
		final var driver = probe(NET_TIMEOUT_MILLIS);
		driver.rejectCleanup = true;
		final var key = key("off-queue-rejected");
		final var upload = initiatedUpload(key);

		driver.recordUploadLeftOpen(op(upload), true, "timed out enqueueing MPU abort operation after 500 ms");

		assertEquals(0, driver.pendingMultipartCleanupTasks(), "nothing left to wait on");
		assertEquals(List.of("failed_orphaned"), states(key), () -> lifecycle(key).toString());
		assertTrue(lifecycle(key).get(0).contains("abort not queued: timed out enqueueing MPU abort operation after 500 ms; "
						+ "abort not sent: RejectedExecutionException"), () -> lifecycle(key).toString());
		final var closeMillis = closeMillis(driver);
		assertTrue(closeMillis < SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		assertTrue(stub.requests().stream().noneMatch(NicSetLoopback::isAbort), () -> "requests: " + stub.requests());
	}

	// ---- the stop hook in subclasses ----

	/** On a NIC set, as on the default route, the stop reads a delayed abort before the I/O shuts down. */
	@Test
	void onANicSetADelayedAbortIsReadBeforeTheIoShutsDown() throws Exception {
		failCompletions();
		stub.abortDelayMillis = ABORT_DELAY_MILLIS;
		final var root = NicSetLoopback.root(stub.listeners(2), nics(2), 1);
		root.val("storage-integrity-mode", "metadata");
		root.val("storage-net-timeoutMilliSec", NET_TIMEOUT_MILLIS);
		final var run = NicSetLoopback.started(NicSetLoopback.driver(root.configVal("storage"), false));
		resources.add(run.driver());
		final var key = key("nic-set");
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);

		assertTrue(run.driver().put(upload));
		awaitFailedUpload(run.results(), key);
		final var closeMillis = closeMillis(run.driver());

		assertEquals(List.of("failed_aborted"), states("nic-set-test", key), () -> lifecycle("nic-set-test", key).toString());
		assertTrue(closeMillis < ABORT_DELAY_MILLIS + SLACK_MILLIS, () -> "close took " + closeMillis + " ms");
		assertEquals(0, stub.openUploads());
	}

	/**
	 * The range NIC-set driver sends no multipart requests of its own, but its stop runs the same wait:
	 * an abort scheduled on it is answered before its stop returns.
	 */
	@Test
	void theRangeNicSetDriverStopWaitsForAPendingAbort() throws Exception {
		stub.abortDelayMillis = ABORT_DELAY_MILLIS;
		stub.absentObjectSize = 10;
		final var run = new NicSetLoopback.RangeRun(NicSetLoopback.storage(stub.listeners(2), nics(2), 1),
						new RangeReadPolicy(3, 2L, 1), 10);
		resources.add(run);
		final var op = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key("range"), 7, 10),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		op.put(S3Api.KEY_UPLOAD_ID, "upload-range");

		run.driver.scheduleFailedCompletionAbort(op, Operation.Status.RESP_FAIL_CLIENT);
		run.driver.stop();

		assertEquals(0, run.driver.pendingMultipartCleanupTasks(), "the stop waited for the abort");
		assertEquals(1, stub.requests().stream().filter(NicSetLoopback::isAbort).count(), () -> "requests: " + stub.requests());
	}

	/**
	 * Every {@link S3StorageDriver} subclass in this module that overrides the stop hook has a test
	 * above showing that its stop reaches the wait. A new override must call
	 * {@code super.beforeIoShutdown()} last, after its own stop work, and get such a test.
	 */
	@Test
	void everyOverrideOfTheStopHookIsCovered() throws Exception {
		assertEquals(Set.of(S3NicSetDriver.class, S3RangeNicSetDriver.class), stopHookOverrides(S3StorageDriver.class));
	}

	/** The subclasses of {@code base} in its own code source that declare {@code beforeIoShutdown}. */
	static Set<Class<?>> stopHookOverrides(final Class<?> base) throws Exception {
		final var location = Path.of(base.getProtectionDomain().getCodeSource().getLocation().toURI());
		final Set<Class<?>> overrides = new HashSet<>();
		try (final var fileSystem = Files.isDirectory(location) ? null
						: FileSystems.newFileSystem(URI.create("jar:" + location.toUri()), Map.of())) {
			final var root = fileSystem == null ? location : fileSystem.getPath("/");
			final var pkg = root.resolve(base.getPackageName().replace('.', '/'));
			try (Stream<Path> classes = Files.walk(pkg)) {
				for (final var file : classes.filter(path -> path.toString().endsWith(".class")).toList()) {
					final var name = root.relativize(file).toString().replace('/', '.').replaceAll("\\.class$", "");
					final var type = Class.forName(name, false, base.getClassLoader());
					if (type != base && base.isAssignableFrom(type) && !Modifier.isInterface(type.getModifiers())
									&& Stream.of(type.getDeclaredMethods()).anyMatch(method -> method.getName().equals("beforeIoShutdown")
													&& method.getParameterCount() == 0)) {
						overrides.add(type);
					}
				}
			}
		}
		return overrides;
	}

	// ---- fixtures ----

	/** A default-route driver whose abort, lease and close the tests can drive. */
	static final class Probe extends S3StorageDriver<DataItem, Operation<DataItem>> {

		interface Abort {
			void run(CompositeDataOperation<?> op, Operation.Status status) throws Exception;
		}

		interface UploadAbort {
			void run(CompositeDataOperation<?> op, String reason) throws Exception;
		}

		volatile Abort abort;
		/** Replaces every abort sent outside the queue, after a failed completion or not. */
		volatile UploadAbort uploadAbort;
		volatile boolean rejectCleanup;
		volatile Runnable afterClosing = () -> {};
		volatile IntPredicate failLease = lease -> false;
		volatile Supplier<Throwable> leaseFailure = () -> new IllegalStateException("lease failure");
		private final AtomicInteger leases = new AtomicInteger();

		Probe(final String stepId, final Config storage) throws Exception {
			super(stepId, new SeedDataInput(1, ITEM_SIZE, 1, true), storage, false, 16);
		}

		@Override
		void abortAfterFailedCompletion(final CompositeDataOperation op, final Operation.Status completionStatus) {
			final var override = abort;
			if (override == null) {
				super.abortAfterFailedCompletion(op, completionStatus);
				return;
			}
			try {
				override.run(op, completionStatus);
			} catch (final RuntimeException e) {
				throw e;
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		void abortUpload(final CompositeDataOperation op, final String reason) {
			final var override = uploadAbort;
			if (override == null) {
				super.abortUpload(op, reason);
				return;
			}
			try {
				override.run(op, reason);
			} catch (final RuntimeException e) {
				throw e;
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		void executeMultipartCleanup(final Runnable task) {
			if (rejectCleanup) {
				throw new RejectedExecutionException("the test refuses the task");
			}
			super.executeMultipartCleanup(task);
		}

		@Override
		void afterMultipartCleanupClosing() {
			afterClosing.run();
		}

		/** Writes the record an abort outside the queue answered after the close's bound would write. */
		void recordLateUploadAbort(final CompositeDataOperation<?> op, final String reason) {
			super.abortUpload(op, reason);
		}

		boolean claimDispatch(final Operation<DataItem> op) {
			return claimDispatchOwnership(op);
		}

		boolean completed(final Operation<DataItem> op) {
			return handleCompleted(op);
		}

		java.util.concurrent.BlockingQueue<Operation<DataItem>> childQueue() {
			return childOpQueue;
		}

		/** Writes the record an abort answered after the close's bound would write. */
		void recordLate(final CompositeDataOperation<?> op) {
			super.abortAfterFailedCompletion(op, Operation.Status.RESP_FAIL_CLIENT);
		}

		Boolean ioStopped() {
			return ioStoppedInTime();
		}

		@Override
		protected NonBlockingConnPool createConnectionPool() {
			final var pool = super.createConnectionPool();
			return new NonBlockingConnPool() {
				@Override
				public void preConnect(final int count) throws ConnectException, InterruptedException {
					pool.preConnect(count);
				}

				@Override
				public Channel lease() throws ConnectException {
					if (failLease.test(leases.incrementAndGet())) {
						throw Probe.<RuntimeException> sneaky(leaseFailure.get());
					}
					return pool.lease();
				}

				@Override
				public int lease(final List<Channel> channels, final int count) throws ConnectException {
					return pool.lease(channels, count);
				}

				@Override
				public void release(final Channel channel) {
					pool.release(channel);
				}

				@Override
				public void release(final List<Channel> channels) {
					pool.release(channels);
				}

				@Override
				public void close() throws IOException {
					pool.close();
				}
			};
		}

		@SuppressWarnings("unchecked")
		private static <T extends Throwable> T sneaky(final Throwable thrown) throws T {
			throw (T) thrown;
		}
	}

	private final String stepId = "multipart-cleanup-" + System.nanoTime();

	private Probe probe(final int netTimeoutMillis) throws Exception {
		final var driver = unstarted(netTimeoutMillis, null);
		driver.start();
		return driver;
	}

	/** A driver that is not started, with a child queue of {@code queueCapacity}. */
	private Probe unstartedProbe(final int queueCapacity) throws Exception {
		return unstarted(NET_TIMEOUT_MILLIS, queueCapacity);
	}

	private Probe unstarted(final int netTimeoutMillis, final Integer queueCapacity) throws Exception {
		final var endpoints = stub.listeners(1);
		endpoint = endpoints.get(0);
		final var root = NicSetLoopback.root(endpoints, List.of(), 1);
		root.val("storage-net-timeoutMilliSec", netTimeoutMillis);
		root.val("storage-integrity-mode", "metadata");
		if (queueCapacity != null) {
			root.val("storage-driver-limit-queue-input", queueCapacity);
		}
		final var driver = new Probe(stepId, root.configVal("storage"));
		resources.add(driver);
		driver.operationResultOutput(results);
		return driver;
	}

	/** The stub listener of the test's driver. */
	private String endpoint;

	private static final String ENQUEUE_TIMEOUT_PROPERTY = "spt.mpu.child.enqueue.timeout.millis";
	private static final Pattern STUB_UPLOAD_ID = Pattern.compile("<UploadId>([^<]+)</UploadId>");

	/** Initiates an upload of {@code key} on the stub, as the driver's initiation would, and returns its id. */
	private String initiateOnTheStub(final String key) throws IOException {
		final var connection = (HttpURLConnection) URI.create("http://" + endpoint + "/bucket/" + key + "?uploads")
						.toURL().openConnection();
		try {
			connection.setRequestMethod("POST");
			try (final var in = connection.getInputStream()) {
				final var matcher = STUB_UPLOAD_ID.matcher(new String(in.readAllBytes(), StandardCharsets.UTF_8));
				assertTrue(matcher.find(), "the stub answered with an upload id");
				return matcher.group(1);
			}
		} finally {
			connection.disconnect();
		}
	}

	/** An upload of {@code key} the stub has initiated, as the base hands it to the driver's hook. */
	private CompositeDataOperationImpl<DataItem> initiatedUpload(final String key) throws IOException {
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		upload.put(S3Api.KEY_UPLOAD_ID, initiateOnTheStub(key));
		assertTrue(stub.openUploads() > 0);
		return upload;
	}

	@SuppressWarnings("unchecked")
	private static Operation<DataItem> op(final Operation<?> op) {
		return (Operation<DataItem>) op;
	}

	private final EndpointSelectionTestSupport.Results results = new EndpointSelectionTestSupport.Results();

	/** Writes {@code key} in {@value #PARTS} parts and returns the upload's final, failed result. */
	private Operation<?> upload(final Probe driver, final String key) throws InterruptedException {
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl(key, 7, (long) PARTS * ITEM_SIZE),
						null, "/bucket", CREDENTIAL, null, 0, ITEM_SIZE);
		assertTrue(driver.put(upload));
		return awaitFailedUpload(results, key);
	}

	private static Operation<?> awaitFailedUpload(final EndpointSelectionTestSupport.Results results, final String key)
					throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			final var result = results.poll();
			if (result instanceof CompositeDataOperation<?> && result.item().name().endsWith("/" + key)
							&& result.status() != Operation.Status.SUCC) {
				return result;
			}
		}
		throw new AssertionError("the upload of " + key + " did not fail");
	}

	private static long closeMillis(final AutoCloseable driver) throws Exception {
		final var start = System.nanoTime();
		driver.close();
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
	}

	/** The stub answers every completion with 400, as SeaweedFS answers one with too small a part. */
	private void failCompletions() {
		stub.failRequest = S3MultipartCleanupWaitTest::isCompletion;
		stub.failureStatus = 400;
	}

	private static boolean isCompletion(final Request request) {
		return "POST".equals(request.method()) && request.query() != null && request.query().contains("uploadId=");
	}

	private long partRequests() {
		return stub.objectRequests().stream().filter(Request::isPart).count();
	}

	/** True for the first lease that {@code which} picks, false for every other. */
	private static IntPredicate once(final IntPredicate which) {
		final var done = new java.util.concurrent.atomic.AtomicBoolean();
		return lease -> which.test(lease) && done.compareAndSet(false, true);
	}

	private void awaitReleased() {
		try {
			// The test releases it; the fallback keeps a wait without a bound from hanging the class.
			released.await(30, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static String key(final String name) {
		// A key of its own: the lifecycle file keeps the records of earlier runs.
		return name + "-" + System.nanoTime();
	}

	private List<String> states(final String key) throws InterruptedException {
		return states(stepId, key);
	}

	private List<String> lifecycle(final String key) {
		return lifecycle(stepId, key);
	}

	/** The states of {@code key}'s lifecycle records, once there is one: the log writes them asynchronously. */
	private static List<String> states(final String step, final String key) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (lifecycle(step, key).isEmpty() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		return lifecycle(step, key).stream()
						.map(record -> Stream.of("failed_aborted", "failed_orphaned", "completed")
										.filter(state -> record.contains("," + state + ",")).findFirst().orElse(record))
						.toList();
	}

	/** The multipart lifecycle records of {@code key}'s upload in step {@code step}'s file. */
	private static List<String> lifecycle(final String step, final String key) {
		try {
			final var file = Path.of(FileManager.INSTANCE.logFileName(Loggers.MULTIPART_LIFECYCLE.getName(), step));
			return Files.exists(file)
							? Files.readAllLines(file).stream().filter(line -> line.contains("," + key + ",")).toList()
							: List.of();
		} catch (final IOException e) {
			throw new java.io.UncheckedIOException(e);
		}
	}

	private static org.apache.logging.log4j.core.Logger errLogger() {
		return (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.ERR.getName());
	}

	private static org.apache.logging.log4j.core.Logger msgLogger() {
		return (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.MSG.getName());
	}
}
