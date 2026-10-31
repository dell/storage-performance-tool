package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.ADDRESSES;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitNoNicIoThreads;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.awaitRefused;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nics;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.rowsOf;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.stoppedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.RangeRun;
import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Run;
import com.dell.spt.storage.driver.coop.netty.nic.NicAdmission;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.github.akurilov.confuse.Config;
import io.netty.channel.Channel;
import io.netty.channel.epoll.Epoll;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Per-NIC admission and deferred custody of the real S3 drivers with a NIC set, on loopback
 * ({@link NicSetLoopback}): the attempts each NIC carries in the three modes, and an operation
 * whose attempt waits for its connection while the step stops or its circulation goes stale.
 * A NIC's attempt waits by blocking that NIC's only event loop, so its connection is delivered
 * only once the test lets it.
 */
@Timeout(60)
final class S3NicSetAdmissionTest {

	/** Long enough that a held request does not idle out. */
	private static final int NET_TIMEOUT_MILLIS = 20_000;
	/** How long a test watches for an attempt the driver must not start. */
	private static final long QUIET_MILLIS = 300;
	private static final long OBJECT_SIZE = 10;
	/** Bytes 2-4 of every object. */
	private static final RangeReadPolicy FIXED = new RangeReadPolicy(3, 2L, 1);

	private final NicSetLoopback stub = new NicSetLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final List<CountDownLatch> blocks = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		blocks.forEach(CountDownLatch::countDown);
		stub.release();
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
		awaitNoNicIoThreads();
	}

	@Test
	void withSharesEachNicCarriesItsShareAndACompletionHandsItsConnectionOn() throws Exception {
		final var run = started(stub.listeners(2), nics(2), 4);
		assertEquals(true, directDispatch(run), "P >= N gives every NIC a share");
		final var admission = run.pool().admission();

		stub.hold();
		for (var i = 0; i < 16; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "share-" + i)));
		}

		waitFor(() -> stub.objectRequests().size() == 4, "the first four attempts reach the stub");
		Thread.sleep(QUIET_MILLIS);
		assertEquals(4, stub.objectRequests().size(), "the permits bound the attempts in flight");
		assertEquals(2, fromAddress(0), () -> "requests: " + stub.objectRequests());
		assertEquals(2, fromAddress(1), () -> "requests: " + stub.objectRequests());
		assertEquals(List.of(2, 2), counts(admission));
		assertEquals(0, admission.room());

		stub.release();
		for (var i = 0; i < 16; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(16, stub.objectRequests().size());
		assertEquals(2, stub.peakConcurrency(ADDRESSES.get(0)), "a NIC never carries more than its share");
		assertEquals(2, stub.peakConcurrency(ADDRESSES.get(1)));
		stub.objectRequests().forEach(request -> assertEquals(ADDRESSES.get(request.listener()), request.source(), request.toString()));
		awaitIdle(run);
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(16L, 16L, 0L, 0L, 0L, 16L * EndpointSelectionTestSupport.ITEM_SIZE), totals(rows),
						"a directly dispatched attempt counts on its connection's Path: " + rows);
	}

	@Test
	void inTheSharedModeEachNicCarriesOneAttemptAndTheRotationReachesThemAll() throws Exception {
		final var run = started(stub.listeners(4), nics(4), 3);
		assertEquals(false, directDispatch(run), "0 < P < N leaves the rotation to the dispatch task");
		final var admission = run.pool().admission();

		stub.hold();
		for (var i = 0; i < 8; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "shared-" + i)));
		}

		waitFor(() -> stub.objectRequests().size() == 3, "the first three attempts reach the stub");
		Thread.sleep(QUIET_MILLIS);
		assertEquals(3, stub.objectRequests().size(), "the permits bound the attempts in flight");
		assertEquals(Set.copyOf(ADDRESSES.subList(0, 3)), sources(), "one attempt on each of the first three NICs");
		assertEquals(List.of(1, 1, 1, 0), counts(admission));

		stub.release();
		for (var i = 0; i < 8; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(Set.copyOf(ADDRESSES), sources(), "the rotation reaches every NIC");
		ADDRESSES.forEach(address -> assertEquals(1, stub.peakConcurrency(address), address));
		awaitIdle(run);
	}

	@Test
	void withoutALimitTheNicsBoundOnlyTheWaitingAttempts() throws Exception {
		final var run = started(stub.listeners(2), nics(2), 0);
		assertEquals(false, directDispatch(run), "no limit, no shares");
		final var admission = run.pool().admission();
		assertEquals(List.of(NicSetSettings.UNLIMITED_WAITING_LIMIT, NicSetSettings.UNLIMITED_WAITING_LIMIT), limits(admission));

		stub.hold();
		for (var i = 0; i < 30; i++) {
			assertTrue(run.driver().put(dataOp(OpType.CREATE, "unlimited-" + i)));
		}

		waitFor(() -> stub.objectRequests().size() == 30, "every attempt is in flight at once");
		assertEquals(List.of(0, 0), counts(admission), "an attempt in flight no longer counts against its NIC");
		// The rotation skips a NIC whose waiting attempts are at the bound, so the split may differ.
		assertTrue(fromAddress(0) > 0 && fromAddress(1) > 0, () -> "requests: " + stub.objectRequests());

		stub.release();
		for (var i = 0; i < 30; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		awaitIdle(run);
	}

	@Test
	void aStaleCirculationWritesNothingAndStaysQueuedForRecovery() throws Exception {
		final var run = started(stub.listeners(1), nics(1), 1);
		final var admission = run.pool().admission();
		final var unblock = blockEventLoop(run.pool(), 0);
		final Operation<DataItem> op = dataOp(OpType.CREATE, "stale");

		assertTrue(run.driver().put(op));
		waitFor(() -> admission.count(0) == 1, "the attempt took the NIC and waits for its connection");
		run.driver().operationLifecycle().enforceDispatchDeadline(System.nanoTime());
		unblock.countDown();

		waitFor(() -> admission.count(0) == 0 && run.driver().activeOpCount() == 0, "the fenced attempt released its NIC and permit");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, run.driver().operationLifecycle().stateOf(op));
		assertTrue(stub.objectRequests().isEmpty(), () -> "requests: " + stub.objectRequests());
		assertNull(run.results().poll(QUIET_MILLIS));

		run.driver().closeAdmission();
		assertEquals(List.of(op), run.driver().recoverQueuedOperations());
		assertEquals(OperationLifecycleState.UNATTEMPTED, run.driver().operationLifecycle().stateOf(op));
	}

	@Test
	void aStopWhileTheConnectionIsOnItsWayWritesNothingAndRecoversTheOperation() throws Exception {
		final var run = started(stub.listeners(1), nics(1), 1);
		final var admission = run.pool().admission();
		final var unblock = blockEventLoop(run.pool(), 0);
		final Operation<DataItem> op = dataOp(OpType.CREATE, "stopped");

		assertTrue(run.driver().put(op));
		waitFor(() -> admission.count(0) == 1, "the attempt took the NIC and waits for its connection");
		run.driver().closeAdmission();
		assertEquals(List.of(op), run.driver().recoverQueuedOperations(), "the waiting operation is still queued");
		unblock.countDown();

		waitFor(() -> admission.count(0) == 0 && run.driver().activeOpCount() == 0, "the fenced attempt released its NIC and permit");
		assertTrue(stub.objectRequests().isEmpty(), () -> "requests: " + stub.objectRequests());
		assertNull(run.results().poll(QUIET_MILLIS), "a recovered operation publishes no result");
		assertRecoveredUnattempted(run, op, 0);
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(1L, 0L, 0L, 1L, 0L, 0L), totals(rows), "interrupted on its Path: " + rows);
		assertEquals(1, rowsOf(rows, NicPathRows.Kind.PATH).get(0).interrupted());
	}

	@Test
	void aConnectFailingAfterTheStopIsNoIoFailure() throws Exception {
		final var endpoints = stub.listeners(1);
		final var run = started(endpoints, nics(1), 1);
		final var admission = run.pool().admission();
		final var pool = run.pool();
		// The first-use bucket check, on the kernel route, passes while the listener still answers.
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "before")).status());
		// The NIC's event loop, found while its Path still answers.
		final var loop = channelOf(pool, 0);
		stub.stop(0);
		awaitRefused(endpoints.get(0));
		waitFor(() -> pool.state().get(0).open() == 0, "the closed idle connection is dropped");
		final var unblock = block(loop);
		final Operation<DataItem> op = dataOp(OpType.CREATE, "refused");

		assertTrue(run.driver().put(op));
		waitFor(() -> admission.count(0) == 1, "the attempt took the NIC and waits for its connection");
		run.driver().closeAdmission();
		assertEquals(List.of(op), run.driver().recoverQueuedOperations());
		unblock.countDown();

		waitFor(() -> admission.count(0) == 0 && run.driver().activeOpCount() == 0, "the failed setup released its NIC and permit");
		assertNull(run.results().poll(QUIET_MILLIS), "the stop, not the refused connect, settled the operation");
		assertRecoveredUnattempted(run, op, 1);
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(2L, 1L, 0L, 1L, 0L, (long) EndpointSelectionTestSupport.ITEM_SIZE), totals(rows),
						"the refused connect after the stop is interrupted: " + rows);
	}

	@Test
	void aStopAfterTheWriteLeavesTheRequestToFinish() throws Exception {
		final var run = started(stub.listeners(1), nics(1), 1);
		final Operation<DataItem> op = dataOp(OpType.CREATE, "written");
		stub.hold();

		assertTrue(run.driver().put(op));
		waitFor(() -> stub.objectRequests().size() == 1, "the request reaches the stub");
		run.driver().closeAdmission();
		assertEquals(List.of(), run.driver().recoverQueuedOperations(), "the write passed custody to the driver");
		assertEquals(OperationLifecycleState.DISPATCHED, run.driver().operationLifecycle().stateOf(op));

		stub.release();
		assertEquals(Operation.Status.SUCC, run.results().await().status());
		awaitIdle(run);
		assertEquals(List.of(1L, 1L, 0L, 0L, 0L, (long) EndpointSelectionTestSupport.ITEM_SIZE),
						totals(stoppedRows(run.driver(), run.driver())));
	}

	@Test
	void aStopWhileAPartWaitsForItsConnectionSendsNothingMoreOfTheUpload() throws Exception {
		final var run = started(stub.listeners(2), nics(2), 2);
		final var pool = run.pool();
		final var admission = pool.admission();
		// The first-use bucket check, on the kernel route, passes before the upload.
		assertEquals(Operation.Status.SUCC, run.execute(dataOp(OpType.CREATE, "before")).status());
		final var channels = List.of(channelOf(pool, 0), channelOf(pool, 1));
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, new DataItemImpl("upload", 7,
						2L * EndpointSelectionTestSupport.ITEM_SIZE), null, "/bucket", EndpointSelectionTestSupport.CREDENTIAL,
						null, 0, EndpointSelectionTestSupport.ITEM_SIZE);
		stub.hold();

		assertTrue(run.driver().put(upload));
		waitFor(() -> stub.objectRequests().size() == 2, "the initiation reaches the stub");
		// The other NIC's event loop is blocked, so the part it takes waits there for its connection.
		final int waiting = ADDRESSES.get(0).equals(stub.objectRequests().get(1).source()) ? 1 : 0;
		final var unblock = block(channels.get(waiting));
		stub.release();
		waitFor(() -> admission.count(waiting) == 1 && admission.count(1 - waiting) == 0 && parts() == 1,
						"one part was sent and answered while the other waits for its connection");
		run.driver().closeAdmission();
		final var recovered = run.driver().recoverQueuedOperations();
		unblock.countDown();

		waitFor(() -> admission.count(waiting) == 0 && run.driver().activeOpCount() == 0, "the waiting part released its NIC and permit");
		assertEquals(1, recovered.size(), recovered::toString);
		assertTrue(recovered.get(0) instanceof PartialOperation<?> part && part.parent() == upload, "the waiting part was recovered");
		final var lifecycle = run.driver().operationLifecycle();
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(recovered.get(0)));
		assertEquals(1, parts(), () -> "requests: " + stub.objectRequests());
		assertTrue(stub.objectRequests().stream().noneMatch(request -> "POST".equals(request.method())
						&& request.query() != null && request.query().startsWith("uploadId=")), "the upload was not completed");
		assertEquals(0, lifecycle.counters().failed(), "fresh work charges no failure");
		// As on the default route, the upload stays in flight for the step's bounded drain to settle.
		assertEquals(OperationLifecycleState.DISPATCHED, lifecycle.stateOf(upload));
		assertEquals(1, lifecycle.snapshot().unattempted());
		assertEquals(1, lifecycle.snapshot().inFlight());
		lifecycle.expireTerminalDeadline();
		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(upload));
		assertTrue(lifecycle.counters().reconciled(), lifecycle.counters()::toString);
		// The first PUT, the initiation and the sent part succeeded; the waiting part was never sent.
		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(4L, 3L, 0L, 1L, 0L, 2L * EndpointSelectionTestSupport.ITEM_SIZE), totals(rows), rows::toString);
	}

	@Test
	void anAttemptSentBeforeTheDrainCutWithNoOutcomeStaysUnresolved() throws Exception {
		final var run = started(stub.listeners(1), nics(1), 1);
		final Operation<DataItem> op = dataOp(OpType.CREATE, "cut");
		stub.hold();

		assertTrue(run.driver().put(op));
		waitFor(() -> stub.objectRequests().size() == 1, "the request reaches the stub");
		run.driver().closeAdmission();
		run.driver().operationLifecycle().expireTerminalDeadline();
		assertEquals(OperationLifecycleState.UNRESOLVED, run.driver().operationLifecycle().stateOf(op));
		stub.release();
		awaitIdle(run);

		final var rows = stoppedRows(run.driver(), run.driver());
		assertEquals(List.of(1L, 0L, 0L, 0L, 1L, 0L), totals(rows), "a late response does not settle it: " + rows);
	}

	@Test
	void aRangeAttemptWaitingOnOneNicDoesNotHoldBackTheOther() throws Exception {
		final var endpoints = stub.listeners(2);
		final var run = rangeRun(endpoints, 2);
		final var admission = run.pool().admission();
		final var unblock = blockEventLoop(run.pool(), 0);

		run.read("range-waiting");
		waitFor(() -> admission.count(0) == 1, "the first read took the first NIC");
		run.read("range-free");

		final var free = run.outcome();
		assertEquals(Operation.Status.SUCC, free.status());
		assertEquals(endpoints.get(1), free.nodeAddr(), "the second read took the other NIC");
		assertEquals(List.of(ADDRESSES.get(1)), stub.objectRequests().stream().map(NicSetLoopback.Request::source).toList());

		unblock.countDown();
		final var waiting = run.outcome();
		assertEquals(Operation.Status.SUCC, waiting.status());
		assertEquals(endpoints.get(0), waiting.nodeAddr());
		waitFor(() -> counts(admission).equals(List.of(0, 0)), "both attempts ended");
		run.close();
		assertTrue(run.runtime.snapshot().reconciled());
		assertEquals(2, run.runtime.snapshot().logical().accepted());
	}

	@Test
	void aStopWhileARangeAttemptWaitsRecoversItUnattempted() throws Exception {
		final var run = rangeRun(stub.listeners(1), 1);
		final var admission = run.pool().admission();
		final var unblock = blockEventLoop(run.pool(), 0);

		run.read("range-stopped");
		waitFor(() -> admission.count(0) == 1, "the read took the NIC and waits for its connection");
		run.runtime.closeAdmission();
		run.driver.closeAdmission();
		run.driver.recoverQueuedOperations();
		unblock.countDown();

		waitFor(() -> admission.count(0) == 0 && run.driver.activeRangeTransports() == 0, "the fenced attempt released its NIC");
		assertNull(run.pollTerminal(QUIET_MILLIS), "a recovered read publishes no outcome");
		assertTrue(stub.objectRequests().isEmpty(), () -> "requests: " + stub.objectRequests());
		assertEquals(List.of(1L, 0L, 0L, 1L, 0L, 0L), totals(stoppedRows(run.driver, run)));
		final var snapshot = run.runtime.snapshot();
		assertTrue(snapshot.reconciled(), snapshot::toString);
		assertEquals(1, snapshot.logical().unattempted());
		assertEquals(0, snapshot.requestsSent());
	}

	@Test
	void aStopWhileARangeRetryWaitsPublishesItsRetainedFailureOnce() throws Exception {
		final var run = rangeRun(stub.listeners(1), 1, true);
		final var admission = run.pool().admission();
		final var loop = channelOf(run.pool(), 0).eventLoop();
		final var unblock = new CountDownLatch(1);
		blocks.add(unblock);
		final var retried = new AtomicBoolean();
		// Queued on the loop that settles the first attempt, so it runs before the retry's connection arrives.
		run.beforeRetry = () -> {
			retried.set(true);
			loop.execute(() -> awaitQuietly(unblock));
		};
		stub.failObjectRequest = n -> true;

		run.read("range-retry");
		waitFor(() -> retried.get() && run.pool().pendingCount() == 1 && admission.count(0) == 1,
						"the retry took the NIC and waits for its connection");
		run.runtime.closeAdmission();
		run.driver.closeAdmission();
		run.runtime.closeRetries();
		assertEquals(List.of(), run.driver.recoverQueuedOperations(), "a retry is not fresh work");

		assertEquals(Operation.Status.RESP_FAIL_SVC, run.outcome().status(), "the first attempt's failure is the outcome");
		unblock.countDown();
		waitFor(() -> admission.count(0) == 0 && run.pool().pendingCount() == 0 && run.driver.activeRangeTransports() == 0
						&& !run.runtime.hasPendingResults(), "the fenced retry released its NIC");
		assertNull(run.pollTerminal(QUIET_MILLIS), "no second outcome");
		assertEquals(1, stub.objectRequests().size(), "the retry was never sent");
		assertEquals(List.of(2L, 0L, 1L, 1L, 0L, 0L), totals(stoppedRows(run.driver, run)), "the failure, then the fenced retry");
		final var snapshot = run.runtime.snapshot();
		assertTrue(snapshot.reconciled(), snapshot::toString);
		assertEquals(1, snapshot.logical().failed());
		assertEquals(1, snapshot.requestsSent());
	}

	private Run started(final List<String> endpoints, final List<String> interfaces, final int concurrency) throws Exception {
		final var driver = NicSetLoopback.driver(storage(endpoints, interfaces, concurrency), false);
		resources.add(driver);
		return NicSetLoopback.started(driver);
	}

	private RangeRun rangeRun(final List<String> endpoints, final int concurrency) throws Exception {
		return rangeRun(endpoints, concurrency, false);
	}

	private RangeRun rangeRun(final List<String> endpoints, final int concurrency, final boolean retry) throws Exception {
		stub.absentObjectSize = OBJECT_SIZE;
		final var run = new RangeRun(storage(endpoints, nics(endpoints.size()), concurrency), FIXED, OBJECT_SIZE, retry);
		resources.add(run);
		return run;
	}

	private static Config storage(final List<String> endpoints, final List<String> interfaces, final int concurrency) {
		final var storage = NicSetLoopback.storage(endpoints, interfaces, concurrency);
		storage.val("net-timeoutMilliSec", NET_TIMEOUT_MILLIS);
		return storage;
	}

	/** Blocks NIC {@code nic}'s only event loop until the returned latch counts down. */
	private CountDownLatch blockEventLoop(final NicSetConnectionPool pool, final int nic) throws Exception {
		return block(channelOf(pool, nic));
	}

	private CountDownLatch block(final Channel channel) throws InterruptedException {
		final var unblock = new CountDownLatch(1);
		final var blocked = new CountDownLatch(1);
		blocks.add(unblock);
		channel.eventLoop().execute(() -> {
			blocked.countDown();
			awaitQuietly(unblock);
		});
		assertTrue(blocked.await(5, TimeUnit.SECONDS), "the event loop runs the blocking task");
		return unblock;
	}

	private static void awaitQuietly(final CountDownLatch unblock) {
		try {
			unblock.await(NET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** A pre-opened connection of NIC {@code nic}, returned to its pool at once; it outlives the call. */
	private static Channel channelOf(final NicSetConnectionPool pool, final int nic) throws Exception {
		final var acquired = new CompletableFuture<Channel>();
		pool.acquire(nic, (channel, failure) -> {
			if (failure == null) {
				acquired.complete(channel);
			} else {
				acquired.completeExceptionally(failure);
			}
		});
		final var channel = acquired.get(5, TimeUnit.SECONDS);
		pool.release(channel);
		return channel;
	}

	private static boolean directDispatch(final Run run) throws ReflectiveOperationException {
		final var field = CoopStorageDriverBase.class.getDeclaredField("directDispatchEnabled");
		field.setAccessible(true);
		return (boolean) field.get(run.driver());
	}

	/** {@code op} was recovered unattempted, and only the {@code dispatched} operations before it were dispatched. */
	private static void assertRecoveredUnattempted(final Run run, final Operation<DataItem> op, final long dispatched) {
		final var lifecycle = run.driver().operationLifecycle();
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op));
		final var snapshot = lifecycle.snapshot();
		assertEquals(1, snapshot.unattempted());
		assertEquals(dispatched, snapshot.dispatched());
		assertEquals(0, snapshot.inFlight());
		assertEquals(0, snapshot.unresolved());
		final var counters = lifecycle.counters();
		assertEquals(0, counters.failed(), "fresh work charges no failure");
		assertTrue(counters.reconciled(), counters::toString);
	}

	private static void awaitIdle(final Run run) throws InterruptedException {
		final var admission = run.pool().admission();
		waitFor(() -> run.driver().activeOpCount() == 0 && counts(admission).stream().allMatch(count -> count == 0),
						"every attempt ended: " + counts(admission));
	}

	/** The upload parts the stub received. */
	private long parts() {
		return stub.objectRequests().stream().filter(request -> request.query() != null && request.query().contains("partNumber="))
						.count();
	}

	private long fromAddress(final int nic) {
		return stub.objectRequests().stream().filter(request -> ADDRESSES.get(nic).equals(request.source())).count();
	}

	private Set<String> sources() {
		final Set<String> sources = new HashSet<>();
		stub.objectRequests().forEach(request -> sources.add(request.source()));
		return sources;
	}

	private static List<Integer> counts(final NicAdmission admission) {
		return IntStream.range(0, admission.size()).map(admission::count).boxed().toList();
	}

	private static List<Integer> limits(final NicAdmission admission) {
		return IntStream.range(0, admission.size()).map(admission::limit).boxed().toList();
	}
}
