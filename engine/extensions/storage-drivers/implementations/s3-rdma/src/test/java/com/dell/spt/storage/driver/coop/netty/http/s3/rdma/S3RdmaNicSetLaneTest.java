package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.BUCKET;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.NICS;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.SIZE;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.closedRows;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.op;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.rdma;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.totals;
import static com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.waitFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.storage.driver.coop.netty.http.s3.rdma.RdmaNicLoopback.Run;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.github.akurilov.confuse.Config;
import io.netty.channel.Channel;
import io.netty.channel.epoll.Epoll;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Preparation Lanes of the real S3-RDMA driver: each NIC prepares its own attempts, what a stop does
 * to an attempt at each point it can wait, and how the lanes and the native contexts end.
 */
@Timeout(90)
final class S3RdmaNicSetLaneTest {

	private final RdmaNicLoopback stub = new RdmaNicLoopback();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final List<CountDownLatch> holds = new ArrayList<>();

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "a NIC set needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		holds.forEach(CountDownLatch::countDown);
		stub.release();
		for (var i = resources.size() - 1; i >= 0; i--) {
			resources.get(i).close();
		}
		stub.close();
	}

	// ---------- the lanes ----------

	@Test
	void eachNicPreparesItsAttemptsOnItsOwnLaneWithItsOwnCopier() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 3), stub.listeners(2), NICS.subList(0, 2), 2));
		final Set<String> firstThreads = new CopyOnWriteArraySet<>();
		final Set<String> secondThreads = new CopyOnWriteArraySet<>();
		stub.transport(0).setBeforeRegister(() -> firstThreads.add(Thread.currentThread().getName()));
		stub.transport(0).setBeforeToken(() -> firstThreads.add(Thread.currentThread().getName()));
		stub.transport(1).setBeforeRegister(() -> secondThreads.add(Thread.currentThread().getName()));
		stub.transport(1).setBeforeToken(() -> secondThreads.add(Thread.currentThread().getName()));

		final int size = 4 << 20;
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "split-0", size)).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.CREATE, "split-1", size)).status());

		assertEquals(Set.of(RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(0))), firstThreads);
		assertEquals(Set.of(RdmaPreparationLane.threadName("rdma-nic-test", NICS.get(1))), secondThreads);
		assertNotSame(run.nic(0).copier, run.nic(1).copier);
		// A copier of 3 threads: its lane and two helpers, named after its NIC (D44: per NIC).
		for (final var nic : NICS.subList(0, 2)) {
			assertEquals(2, Thread.getAllStackTraces().keySet().stream()
							.filter(thread -> thread.isAlive() && thread.getName().startsWith("rdma-nic-test-" + nic + "-rdma-copy-")).count(), nic);
		}
		assertArrayEquals(RdmaNicLoopback.expected("split-0", size), stub.object(BUCKET + "/split-0"));
		assertArrayEquals(RdmaNicLoopback.expected("split-1", size), stub.object(BUCKET + "/split-1"));
	}

	@Test
	void aLaneDelayedInRegistrationHoldsOnlyItsNic() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));

		lanePreparesWhileOtherNicCompletes(run, entered, release);
	}

	@Test
	void aLaneDelayedInAllocationHoldsOnlyItsNic() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		run.nic(0).pool.allocator = size -> {
			enterAndAwait(entered, release);
			return ByteBuffer.allocateDirect(size);
		};

		lanePreparesWhileOtherNicCompletes(run, entered, release);
	}

	@Test
	void aLaneDelayedInACopyHoldsOnlyItsNic() throws Exception {
		final var run = started(stub.listeners(2), 2, 4);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		// The first attempt takes the first NIC, and its copy holds that lane.
		assertTrue(run.driver().put(blockedPut("held-copy", SIZE, entered, release)));
		assertTrue(entered.await(10, TimeUnit.SECONDS));
		for (var i = 0; i < 6; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "free-" + i)));
		}
		final var finished = new ArrayList<Operation<DataItem>>();
		for (var i = 0; i < 5; i++) {
			finished.add(run.results().await());
		}
		finished.forEach(result -> assertEquals(Operation.Status.SUCC, result.status()));
		assertEquals(0, stub.objectRequests().stream().filter(request -> request.sourceNic() == 0).count(), "the first NIC sent nothing");

		release.countDown();
		for (var i = 0; i < 2; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		run.awaitIdle();
	}

	private void lanePreparesWhileOtherNicCompletes(final Run run, final CountDownLatch entered, final CountDownLatch release) throws Exception {
		final var total = 10;
		for (var i = 0; i < total; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "delayed-" + i)));
		}
		assertTrue(entered.await(10, TimeUnit.SECONDS), "the first NIC's lane is held");
		// The second NIC serves everything but what waits on the first NIC's share of 2.
		for (var i = 0; i < total - 2; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(total - 2L, stub.objectRequests().stream().filter(request -> request.sourceNic() == 1).count());
		assertEquals(2, run.pool().admission().count(0), "held to its share");
		assertNull(run.results().poll(100));

		release.countDown();
		for (var i = 0; i < 2; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		run.awaitIdle();
	}

	@Test
	void withoutALimitAStalledNicHoldsItsBoundAndTheOtherGoesOn() throws Exception {
		final var run = started(stub.listeners(2), 2, 0);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));
		final int total = 30;

		for (var i = 0; i < total; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "stalled-" + i)));
		}
		assertTrue(entered.await(10, TimeUnit.SECONDS));
		final int bound = NicSetSettings.UNLIMITED_WAITING_LIMIT;
		for (var i = 0; i < total - bound; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertEquals(bound, run.pool().admission().count(0), "the stalled NIC holds its bound");
		assertEquals(0, run.pool().admission().room() - bound, "only the other NIC has room");

		release.countDown();
		for (var i = 0; i < bound; i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		run.awaitIdle();
	}

	@Test
	void aPreparationFailureFailsItsAttemptReturnsThePermitAndTheLaneGoesOn() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		stub.transport(0).setFailTokenGeneration(true);

		assertEquals(Operation.Status.FAIL_IO, run.executeAlone(op(OpType.READ, "failed")).status());

		assertEquals(0, run.pool().admission().count(0));
		assertEquals(0, run.driver().activeOpCount());
		assertFalse(run.lane(0).exited());
		stub.transport(0).setFailTokenGeneration(false);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "after")).status());
		assertEquals(1, run.nic(0).pool.idleBuffers());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(2L, 1L, 1L, 0L, 0L, (long) SIZE), totals(rows), rows::toString);
	}

	@Test
	void aLaneThatDiesFailsTheStep() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		stub.transport(0).setBeforeRegister(() -> {
			throw new InternalError("injected lane death");
		});

		assertTrue(run.driver().put(op(OpType.READ, "kills-the-lane")));

		waitFor(() -> run.driver().terminalFailure() != null, "the driver reports the failure");
		final var failure = run.driver().terminalFailure();
		assertTrue(failure.getMessage().contains(NICS.get(0)), failure.getMessage());
		assertTrue(failure.getMessage().contains("injected lane death"), failure.getMessage());
		waitFor(() -> run.lane(0).exited(), "the lane ended");
		run.awaitIdle();
		assertTrue(stub.objectRequests().isEmpty());
		// The operation is recovered with the other queued ones when the failed step stops.
		run.driver().closeAdmission();
		assertEquals(1, run.driver().recoverQueuedOperations().size());
	}

	@Test
	void httpPayloadsSkipTheLane() throws Exception {
		// Each NIC's share is 2, so the first NIC has room beside the attempt its lane holds.
		final var run = started(stub.listeners(2), 2, 4);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));
		assertTrue(run.driver().put(op(OpType.READ, "holds-the-lane")));
		assertTrue(entered.await(10, TimeUnit.SECONDS));

		// The next attempt takes the second NIC and the one after takes the first, whose lane is held.
		// A completed attempt's NIC gets its turn again, so the first NIC serves one of these.
		final var overHttp = List.of(op(OpType.CREATE, "small-0", 512), op(OpType.CREATE, "small-1", 512), op(OpType.CREATE, "small-2", 512));
		for (final var small : overHttp) {
			assertTrue(run.driver().put(small));
		}
		for (var i = 0; i < overHttp.size(); i++) {
			assertEquals(Operation.Status.SUCC, run.results().await().status());
		}
		assertTrue(stub.objectRequests().stream().anyMatch(request -> request.sourceNic() == 0), () -> "a payload went on the held NIC: " + stub.objectRequests());
		release.countDown();
		assertEquals(Operation.Status.SUCC, run.results().await().status());
	}

	// ---------- stop latched at each point an attempt can wait ----------

	@Test
	void aStopAtTheLanesDequeueRecoversEveryQueuedAttempt() throws Exception {
		final var run = started(stub.listeners(1), 1, 3);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));
		final var ops = List.of(op(OpType.READ, "preparing"), op(OpType.READ, "queued-0"), op(OpType.READ, "queued-1"));
		ops.forEach(op -> assertTrue(run.driver().put(op)));
		assertTrue(entered.await(10, TimeUnit.SECONDS));
		waitFor(() -> run.pool().admission().count(0) == 3, "one attempt prepares and two wait for the lane");

		run.driver().closeAdmission();
		assertEquals(Set.copyOf(ops), Set.copyOf(run.driver().recoverQueuedOperations()), "each waiting operation is still queued");
		release.countDown();

		run.awaitIdle();
		assertUnattempted(run, ops);
		assertTrue(stub.objectRequests().isEmpty());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(3L, 0L, 0L, 3L, 0L, 0L), totals(rows), "interrupted, never unresolved: " + rows);
		assertEquals(3, rows.rows().stream().filter(row -> row.kind() == NicPathRows.Kind.NIC).mapToLong(NicPathRows.Row::interrupted).sum());
		assertClosedOnce(0);
		assertBuffersBack(run, 0);
	}

	@Test
	void aStopDuringRegistrationWritesNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));

		stopWhileHeld(run, op(OpType.READ, "registering"), entered, release);
	}

	@Test
	void aStopDuringAllocationWritesNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		run.nic(0).pool.allocator = size -> {
			enterAndAwait(entered, release);
			return ByteBuffer.allocateDirect(size);
		};

		stopWhileHeld(run, op(OpType.READ, "allocating"), entered, release);
	}

	@Test
	void aStopDuringAOnePartCopyWritesNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		// A one-part copy of several chunks, held at its first.
		final var put = blockedPut("one-part", 3 << 20, entered, release);

		stopWhileHeld(run, put, entered, release);
	}

	@Test
	void aStopDuringASplitCopyWritesNothing() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 2), stub.listeners(1), NICS.subList(0, 1), 1));
		final var entered = new CountDownLatch(1);
		final var release = hold();
		final var put = blockedPut("split", 4 << 20, entered, release);

		stopWhileHeld(run, put, entered, release);
	}

	@Test
	void aStopBetweenPreparationAndTheWriteWritesNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeToken(() -> enterAndAwait(entered, release));

		stopWhileHeld(run, op(OpType.READ, "after-the-token"), entered, release);
	}

	private void stopWhileHeld(final Run run, final Operation<DataItem> op, final CountDownLatch entered, final CountDownLatch release)
					throws Exception {
		assertTrue(run.driver().put(op));
		assertTrue(entered.await(10, TimeUnit.SECONDS), "the lane is held");

		run.driver().closeAdmission();
		assertTrue(run.lane(0).cancelled(), "the stop cancelled the lane at once");
		assertEquals(List.of(op), run.driver().recoverQueuedOperations(), "the waiting operation is still queued");
		release.countDown();

		run.awaitIdle();
		assertUnattempted(run, List.of(op));
		assertNull(run.results().poll(100), "a recovered operation publishes no result");
		assertTrue(stub.objectRequests().isEmpty(), () -> "requests: " + stub.objectRequests());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(1L, 0L, 0L, 1L, 0L, 0L), totals(rows), rows::toString);
		assertClosedOnce(0);
		assertBuffersBack(run, 0);
	}

	@Test
	void aStopWhileTheConnectionIsOnItsWayGivesBackTheBufferAndThePermit() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var channel = channelOf(run.pool(), 0);
		final var unblock = block(channel);
		final var op = op(OpType.READ, "connection-on-its-way");

		assertTrue(run.driver().put(op));
		waitFor(() -> run.nic(0).pool.liveBuffers() == 1 && run.pool().pendingCount() == 1, "the attempt is prepared and waits for its connection");
		run.driver().closeAdmission();
		assertEquals(List.of(op), run.driver().recoverQueuedOperations());
		unblock.countDown();

		run.awaitIdle();
		assertUnattempted(run, List.of(op));
		assertTrue(stub.objectRequests().isEmpty());
		assertEquals(1, run.nic(0).pool.idleBuffers(), "nothing was sent, so its buffer is reusable");
		assertEquals(0, run.nic(0).pool.invalidReturns.sum());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(1L, 0L, 0L, 1L, 0L, 0L), totals(rows), rows::toString);
	}

	@Test
	void aStopAfterTheWriteLeavesTheRequestToFinishAndItsBufferUntilTheResponse() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var op = op(OpType.READ, "written");
		stub.hold();

		assertTrue(run.driver().put(op));
		waitFor(() -> stub.objectRequests().size() == 1, "the request reaches the stub");
		run.driver().closeAdmission();
		assertEquals(List.of(), run.driver().recoverQueuedOperations(), "the claim passed custody to the driver");
		assertEquals(OperationLifecycleState.DISPATCHED, run.driver().operationLifecycle().stateOf(op));
		assertEquals(0, run.nic(0).pool.idleBuffers(), "the server may still write it");

		stub.release();
		assertEquals(Operation.Status.SUCC, run.results().await().status());
		run.awaitIdle();
		assertEquals(1, run.nic(0).pool.idleBuffers());
		final var rows = closedRows(run.driver());
		assertEquals(List.of(1L, 1L, 0L, 0L, 0L, (long) SIZE), totals(rows), rows::toString);
	}

	@Test
	void aStaleAttemptWritesNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var channel = channelOf(run.pool(), 0);
		final var unblock = block(channel);
		final var op = op(OpType.READ, "stale");

		assertTrue(run.driver().put(op));
		waitFor(() -> run.nic(0).pool.liveBuffers() == 1 && run.pool().pendingCount() == 1, "the attempt is prepared and waits for its connection");
		run.driver().operationLifecycle().enforceDispatchDeadline(System.nanoTime());
		unblock.countDown();

		run.awaitIdle();
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, run.driver().operationLifecycle().stateOf(op));
		assertTrue(stub.objectRequests().isEmpty());
		assertEquals(1, run.nic(0).pool.idleBuffers());
		run.driver().closeAdmission();
		assertEquals(List.of(op), run.driver().recoverQueuedOperations());
		assertEquals(OperationLifecycleState.UNATTEMPTED, run.driver().operationLifecycle().stateOf(op));
	}

	@Test
	void anUnsentPartFollowsQueueRecoveryAndItsParentStaysDispatched() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		// The first part is sent, which dispatches the parent; the second part's token is held.
		stub.transport(0).setBeforeToken(() -> {
			if (stub.objectRequests().stream().anyMatch(request -> request.query() != null && request.query().contains("partNumber="))) {
				enterAndAwait(entered, release);
			}
		});
		final var upload = S3RdmaNicSetDriverTest.upload("unsent", 2048, 2);

		assertTrue(run.driver().put(upload));
		assertTrue(entered.await(10, TimeUnit.SECONDS), "the first part was sent and the second's preparation is held");
		run.driver().closeAdmission();
		final var recovered = run.driver().recoverQueuedOperations();
		release.countDown();

		run.awaitIdle();
		assertEquals(1, recovered.size(), recovered::toString);
		assertTrue(recovered.get(0) instanceof PartialOperation<?> part && part.parent() == upload);
		final var lifecycle = run.driver().operationLifecycle();
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(recovered.get(0)));
		assertEquals(OperationLifecycleState.DISPATCHED, lifecycle.stateOf(upload), "as on the default route, for the bounded drain");
		assertEquals(0, lifecycle.counters().failed(), "fresh work charges no failure");
		assertEquals(2, stub.objectRequests().size(), () -> "the initiation and the first part: " + stub.objectRequests());
		assertBuffersBack(run, 0, 1);
	}

	@Test
	void aPartNotYetStartedIsRecoveredWithItsParent() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));
		final var upload = S3RdmaNicSetDriverTest.upload("not-started", 2048, 1);

		assertTrue(run.driver().put(upload));
		assertTrue(entered.await(10, TimeUnit.SECONDS), "the initiation succeeded and the part's preparation is held");
		run.driver().closeAdmission();
		final var recovered = run.driver().recoverQueuedOperations();
		release.countDown();

		run.awaitIdle();
		assertEquals(2, recovered.size(), recovered::toString);
		assertTrue(recovered.contains(upload), "no part claimed the parent, so it is fresh work too");
		assertEquals(1, stub.objectRequests().size(), () -> "only the initiation was sent: " + stub.objectRequests());
	}

	// ---------- lane shutdown ----------

	@Test
	void allLanesAreCancelledAtOnceAndCloseTakesOneBudget() throws Exception {
		final var run = started(RdmaNicLoopback.root(rdma(false, true, 1), stub.listeners(4), NICS, 4));
		final var entered = new CountDownLatch(4);
		final var release = hold();
		IntStream.range(0, 4).forEach(i -> stub.transport(i).setBeforeRegister(() -> enterAndAwait(entered, release)));
		for (var i = 0; i < 4; i++) {
			assertTrue(run.driver().put(op(OpType.READ, "held-" + i)));
		}
		assertTrue(entered.await(10, TimeUnit.SECONDS), "every lane is held");
		run.driver().laneStopBudgetMillis = 2_000;

		final long start = System.nanoTime();
		run.driver().close();
		final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		assertTrue(elapsedMillis < 6_000, () -> "one budget, not one per NIC: " + elapsedMillis + " ms");
		IntStream.range(0, 4).forEach(i -> {
			assertTrue(run.lane(i).cancelled());
			assertEquals(0, stub.transport(i).getCloseCount(), "a held lane keeps its context open");
		});
		release.countDown();
		waitFor(() -> IntStream.range(0, 4).allMatch(i -> stub.transport(i).getCloseCount() == 1), "each lane closed its NIC");
		IntStream.range(0, 4).forEach(i -> assertFalse(stub.transport(i).wasClosedDuringACall()));
		assertEquals(0, run.driver().activeOpCount());
		assertTrue(stub.objectRequests().isEmpty());
	}

	@Test
	void aLanePastTheBudgetKeepsItsNicOpenAndClosesItWhenTheCallReturns() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		stub.transport(0).setBeforeRegister(() -> enterAndAwait(entered, release));
		run.driver().laneStopBudgetMillis = 100;
		assertTrue(run.driver().put(op(OpType.READ, "past-the-budget")));
		assertTrue(entered.await(10, TimeUnit.SECONDS));

		try (final var log = new MessageLog()) {
			run.driver().close();
			final var warning = log.only("after the 100 ms stop budget");
			assertTrue(warning.contains("NIC " + NICS.get(0)), warning);
			assertTrue(warning.contains("buffer registration"), warning);
		}

		assertEquals(0, stub.transport(0).getCloseCount(), "its context stays open under the call");
		assertFalse(run.nic(0).closed());
		assertEquals(1, stub.transport(1).getCloseCount(), "the other NIC closed normally");
		assertTrue(run.nic(1).closed());

		release.countDown();
		waitFor(() -> run.nic(0).closed() && stub.transport(0).getCloseCount() == 1, "the lane closed its NIC when the call returned");
		assertFalse(stub.transport(0).wasClosedDuringACall(), "no native handle closed while a call on it ran");
		assertEquals(0, run.nic(0).pool.liveBuffers(), "its result was discarded: " + run.nic(0).pool.summary());
		assertEquals(0, stub.transport(0).getActiveRegistrationsAtClose(), "nothing stayed registered");
		assertEquals(0, run.driver().activeOpCount());
		assertTrue(stub.objectRequests().isEmpty());
	}

	@Test
	void aRestartWhileAnOldLaneRunsGetsNewResourcesAndTheOldLanesLateCallbacksChangeNothing() throws Exception {
		final var run = started(stub.listeners(1), 1, 2);
		final var entered = new CountDownLatch(1);
		final var release = hold();
		final var oldTransport = stub.transport(0);
		oldTransport.setBeforeRegister(() -> enterAndAwait(entered, release));
		final var oldOp = op(OpType.READ, "old-attempt");
		assertTrue(run.driver().put(oldOp));
		assertTrue(entered.await(10, TimeUnit.SECONDS));
		final var old = run.driver().openRdmaResources();
		run.driver().laneStopBudgetMillis = 100;

		run.driver().stopRdma(old);
		run.driver().reopenRdma();

		final var current = run.driver().openRdmaResources();
		assertNotSame(old.nics().get(0), current.nics().get(0));
		assertNotSame(old.nics().get(0).pool, current.nics().get(0).pool);
		assertNotSame(old.lanes().get(0), current.lanes().get(0));
		final var newTransport = stub.transport(0);
		assertNotSame(oldTransport, newTransport);
		assertEquals(0, oldTransport.getCloseCount(), "the old lane still holds its context");

		assertEquals(Operation.Status.SUCC, run.execute(op(OpType.READ, "new-attempt")).status());
		assertEquals(1, stub.objectRequests().size());
		assertEquals("/bucket/new-attempt", stub.objectRequests().get(0).path());

		release.countDown();
		waitFor(() -> oldTransport.getCloseCount() == 1, "the old lane closed what it owns");
		run.awaitIdle();
		assertFalse(oldTransport.wasClosedDuringACall());
		assertEquals(1, stub.objectRequests().size(), () -> "the old lane wrote nothing: " + stub.objectRequests());
		assertEquals(0, newTransport.getCloseCount(), "the new context is untouched");
		assertTrue(newTransport.isAvailable());
		assertEquals(1, current.nics().get(0).pool.idleBuffers());
		assertNull(run.results().poll(100), "the old attempt published nothing");
	}

	// ---------- logs ----------

	@Test
	void theDataPathSummaryIsWrittenPerNicAndForTheWholeDriverLast() throws Exception {
		final var run = started(stub.listeners(2), 2, 2);
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "summary-0")).status());
		assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "summary-1")).status());

		try (final var log = new MessageLog()) {
			run.driver().close();
			final var lines = log.matching("RDMA data path summary");
			assertEquals(3, lines.size(), lines::toString);
			assertTrue(lines.get(0).startsWith("rdma-nic-test: nic=" + NICS.get(0) + ": RDMA data path summary (transport available): rdmaTransferred=1 "), lines.get(0));
			assertTrue(lines.get(1).startsWith("rdma-nic-test: nic=" + NICS.get(1) + ": RDMA data path summary (transport available): rdmaTransferred=1 "), lines.get(1));
			assertTrue(lines.get(2).startsWith("rdma-nic-test: RDMA data path summary (transport available): rdmaTransferred=2 "), lines.get(2));
			assertTrue(lines.get(2).contains("poolCreated=2"), lines.get(2));
		}
	}

	@Test
	void theNumaDiagnosticLogsEachPoolBufferOnceAtDebugOnly() throws Exception {
		final var run = started(stub.listeners(1), 1, 1);
		try (final var log = new MessageLog(Level.DEBUG)) {
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "debug-0")).status());
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "debug-1")).status());
			final var line = log.only("pool buffer address=0x");
			assertTrue(line.contains("rdma-nic-test: NIC " + NICS.get(0) + " pool buffer address=0x0"), line);
			assertTrue(line.endsWith("capacity=" + RdmaBufferPool.MIN_CLASS_BYTES), line);
		}
		try (final var log = new MessageLog()) {
			run.nic(0).pool.reclaimIdle();
			assertEquals(Operation.Status.SUCC, run.executeAlone(op(OpType.READ, "info")).status());
			assertTrue(log.matching("pool buffer address=0x").isEmpty(), "off at INFO");
		}
	}

	private static void assertUnattempted(final Run run, final List<Operation<DataItem>> ops) {
		final var lifecycle = run.driver().operationLifecycle();
		ops.forEach(op -> assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op)));
		final var snapshot = lifecycle.snapshot();
		assertEquals(ops.size(), snapshot.unattempted());
		assertEquals(0, snapshot.dispatched());
		assertEquals(0, snapshot.inFlight());
		assertEquals(0, snapshot.unresolved());
		final var counters = lifecycle.counters();
		assertEquals(0, counters.failed(), "fresh work charges no failure");
		assertTrue(counters.reconciled(), counters::toString);
	}

	private void assertClosedOnce(final int nic) {
		assertEquals(1, stub.transport(nic).getCloseCount());
		assertFalse(stub.transport(nic).wasClosedDuringACall(), "no native handle closed while a call on it ran");
	}

	private void assertBuffersBack(final Run run, final int nic) {
		assertBuffersBack(run, nic, 0);
	}

	private void assertBuffersBack(final Run run, final int nic, final int live) {
		assertEquals(0, run.nic(nic).pool.invalidReturns.sum(), "each buffer came back once");
		assertEquals(live, run.nic(nic).pool.liveBuffers(), run.nic(nic).pool::summary);
		assertEquals(live, stub.transport(nic).getActiveRegistrationCount());
	}

	private Run started(final List<String> endpoints, final int nics, final int concurrency) throws Exception {
		return started(RdmaNicLoopback.root(rdma(false, true, 1), endpoints, NICS.subList(0, nics), concurrency));
	}

	private Run started(final Config root) throws Exception {
		final var driver = stub.driver(root);
		resources.add(driver);
		return RdmaNicLoopback.started(driver);
	}

	private CountDownLatch hold() {
		final var release = new CountDownLatch(1);
		holds.add(release);
		return release;
	}

	private static void enterAndAwait(final CountDownLatch entered, final CountDownLatch release) {
		entered.countDown();
		boolean interrupted = false;
		while (true) {
			try {
				release.await();
				break;
			} catch (final InterruptedException e) {
				interrupted = true;
			}
		}
		// The call cannot be interrupted, as a JNI call or an allocation cannot.
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * A PUT whose item holds its reader at the first read, of the item or of a slice of it: a real
	 * item otherwise, so the copy goes on when released.
	 */
	@SuppressWarnings("unchecked")
	private static Operation<DataItem> blockedPut(final String name, final int size, final CountDownLatch entered, final CountDownLatch release)
					throws Exception {
		final DataItem item = spy(new DataItemImpl(name, name.hashCode() & 0xFFFF, size));
		final var held = new AtomicBoolean();
		final Function<org.mockito.invocation.InvocationOnMock, Object> read = call -> {
			if (held.compareAndSet(false, true)) {
				enterAndAwait(entered, release);
			}
			try {
				return call.callRealMethod();
			} catch (final RuntimeException | Error e) {
				throw e;
			} catch (final Throwable e) {
				throw new IllegalStateException(e);
			}
		};
		doAnswer(read::apply).when(item).read(any(ByteBuffer.class));
		doAnswer(call -> {
			final DataItem slice = spy((DataItem) call.callRealMethod());
			doAnswer(read::apply).when(slice).read(any(ByteBuffer.class));
			return slice;
		}).when(item).slice(anyLong(), anyLong());
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(0, OpType.CREATE, item, null, BUCKET, RdmaNicLoopback.CREDENTIAL, null, 0);
	}

	private CountDownLatch block(final Channel channel) throws InterruptedException {
		final var unblock = hold();
		final var blocked = new CountDownLatch(1);
		channel.eventLoop().execute(() -> enterAndAwait(blocked, unblock));
		assertTrue(blocked.await(5, TimeUnit.SECONDS), "the event loop runs the blocking task");
		return unblock;
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

	/** The formatted messages of the driver's message log while it is open. */
	private static final class MessageLog extends AbstractAppender implements AutoCloseable {

		private static final String MARKER = "s3-rdma-lane-test-marker";
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		private final Level previousLevel = logger.getLevel();

		MessageLog() {
			this(Level.INFO);
		}

		MessageLog(final Level level) {
			super("s3RdmaLaneMessageLog", null, null, true, Property.EMPTY_ARRAY);
			start();
			logger.addAppender(this);
			logger.setLevel(level);
		}

		@Override
		public void append(final LogEvent event) {
			captured.add(event.getMessage().getFormattedMessage());
		}

		List<String> matching(final String text) throws InterruptedException {
			Loggers.MSG.warn(MARKER);
			waitFor(() -> captured.contains(MARKER), "the log marker arrived");
			synchronized (captured) {
				return captured.stream().filter(message -> message.contains(text)).collect(Collectors.toList());
			}
		}

		String only(final String text) throws InterruptedException {
			final var matching = matching(text);
			assertEquals(1, matching.size(), captured::toString);
			assertNotNull(matching.get(0));
			return matching.get(0);
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			logger.setLevel(previousLevel);
			stop();
		}
	}
}
