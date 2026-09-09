package com.dell.spt.base.metrics.range;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadMetricsTest {
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, null, 1);

	private static RangeReadOperation<DataItemImpl> op(OperationLifecycleTracker<RangeReadOperation<DataItemImpl>> tracker, long size) {
		var op = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY).buildOp(new DataItemImpl("item", 0, size));
		assertTrue(tracker.driverQueued(op));
		return op;
	}

	private static RangeReadAttempt send(RangeReadOperation<DataItemImpl> op,
					OperationLifecycleTracker<RangeReadOperation<DataItemImpl>> tracker, RangeReadMetrics metrics) {
		var a = op.circulation().beginAttempt(null);
		assertTrue(tracker.dispatched(op));
		assertTrue(metrics.requestHandoff(a));
		assertFalse(metrics.requestHandoff(a));
		return a;
	}

	private static void headers(RangeReadAttempt a) {
		assertTrue(a.headers(206, Status.SUCC, List.of("bytes 0-1/2"), List.of(), List.of(), false));
	}

	@Test
	void separatesLogicalFailuresAttemptsAndBytesAcrossRetryAndDeadline() {
		var metrics = new RangeReadMetrics(POLICY);
		var tracker = OperationLifecycleTracker.<RangeReadOperation<DataItemImpl>> withDeadlineSettlement((result, owner) -> {
			boolean changed = result.circulation().settleAtDeadline(result, owner);
			if (changed) {
				metrics.recordFinal(result.circulation());
			}
			return changed;
		});
		tracker.terminalObserver(result -> metrics.recordFinal(result.circulation()));
		var success = op(tracker, 2);
		var first = send(success, tracker, metrics);
		headers(first);
		first.bodyBytes(1);
		first.transportFailure(Status.FAIL_IO);
		assertTrue(metrics.recordAttempt(success.circulation(), first));
		assertFalse(metrics.recordAttempt(success.circulation(), first));
		var retry = success.circulation().beginAttempt(first);
		metrics.requestHandoff(retry);
		headers(retry);
		retry.bodyBytes(2);
		retry.finish(true);
		assertTrue(success.circulation().complete(success, tracker, retry));
		assertFalse(metrics.recordFinal(success.circulation()));
		var local = op(tracker, 0);
		assertTrue(tracker.localFailure(local, local.lifecycle()));
		var http = op(tracker, 2);
		var ha = send(http, tracker, metrics);
		ha.headers(404, Status.RESP_FAIL_NOT_FOUND, List.of(), List.of(), List.of(), false);
		assertTrue(http.circulation().complete(http, tracker, ha));
		var validation = op(tracker, 2);
		var va = send(validation, tracker, metrics);
		headers(va);
		va.bodyBytes(3);
		assertTrue(validation.circulation().complete(validation, tracker, va));
		var transport = op(tracker, 2);
		var ta = send(transport, tracker, metrics);
		ta.transportFailure(Status.FAIL_TIMEOUT);
		assertTrue(transport.circulation().complete(transport, tracker, ta));
		var unresolved = op(tracker, 2);
		var ua = send(unresolved, tracker, metrics);
		headers(ua);
		ua.bodyBytes(1);
		assertEquals(1, tracker.expireTerminalDeadline());
		assertFalse(metrics.recordFinal(unresolved.circulation()));
		var snapshot = metrics.snapshot(tracker.counters());
		assertEquals(1, snapshot.schemaVersion());
		assertEquals(6, snapshot.logical().selected());
		assertEquals(6, snapshot.attempted());
		assertEquals(6, snapshot.requestsSent());
		assertEquals(2, snapshot.successfulBytes());
		assertEquals(1, snapshot.localSelectionErrors());
		assertEquals(1, snapshot.httpFailures());
		assertEquals(1, snapshot.responseValidationFailures());
		assertEquals(1, snapshot.transportFailures());
		assertEquals(1, snapshot.httpAttemptFailures());
		assertEquals(1, snapshot.responseValidationAttemptFailures());
		assertEquals(2, snapshot.transportAttemptFailures());
		assertEquals(4, snapshot.failedReceivedBytes());
		assertEquals(1, snapshot.unresolvedReceivedBytes());
		assertTrue(snapshot.reconciled());
	}

	private static OperationLifecycleCounters empty() {
		return new OperationLifecycleCounters(true, 0, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	@Test
	void saturationAndDerivedOverflowInvalidateExactAccounting() {
		var metrics = new RangeReadMetrics(POLICY);
		metrics.add(RangeReadMetrics.Counter.REQUESTS, Long.MAX_VALUE);
		assertFalse(metrics.snapshot(empty()).overflow());
		metrics.add(RangeReadMetrics.Counter.REQUESTS, 1);
		metrics.add(RangeReadMetrics.Counter.FAILED_BYTES, Long.MAX_VALUE);
		metrics.add(RangeReadMetrics.Counter.FAILED_BYTES, 1);
		var snapshot = metrics.snapshot(empty());
		assertEquals(Long.MAX_VALUE, snapshot.requestsSent());
		assertEquals(Long.MAX_VALUE, snapshot.failedReceivedBytes());
		assertTrue(snapshot.overflow());
		assertFalse(snapshot.reconciled());
		assertThrows(IllegalArgumentException.class, () -> metrics.add(RangeReadMetrics.Counter.REQUESTS, -1));
		var derived = new RangeReadMetrics(POLICY).snapshot(new OperationLifecycleCounters(
						true, Long.MAX_VALUE, Long.MAX_VALUE, 1, Long.MAX_VALUE, 0, 0, 0, 0, 0));
		assertEquals(Long.MAX_VALUE, derived.attempted());
		assertTrue(derived.overflow());
		assertFalse(derived.reconciled());
	}

	@Test
	void concurrentAccumulationDoesNotLoseIncrements() throws Exception {
		var metrics = new RangeReadMetrics(POLICY);
		try (var pool = Executors.newFixedThreadPool(4)) {
			List<Future<?>> pending = new ArrayList<>();
			for (int worker = 0; worker < 4; worker++) {
				pending.add(pool.submit(() -> {
					for (int i = 0; i < 10000; i++) {
						metrics.add(RangeReadMetrics.Counter.REQUESTS, 1);
					}
				}));
			}
			for (var future : pending) {
				future.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(40000, metrics.snapshot(empty()).requestsSent());
		assertFalse(metrics.snapshot(empty()).overflow());
	}

	@Test
	void negativeCategoriesCannotCancelIntoAReconciledSnapshot() {
		var invalid = new RangeReadSnapshot(1, POLICY, empty(), 0, 0, 0,
						-1, 1, 0, 0, 0, 0, 0, 0, 0, false);
		assertFalse(invalid.reconciled());
	}

	@Test
	void alreadyWrappedLogicalCountersCannotLookLikeExactRangeAccounting() {
		var snapshot = new RangeReadMetrics(POLICY).snapshot(new OperationLifecycleCounters(
						true, Long.MIN_VALUE, Long.MIN_VALUE, 0, Long.MIN_VALUE, 0, 0, 0, 0, 0));
		assertEquals(Long.MAX_VALUE, snapshot.attempted());
		assertTrue(snapshot.overflow());
		assertFalse(snapshot.reconciled());
	}

	@Test
	void canceledQueuedRetryDoesNotInventAnAdditionalFailedAttempt() {
		var metrics = new RangeReadMetrics(POLICY);
		var tracker = OperationLifecycleTracker.<RangeReadOperation<DataItemImpl>> withDeadlineSettlement((result, owner) -> {
			boolean changed = result.circulation().settleAtDeadline(result, owner);
			if (changed) {
				metrics.recordFinal(result.circulation());
			}
			return changed;
		});
		tracker.terminalObserver(result -> metrics.recordFinal(result.circulation()));
		var op = op(tracker, 2);
		var first = send(op, tracker, metrics);
		first.headers(503, Status.RESP_FAIL_SVC, List.of(), List.of(), List.of(), false);
		assertTrue(metrics.recordAttempt(op.circulation(), first));
		var queued = op.circulation().beginAttempt(first);
		assertNotNull(queued);
		assertEquals(0, tracker.expireTerminalDeadline());
		assertFalse(metrics.requestHandoff(queued));
		var snapshot = metrics.snapshot(tracker.counters());
		assertEquals(1, snapshot.requestsSent());
		assertEquals(1, snapshot.httpAttemptFailures());
		assertEquals(1, snapshot.httpFailures());
		assertEquals(0, snapshot.failedReceivedBytes());
		assertTrue(snapshot.reconciled());
	}
}
