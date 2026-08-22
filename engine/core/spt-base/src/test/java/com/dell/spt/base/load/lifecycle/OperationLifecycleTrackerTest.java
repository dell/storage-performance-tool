package com.dell.spt.base.load.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

final class OperationLifecycleTrackerTest {

	@Test
	void attemptBeginsAtActualDispatchAndTerminalCompletionIsRecordedOnce() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("queued");

		assertTrue(tracker.generatorBuffered(op));
		assertTrue(tracker.driverQueued(op));
		assertEquals(0, tracker.snapshot().dispatched());
		assertEquals(1, tracker.snapshot().driverQueued());

		assertTrue(tracker.dispatched(op));
		assertTrue(tracker.completionStarted(op));
		assertTrue(tracker.terminal(op));
		assertFalse(tracker.terminal(op));

		final var snapshot = tracker.snapshot();
		assertEquals(1, snapshot.dispatched());
		assertEquals(0, snapshot.inFlight());
		assertEquals(1, snapshot.terminal());
		assertEquals(OperationLifecycleState.TERMINAL, op.lifecycle().state());
	}

	@Test
	void explicitDispatchBoundaryIsDistinctFromCompatibilityDispatchEvidence() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var compatibilityDispatch = operation("completion-fallback");
		final var explicitDispatch = operation("explicit-handoff");

		assertTrue(tracker.driverQueued(compatibilityDispatch));
		assertTrue(tracker.dispatched(compatibilityDispatch));
		assertFalse(tracker.hasExplicitDispatchBoundary(compatibilityDispatch));

		assertTrue(tracker.driverQueued(explicitDispatch));
		assertTrue(tracker.explicitlyDispatched(explicitDispatch));
		assertTrue(tracker.hasExplicitDispatchBoundary(explicitDispatch));
	}

	@Test
	@SuppressWarnings("unchecked")
	void compatibilityExplicitDispatchEvidenceIsScopedToOneCirculation() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);

		assertTrue(tracker.driverQueued(legacy));
		assertTrue(tracker.explicitlyDispatched(legacy));
		assertTrue(tracker.hasExplicitDispatchBoundary(legacy));
		assertTrue(tracker.completionStarted(legacy));
		assertTrue(tracker.terminal(legacy));

		assertTrue(tracker.generatorBuffered(legacy));
		assertTrue(tracker.driverQueued(legacy));
		assertFalse(tracker.hasExplicitDispatchBoundary(legacy));
	}

	@Test
	@SuppressWarnings("unchecked")
	void capturedCompatibilityTokenRetainsPriorCirculationProvenanceAcrossRecycle() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);

		assertTrue(tracker.driverQueued(legacy));
		final var dispatchToken = tracker.queuedDispatchToken(legacy);
		assertTrue(tracker.explicitlyDispatched(legacy));
		assertTrue(tracker.completionStarted(legacy));
		assertTrue(tracker.terminal(legacy));

		assertTrue(tracker.generatorBuffered(legacy));
		assertTrue(tracker.driverQueued(legacy));
		assertFalse(tracker.hasExplicitDispatchBoundary(legacy));
		assertTrue(tracker.hasExplicitDispatchBoundary(dispatchToken));
		assertFalse(tracker.unresolvedSubmission(dispatchToken),
						"the prior submission token must not resolve the recycled circulation");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, tracker.stateOf(legacy));
	}

	@Test
	void recoveredAndTimedOutOperationsRemainDistinct() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var generatorBuffered = operation("generator-buffered");
		final var driverQueued = operation("driver-queued");
		final var dispatched = operation("dispatched");

		tracker.generatorBuffered(generatorBuffered);
		tracker.generatorBuffered(driverQueued);
		tracker.driverQueued(driverQueued);
		tracker.generatorBuffered(dispatched);
		tracker.driverQueued(dispatched);
		tracker.dispatched(dispatched);

		assertTrue(tracker.unattempted(generatorBuffered));
		assertTrue(tracker.unattempted(driverQueued));
		assertTrue(tracker.unresolved(dispatched));
		assertFalse(tracker.terminal(dispatched), "late completion must not replace unresolved");

		final var snapshot = tracker.snapshot();
		assertEquals(2, snapshot.unattempted());
		assertEquals(1, snapshot.unresolved());
		assertEquals(List.of(generatorBuffered, driverQueued), snapshot.unattemptedOperations());
		assertEquals(List.of(dispatched), snapshot.unresolvedOperations());
	}

	@Test
	void terminalDeadlineRejectsACompletionBeforeTheLaterDrainPhaseBegins() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("post-deadline-completion");
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		tracker.enforceTerminalDeadline(System.nanoTime() - 1);

		assertFalse(tracker.completionStarted(op));
		assertEquals(0, tracker.snapshot().terminal());
		assertEquals(1, tracker.snapshot().unresolved());
		assertEquals(OperationLifecycleState.UNRESOLVED, op.lifecycle().state());
	}

	@Test
	void terminalDeadlineWinsWhenOutputStartedBeforeButFinishedAfterTheCutoff() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("cutoff-during-output");
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		assertTrue(tracker.completionStarted(op));
		tracker.enforceTerminalDeadline(System.nanoTime() - 1);

		assertFalse(tracker.terminal(op));
		assertEquals(0, tracker.snapshot().terminal());
		assertEquals(1, tracker.snapshot().unresolved());
		assertEquals(OperationLifecycleState.UNRESOLVED, op.lifecycle().state());
	}

	@Test
	void terminalDeadlineIsRecheckedAfterWaitingForLifecycleOwnership() throws Exception {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("cutoff-while-terminal-waits");
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		assertTrue(tracker.completionStarted(op));
		final var terminalAccepted = new java.util.concurrent.atomic.AtomicReference<Boolean>();
		final Thread terminalThread;
		synchronized (op.lifecycle()) {
			terminalThread = Thread.ofPlatform().start(() -> terminalAccepted.set(tracker.terminal(op)));
			final long waitDeadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
			while (terminalThread.getState() != Thread.State.BLOCKED
							&& System.nanoTime() < waitDeadlineNanos) {
				Thread.onSpinWait();
			}
			assertEquals(Thread.State.BLOCKED, terminalThread.getState());
			tracker.enforceTerminalDeadline(System.nanoTime() - 1);
		}
		terminalThread.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(2));

		assertFalse(terminalThread.isAlive());
		assertFalse(terminalAccepted.get());
		assertEquals(0, tracker.snapshot().terminal());
		assertEquals(1, tracker.snapshot().unresolved());
	}

	@Test
	void terminalDeadlineGuardResolvesNeverCompletingDispatchedWork() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("never-completes");
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));

		assertEquals(1, tracker.expireTerminalDeadline());
		assertFalse(tracker.completionStarted(op));
		assertEquals(1, tracker.snapshot().unresolved());
	}

	@Test
	void absoluteDispatchDeadlineRejectsTransportHandoffWithoutWaitingForTheGuardThread() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("post-deadline-dispatch");
		assertTrue(tracker.driverQueued(op));
		tracker.enforceDispatchDeadline(System.nanoTime() - 1);

		assertFalse(tracker.dispatched(op));
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, op.lifecycle().state());
		assertEquals(0, tracker.snapshot().dispatched());
	}

	@Test
	void terminalWorkDoesNotAccumulateInOutstandingMemory() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final int operationCount = 4096;

		for (var i = 0; i < operationCount; i++) {
			final var op = operation("completed-" + i);
			assertTrue(tracker.driverQueued(op));
			assertTrue(tracker.dispatched(op));
			assertTrue(tracker.completionStarted(op));
			assertTrue(tracker.terminal(op));
		}

		final var snapshot = tracker.snapshot();
		assertEquals(operationCount, snapshot.dispatched());
		assertEquals(operationCount, snapshot.terminal());
		assertEquals(0, snapshot.generatorBuffered());
		assertEquals(0, snapshot.driverQueued());
		assertEquals(0, snapshot.inFlight());
		assertEquals(0, tracker.outstandingOperationCount());
		assertTrue(snapshot.unattemptedOperations().isEmpty());
		assertTrue(snapshot.unresolvedOperations().isEmpty());
	}

	@Test
	void mutableResultItemHashCannotLeaveATerminalRegistryGhost() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation("content-update-recycle");
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		assertTrue(tracker.completionStarted(op));

		final var result = op.result();
		assertSame(op.item(), result.item(), "result publication shares the item used by recycle");
		result.item().offset(17);
		assertTrue(tracker.terminal(op));

		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightOperationCount());
		assertEquals(0, tracker.resolveOutstandingAsUnresolved());
	}

	@Test
	void completionAtTheDispatchBoundaryCannotPrecedeRegistryPublication() {
		final var trackerRef = new AtomicReference<OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>>();
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>(op -> {
			assertTrue(trackerRef.get().completionStarted(op));
			assertTrue(trackerRef.get().terminal(op));
		});
		trackerRef.set(tracker);
		final var op = operation("dispatch-publication-race");
		assertTrue(tracker.driverQueued(op));

		assertTrue(tracker.dispatched(op));

		assertEquals(OperationLifecycleState.TERMINAL, op.lifecycle().state());
		assertEquals(1, tracker.snapshot().dispatched());
		assertEquals(1, tracker.snapshot().terminal());
		assertEquals(0, tracker.snapshot().inFlight());
		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightOperationCount());
	}

	@Test
	void inFlightCountTracksOnlyActuallyDispatchedWork() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var queued = operation("queued");
		final var completing = operation("completing");

		assertTrue(tracker.driverQueued(queued));
		assertTrue(tracker.driverQueued(completing));
		assertEquals(0, tracker.inFlightCount());
		assertTrue(tracker.dispatched(completing));
		assertEquals(1, tracker.inFlightCount());
		assertTrue(tracker.completionStarted(completing));
		assertEquals(1, tracker.inFlightCount());
		assertTrue(tracker.terminal(completing));
		assertEquals(0, tracker.inFlightCount());
		assertTrue(tracker.unattempted(queued));
		assertEquals(0, tracker.inFlightCount());
	}

	@Test
	void deadlineResolutionScansOnlyActuallyDispatchedOwnership() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		for (var i = 0; i < 4096; i++) {
			assertTrue(tracker.driverQueued(operation("queued-" + i)));
		}
		final var dispatched = operation("dispatched");
		assertTrue(tracker.driverQueued(dispatched));
		assertTrue(tracker.dispatched(dispatched));

		assertEquals(1, tracker.inFlightOperationCount());
		assertEquals(1, tracker.resolveOutstandingAsUnresolved());
		assertEquals(0, tracker.inFlightOperationCount());
		assertEquals(4096, tracker.driverQueuedOperations().size());
	}

	@Test
	void recycledResultUsesFreshAttemptWhileLateCallbackKeepsCompletedAttempt() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var original = operation("recycled");
		assertTrue(tracker.driverQueued(original));
		assertTrue(tracker.dispatched(original));
		assertTrue(tracker.completionStarted(original));

		final var recycled = original.result();
		final var completedAttempt = original.lifecycle();
		assertTrue(tracker.generatorBuffered(recycled),
						"the retained result may start its next circulation while publication completes");
		assertTrue(tracker.terminal(original));
		assertTrue(tracker.driverQueued(recycled));
		assertTrue(tracker.dispatched(recycled));

		assertFalse(tracker.completionStarted(original), "a duplicate old callback must stay rejected");
		assertEquals(OperationLifecycleState.TERMINAL, completedAttempt.state());
		assertEquals(OperationLifecycleState.DISPATCHED, recycled.lifecycle().state());
		assertEquals(1, tracker.snapshot().inFlight());
		assertEquals(1, tracker.snapshot().terminal());
	}

	@Test
	@SuppressWarnings("unchecked")
	void compatibilityOperationUsesExactlyOnceSidecarState() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);

		assertTrue(tracker.driverQueued(legacy));
		assertTrue(tracker.dispatched(legacy));
		assertTrue(tracker.completionStarted(legacy));
		assertTrue(tracker.terminal(legacy));
		assertFalse(tracker.completionStarted(legacy));
		assertFalse(tracker.terminal(legacy));
		final Operation<Item> timedOutLegacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);
		assertTrue(tracker.driverQueued(timedOutLegacy));
		assertTrue(tracker.dispatched(timedOutLegacy));
		assertTrue(tracker.unresolved(timedOutLegacy));
		assertFalse(tracker.dispatched(timedOutLegacy));
		assertFalse(tracker.completionStarted(timedOutLegacy));

		final var snapshot = tracker.snapshot();
		assertEquals(2, snapshot.dispatched());
		assertEquals(1, snapshot.terminal());
		assertEquals(1, snapshot.unresolved());
		assertEquals(0, snapshot.inFlight());
	}

	@Test
	void compatibilityStateKeepsDistinctValueEqualOperationIdentitiesSeparate() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final var first = new ValueEqualLegacyOperation(41, 7).operation();
		final var second = new ValueEqualLegacyOperation(41, 7).operation();

		assertTrue(first.equals(second), "the compatibility contract does not require identity equality");
		assertTrue(tracker.driverQueued(first));
		assertTrue(tracker.driverQueued(second));
		assertTrue(tracker.dispatched(first));
		assertTrue(tracker.dispatched(second));
		assertTrue(tracker.completionStarted(first));
		assertTrue(tracker.completionStarted(second));
		assertTrue(tracker.terminal(first));
		assertTrue(tracker.terminal(second));

		assertEquals(2, tracker.snapshot().terminal());
		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightOperationCount());
	}

	@Test
	void compatibilityStateSurvivesMutableHashAcrossDispatchAndCompletion() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final var legacy = new ValueEqualLegacyOperation(13, 5);
		final var op = legacy.operation();

		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		legacy.hashCodeValue(29);
		assertTrue(tracker.completionStarted(op));
		assertTrue(tracker.terminal(op));

		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightOperationCount());
		assertEquals(0, tracker.resolveOutstandingAsUnresolved());
	}

	@Test
	void compatibilityCompletionAtFallbackBoundarySeesPublishedRegistryOwnership() {
		final var trackerRef = new AtomicReference<OperationLifecycleTracker<Operation<Item>>>();
		final var tracker = new OperationLifecycleTracker<Operation<Item>>(op -> {
			assertTrue(trackerRef.get().completionStarted(op));
			assertTrue(trackerRef.get().terminal(op));
		});
		trackerRef.set(tracker);
		final var op = new ValueEqualLegacyOperation(37, 11).operation();
		assertTrue(tracker.driverQueued(op));
		final var dispatchToken = tracker.queuedDispatchToken(op);
		assertTrue(dispatchToken != null);

		assertTrue(tracker.dispatched(dispatchToken));

		assertEquals(OperationLifecycleState.TERMINAL, tracker.stateOf(op));
		assertEquals(1, tracker.snapshot().dispatched());
		assertEquals(1, tracker.snapshot().terminal());
		assertEquals(0, tracker.snapshot().inFlight());
		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightOperationCount());
	}

	@Test
	@SuppressWarnings("unchecked")
	void compatibilityOperationMayStartAnotherCirculationAfterTerminalResult() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);

		assertTrue(tracker.driverQueued(legacy));
		assertTrue(tracker.dispatched(legacy));
		assertTrue(tracker.completionStarted(legacy));
		assertTrue(tracker.terminal(legacy));

		assertTrue(tracker.generatorBuffered(legacy));
		assertTrue(tracker.driverQueued(legacy));
		assertTrue(tracker.dispatched(legacy));
		assertEquals(2, tracker.snapshot().dispatched());
		assertEquals(1, tracker.inFlightCount());
	}

	@Test
	@SuppressWarnings("unchecked")
	void resetKeepsLegacyTerminalGuardAgainstPriorRunCallback() {
		final var tracker = new OperationLifecycleTracker<Operation<Item>>();
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);

		assertTrue(tracker.driverQueued(legacy));
		assertTrue(tracker.dispatched(legacy));
		assertTrue(tracker.completionStarted(legacy));
		assertTrue(tracker.terminal(legacy));

		tracker.reset();

		assertEquals(OperationLifecycleState.TERMINAL, tracker.stateOf(legacy));
		assertFalse(tracker.dispatched(legacy));
		assertFalse(tracker.completionStarted(legacy));
		assertEquals(0, tracker.snapshot().dispatched());
		assertEquals(0, tracker.inFlightCount());
	}

	private static DataOperationImpl<DataItemImpl> operation(final String name) {
		return new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl(name, 0, 1), null, "/bucket", null, null, 0);
	}

	private static final class ValueEqualLegacyOperation implements InvocationHandler {
		private final int equalityGroup;
		private final Operation<Item> operation;
		private volatile int hashCodeValue;

		@SuppressWarnings("unchecked")
		private ValueEqualLegacyOperation(final int hashCodeValue, final int equalityGroup) {
			this.hashCodeValue = hashCodeValue;
			this.equalityGroup = equalityGroup;
			this.operation = (Operation<Item>) Proxy.newProxyInstance(
							Operation.class.getClassLoader(), new Class<?>[]{Operation.class
							}, this);
		}

		private Operation<Item> operation() {
			return operation;
		}

		private void hashCodeValue(final int hashCodeValue) {
			this.hashCodeValue = hashCodeValue;
		}

		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args) {
			return switch (method.getName()) {
			case "lifecycle", "startNextLifecycle" -> OperationLifecycle.untracked();
			case "hashCode" -> hashCodeValue;
			case "equals" -> args != null
							&& args.length == 1
							&& args[0] != null
							&& Proxy.isProxyClass(args[0].getClass())
							&& Proxy.getInvocationHandler(args[0]) instanceof ValueEqualLegacyOperation other
							&& equalityGroup == other.equalityGroup;
			case "toString" -> "legacy-operation-" + equalityGroup;
			default -> throw new UnsupportedOperationException(method.getName());
			};
		}
	}
}
