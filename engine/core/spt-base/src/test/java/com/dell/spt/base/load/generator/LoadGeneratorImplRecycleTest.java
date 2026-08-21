package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.io.StorageItemInput;
import com.dell.spt.base.item.io.TerminalItemInputException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationAssembler;
import com.dell.spt.base.item.op.OperationAssemblyResult;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Characterization tests for the recycle-poll path in {@link LoadGeneratorImpl#doWork()}.
 * <p>
 * These tests document the current behavior of the spin-wait, yield, and output
 * logic in the shared recycle-queue branch.
 */
@SuppressWarnings("unchecked")
class LoadGeneratorImplRecycleTest {

	private static final int BATCH_SIZE = 4;

	private Input<DataItem> itemInput;
	private OperationsBuilder<DataItem, DataOperation<DataItem>> opsBuilder;
	private CollectingOutput<DataOperation<DataItem>> output;
	private LoadGeneratorImpl<DataItem, DataOperation<DataItem>> generator;

	@BeforeEach
	void setUp() throws Exception {
		// Default mock: get(List,int) returns 0 and doesn't populate the list,
		// so items stays empty and itemInputFinishFlag is set on first doWork().
		itemInput = mock(Input.class);
		when(itemInput.toString()).thenReturn("EmptyInput");

		opsBuilder = mock(OperationsBuilder.class);
		when(opsBuilder.opType()).thenReturn(OpType.READ);
		when(opsBuilder.originIndex()).thenReturn(0);

		output = new CollectingOutput<>();

		generator = new LoadGeneratorImpl<>(
						itemInput,
						opsBuilder,
						List.of(),
						output,
						BATCH_SIZE,
						0, // countLimit -> Long.MAX_VALUE
						1000, // recycleQueueCapacity
						true, // recycleFlag
						false); // shuffleFlag
	}

	@AfterEach
	void tearDown() {
		generator.close();
	}

	/**
	 * Exhaust item input, then recycle one op, then call doWork() again.
	 * The recycled op should appear in the output via the single-op path.
	 */
	@Test
	void recycleEnqueueThenDoWorkOutputsOp() throws Exception {
		// First doWork: exhausts item input, sets itemInputFinishFlag
		generator.doWork();
		assertTrue(generator.isItemInputFinished());
		assertEquals(0, output.received.size());

		// Recycle one op
		final DataOperation<DataItem> op = newOp("item-1");
		generator.recycle(op);
		assertFalse(generator.isNothingToRecycle());

		// Second doWork: enters recycle branch, picks up op, outputs it
		generator.doWork();
		assertEquals(1, output.received.size());
		assertSame(op, output.received.get(0));
		assertTrue(generator.isNothingToRecycle());
		assertEquals(1, generator.generatedOpCount());
	}

	/**
	 * With empty recycle queue, doWork() should return promptly via the
	 * spin-wait + yield path. This verifies the yield doesn't block
	 * indefinitely.
	 */
	@Test
	void emptyRecycleQueueDoWorkReturnsPromptly() throws Exception {
		// Exhaust item input
		generator.doWork();
		assertTrue(generator.isItemInputFinished());

		// doWork with empty recycle queue -- should return quickly
		final long start = System.nanoTime();
		generator.doWork();
		final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

		// Spin-wait (128x ~10ns) + yield is well under 100ms
		assertTrue(elapsedMs < 100, "doWork took " + elapsedMs + "ms; expected < 100ms");
		assertEquals(0, output.received.size());
	}

	/**
	 * Recycle multiple ops and verify a single doWork() picks up all of them
	 * (up to batchSize) and outputs them as a batch.
	 */
	@Test
	void batchRecycleOutputsAllOps() throws Exception {
		// Exhaust item input
		generator.doWork();

		// Recycle BATCH_SIZE ops
		final List<DataOperation<DataItem>> ops = new ArrayList<>();
		for (int i = 0; i < BATCH_SIZE; i++) {
			final DataOperation<DataItem> op = newOp("item-" + i);
			ops.add(op);
			generator.recycle(op);
		}

		// Single doWork should pick up and output all of them
		generator.doWork();
		assertEquals(BATCH_SIZE, output.received.size());
		for (int i = 0; i < BATCH_SIZE; i++) {
			assertSame(ops.get(i), output.received.get(i));
		}
		assertEquals(BATCH_SIZE, generator.generatedOpCount());
	}

	@Test
	void fullInputReadCanEmitOneRequestOperation() throws Exception {
		final var originIndex = 7;
		final var inputItems = List.<DataItem> of(
						new DataItemImpl("item-0", 0, 1024),
						new DataItemImpl("item-1", 0, 1024),
						new DataItemImpl("item-2", 0, 1024),
						new DataItemImpl("item-3", 0, 1024));
		final var fullInput = fixedBatchInput("FullInput", inputItems);

		final var assemblerCloseCount = new AtomicInteger();
		final var assembler = new OperationAssembler<DataItem, DataOperation<DataItem>>() {
			@Override
			public int originIndex() {
				return originIndex;
			}

			@Override
			public OpType opType() {
				return OpType.DELETE;
			}

			@Override
			public OperationAssemblyResult assemble(
							final List<DataItem> items, final List<DataOperation<DataItem>> operations) {
				assertEquals(inputItems, items);
				operations.add(new DataOperationImpl<>(
								originIndex, OpType.DELETE, items.get(0), "/bucket", null, null, List.of(), 0));
				return new OperationAssemblyResult(items.size(), operations.size());
			}

			@Override
			public void close() {
				assemblerCloseCount.incrementAndGet();
			}
		};
		final var throttleIndexes = new ArrayList<Integer>();
		final var throttlePermits = new ArrayList<Integer>();
		final var throttle = new com.github.akurilov.commons.concurrent.throttle.IndexThrottle() {
			@Override
			public boolean tryAcquire(final int index) {
				return true;
			}

			@Override
			public int tryAcquire(final int index, final int n) {
				throttleIndexes.add(index);
				throttlePermits.add(n);
				return n;
			}
		};
		final var assembledOutput = new CollectingOutput<DataOperation<DataItem>>();
		final var assembledGenerator = new LoadGeneratorImpl<>(
						fullInput,
						assembler,
						List.of(throttle),
						assembledOutput,
						BATCH_SIZE,
						0,
						1000,
						false,
						false);

		try {
			assembledGenerator.start();
			assertTrue(assembledGenerator.await(5, TimeUnit.SECONDS), "generator should complete");
			assertEquals(inputItems.size(), assembledGenerator.consumedItemCount());
			assertEquals(1, assembledGenerator.generatedOpCount());
			assertEquals(List.of(originIndex), throttleIndexes);
			assertEquals(List.of(1), throttlePermits);
			assertEquals(1, assembledOutput.received.size());
			assertEquals(OpType.DELETE, assembledOutput.received.get(0).type());
			assertEquals(originIndex, assembledOutput.received.get(0).originIndex());
		} finally {
			assembledGenerator.close();
		}
		assertEquals(1, assemblerCloseCount.get());
		verify(fullInput).close();
	}

	@Test
	void assembledOutputRangesStayWithinBufferAcrossPartialWritesAndBackpressure() throws Exception {
		final var inputItems = List.<DataItem> of(
						new DataItemImpl("item-0", 0, 1024),
						new DataItemImpl("item-1", 0, 1024),
						new DataItemImpl("item-2", 0, 1024),
						new DataItemImpl("item-3", 0, 1024));
		final var fullInput = fixedBatchInput("FullInput", inputItems);
		final var assembler = new OperationAssembler<DataItem, DataOperation<DataItem>>() {
			@Override
			public int originIndex() {
				return 0;
			}

			@Override
			public OpType opType() {
				return OpType.DELETE;
			}

			@Override
			public OperationAssemblyResult assemble(
							final List<DataItem> items, final List<DataOperation<DataItem>> operations) {
				operations.addAll(items.subList(0, 3).stream().<DataOperation<DataItem>> map(item -> new DataOperationImpl<>(
								0, OpType.DELETE, item, "/bucket", null, null, List.of(), 0)).toList());
				return new OperationAssemblyResult(items.size(), operations.size());
			}

			@Override
			public void close() {}
		};
		final var partialOutput = new PartialCollectingOutput<DataOperation<DataItem>>();
		final var assembledGenerator = new LoadGeneratorImpl<>(
						fullInput, assembler, List.of(), partialOutput, BATCH_SIZE, 0, 1000, false, false);
		try {
			assembledGenerator.doWork();
			assembledGenerator.doWork();
			assembledGenerator.doWork();

			assertEquals(List.of(3, 2, 2), partialOutput.requestedCounts);
			assertEquals(List.of(3, 2, 2), partialOutput.bufferSizes);
			assertEquals(
							List.of("item-0", "item-1", "item-2"),
							partialOutput.received.stream().map(op -> op.item().name()).toList());
			assertEquals(3, assembledGenerator.generatedOpCount());
			assertEquals(4, assembledGenerator.consumedItemCount());
		} finally {
			assembledGenerator.close();
		}
	}

	@Test
	void assemblyResultRejectsNegativeCardinalities() {
		assertThrows(IllegalArgumentException.class, () -> new OperationAssemblyResult(-1, 0));
		assertThrows(IllegalArgumentException.class, () -> new OperationAssemblyResult(0, -1));
	}

	@Test
	void consumedIdentityMismatchStopsAfterOneAssemblyAttempt() throws Exception {
		assertAssemblyContractRejected(BATCH_SIZE - 1, 0, 0);
	}

	@Test
	void emittedOperationMismatchStopsAfterOneAssemblyAttempt() throws Exception {
		assertAssemblyContractRejected(BATCH_SIZE, 1, 0);
	}

	@Test
	void operationBufferCapacityOverflowStopsAfterOneAssemblyAttempt() throws Exception {
		assertAssemblyContractRejected(BATCH_SIZE, BATCH_SIZE + 1, BATCH_SIZE + 1);
	}

	@Test
	void legacyBuilderConstructorPreservesSingleItemBehaviorAndResourceOwnership() throws Exception {
		final var originIndex = 11;
		final var inputItems = List.<DataItem> of(
						new DataItemImpl("first", 0, 1024),
						new DataItemImpl("second", 0, 1024));
		final var legacyInput = fixedBatchInput("LegacyInput", inputItems);
		final var outputPaths = (Input<String>) mock(Input.class);
		when(outputPaths.get()).thenReturn("bucket-a", "bucket-b");
		final var credentials = (Input<Credential>) mock(Input.class);
		final var firstCredential = Credential.getInstance("first-uid", "first-secret");
		final var secondCredential = Credential.getInstance("second-uid", "second-secret");
		when(credentials.get()).thenReturn(firstCredential, secondCredential);
		final var legacyBuilder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(originIndex);
		legacyBuilder
						.opType(OpType.UPDATE)
						.inputPath("/source")
						.outputPathInput(outputPaths)
						.credentialInput(credentials);
		final var throttleIndexes = new ArrayList<Integer>();
		final var throttle = new com.github.akurilov.commons.concurrent.throttle.IndexThrottle() {
			@Override
			public boolean tryAcquire(final int index) {
				return true;
			}

			@Override
			public int tryAcquire(final int index, final int n) {
				throttleIndexes.add(index);
				return n;
			}
		};
		final var legacyOutput = new CollectingOutput<Operation<DataItem>>();
		final var legacyGenerator = new LoadGeneratorImpl<>(
						legacyInput,
						legacyBuilder,
						List.of(throttle),
						legacyOutput,
						BATCH_SIZE,
						0,
						1000,
						false,
						false);

		try {
			assertEquals("UpdateLegacyInput", legacyGenerator.toString());
			legacyGenerator.start();
			assertTrue(legacyGenerator.await(5, TimeUnit.SECONDS), "legacy generator should complete");
			assertEquals(2, legacyGenerator.generatedOpCount());
			assertEquals(2, legacyGenerator.consumedItemCount());
			assertEquals(List.of(originIndex), throttleIndexes);
			assertEquals(List.of("first", "second"), legacyOutput.received.stream().map(op -> op.item().name()).toList());
			assertEquals(List.of(OpType.UPDATE, OpType.UPDATE), legacyOutput.received.stream().map(Operation::type).toList());
			assertEquals(List.of(originIndex, originIndex), legacyOutput.received.stream().map(Operation::originIndex).toList());
			assertEquals(List.of("bucket-a", "bucket-b"), legacyOutput.received.stream().map(Operation::dstPath).toList());
			assertEquals(
							List.of(firstCredential, secondCredential),
							legacyOutput.received.stream().map(Operation::credential).toList());
		} finally {
			legacyGenerator.close();
		}
		verify(legacyInput).close();
		verify(outputPaths).close();
		verify(credentials).close();
	}

	/**
	 * Start the generator VT loop, recycle ops from the test thread,
	 * and verify all ops eventually reach the output.
	 */
	@Test
	void concurrentRecycleDuringRunningGenerator() throws Exception {
		final int opCount = 20;
		final ConcurrentCollectingOutput<DataOperation<DataItem>> concurrentOutput = new ConcurrentCollectingOutput<>(opCount);

		final LoadGeneratorImpl<DataItem, DataOperation<DataItem>> runningGen = new LoadGeneratorImpl<>(
						itemInput,
						opsBuilder,
						List.of(),
						concurrentOutput,
						BATCH_SIZE,
						0,
						1000,
						true,
						false);

		try {
			runningGen.start();

			// Wait for item input to be exhausted
			assertEventually(runningGen::isItemInputFinished, 2000);

			// Recycle ops from test thread
			for (int i = 0; i < opCount; i++) {
				runningGen.recycle(newOp("concurrent-" + i));
			}

			// Wait for all ops to be output
			assertTrue(
							concurrentOutput.latch.await(5, TimeUnit.SECONDS),
							"Expected " + opCount + " ops but got " + concurrentOutput.received.size());
			assertEquals(opCount, concurrentOutput.received.size());
		} finally {
			runningGen.stop();
			runningGen.await(5, TimeUnit.SECONDS);
			runningGen.close();
		}
	}

	/**
	 * Verify isNothingToRecycle() and generatedOpCount() track queue state
	 * correctly across recycle() and doWork() calls.
	 */
	@Test
	void recycleQueueStateTracking() throws Exception {
		// Initially empty
		assertTrue(generator.isNothingToRecycle());
		assertEquals(0, generator.generatedOpCount());

		// Exhaust item input
		generator.doWork();

		// Recycle 3 ops
		for (int i = 0; i < 3; i++) {
			generator.recycle(newOp("track-" + i));
		}
		assertFalse(generator.isNothingToRecycle());

		// doWork drains the queue and outputs all ops
		generator.doWork();
		assertTrue(generator.isNothingToRecycle());
		assertEquals(3, generator.generatedOpCount());
		assertEquals(3, output.received.size());
	}

	@Test
	void closingAdmissionRecoversGeneratorBufferedWorkAsUnattempted() throws Exception {
		final var tracker = new OperationLifecycleTracker<DataOperation<DataItem>>();
		generator.operationLifecycle(tracker);
		generator.doWork();
		final var buffered = newOp("buffered-at-stop");
		generator.recycle(buffered);

		generator.closeAdmission();
		final var recovered = generator.recoverBufferedOperations();

		assertEquals(List.of(buffered), recovered);
		assertEquals(OperationLifecycleState.UNATTEMPTED, buffered.lifecycle().state());
		assertEquals(1, tracker.snapshot().unattempted());
		assertTrue(generator.recoverBufferedOperations().isEmpty(), "recovery must be idempotent");

		final var lateRecycle = newOp("late-recycle");
		final var lateRetry = newOp("late-retry");
		generator.recycle(lateRecycle);
		generator.retry(lateRetry);
		assertTrue(generator.recoverBufferedOperations().isEmpty(),
						"closed admission must reject late circulation without retaining work");
		assertEquals(OperationLifecycleState.UNATTEMPTED, lateRecycle.lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, lateRetry.lifecycle().state());
		assertEquals(3, tracker.snapshot().unattempted());
	}

	@Test
	void mutableOperationHashCannotLeaveAHandedOffGeneratorGhost() throws Exception {
		final var tracker = new OperationLifecycleTracker<DataOperation<DataItem>>();
		generator.operationLifecycle(tracker);
		generator.doWork();
		final var handedOff = newOp("mutable-generator-identity");
		generator.recycle(handedOff);
		handedOff.item().offset(23);

		generator.doWork();
		assertSame(handedOff, output.received.get(0));
		assertTrue(tracker.driverQueued(handedOff), "the downstream driver now owns this identity");

		generator.closeAdmission();
		assertTrue(generator.recoverBufferedOperations().isEmpty(),
						"generator recovery must not rediscover an identity already handed to the driver");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, handedOff.lifecycle().state());
	}

	@Test
	void admissionClosureReconcilesCirculationOnBothSidesOfTheBoundary() throws Exception {
		final var tracker = new OperationLifecycleTracker<DataOperation<DataItem>>();
		generator.operationLifecycle(tracker);
		final var operations = new ArrayList<DataOperation<DataItem>>();
		for (var i = 0; i < 128; i++) {
			operations.add(newOp("closure-race-" + i));
		}
		final var admitted = new CountDownLatch(1);
		final var continueAfterClosure = new CountDownLatch(1);
		final var producer = Thread.ofVirtual().start(() -> {
			for (var i = 0; i < operations.size(); i++) {
				generator.recycle(operations.get(i));
				if (i == 31) {
					admitted.countDown();
					try {
						continueAfterClosure.await();
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}
				}
			}
		});

		assertTrue(admitted.await(5, TimeUnit.SECONDS));
		generator.closeAdmission();
		generator.recoverBufferedOperations();
		continueAfterClosure.countDown();
		producer.join(TimeUnit.SECONDS.toMillis(5));
		generator.recoverBufferedOperations();

		assertFalse(producer.isAlive());
		assertEquals(128, tracker.snapshot().unattempted());
		assertTrue(operations.stream()
						.allMatch(op -> op.lifecycle().state() == OperationLifecycleState.UNATTEMPTED));
	}

	@Test
	void terminalSingleOutputIsStickyAndNeverResubmitted() throws Exception {
		final var failure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "terminal output");
		final var terminalOutput = new TerminalOutput<DataOperation<DataItem>>(failure);
		final var terminalGenerator = new LoadGeneratorImpl<>(
						itemInput, opsBuilder, List.of(), terminalOutput, BATCH_SIZE, 0, 1000, true, false);
		try {
			terminalGenerator.doWork();
			terminalGenerator.recycle(newOp("terminal-single"));
			assertSame(failure, assertThrows(
							IntegrityTerminalException.class, terminalGenerator::doWork));
			assertSame(failure, terminalGenerator.terminalFailure());
			assertSame(failure, assertThrows(
							IntegrityTerminalException.class, terminalGenerator::doWork));
			assertEquals(1, terminalOutput.singleCalls.get());
		} finally {
			terminalGenerator.close();
		}
	}

	@Test
	void terminalBatchOutputIsStickyAndNeverResubmitted() throws Exception {
		final var failure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION, "terminal batch output");
		final var terminalOutput = new TerminalOutput<DataOperation<DataItem>>(failure);
		final var terminalGenerator = new LoadGeneratorImpl<>(
						itemInput, opsBuilder, List.of(), terminalOutput, BATCH_SIZE, 0, 1000, true, false);
		try {
			terminalGenerator.doWork();
			terminalGenerator.recycle(newOp("terminal-batch-1"));
			terminalGenerator.recycle(newOp("terminal-batch-2"));
			assertSame(failure, assertThrows(
							IntegrityTerminalException.class, terminalGenerator::doWork));
			assertSame(failure, assertThrows(
							IntegrityTerminalException.class, terminalGenerator::doWork));
			assertEquals(1, terminalOutput.batchCalls.get());
		} finally {
			terminalGenerator.close();
		}
	}

	@Test
	void typedInputFailurePropagatesWhileOrdinaryInputFailureRetainsLegacyEofBehavior() throws Exception {
		final Input<DataItem> typedInput = mock(Input.class);
		when(typedInput.toString()).thenReturn("TypedInput");
		final var failure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.INPUT, "bad LIST input");
		doThrow(failure).when(typedInput).get(anyList(), anyInt());
		final var typedGenerator = new LoadGeneratorImpl<>(
						typedInput, opsBuilder, List.of(), output, BATCH_SIZE, 0, 1000, false, false);
		try {
			assertSame(failure, assertThrows(
							IntegrityTerminalException.class, typedGenerator::doWork));
			assertSame(failure, typedGenerator.terminalFailure());
		} finally {
			typedGenerator.close();
		}

		final StorageDriver<DataItem, DataOperation<DataItem>> listingDriver = mock(StorageDriver.class);
		final var listingFailure = new TerminalItemInputException(
						"failed deterministic S3 listing", new IOException("truncated XML"));
		doThrow(listingFailure).when(listingDriver).list(
						any(), anyString(), anyString(), anyInt(), nullable(DataItem.class),
						anyInt(), any(ListOptions.class));
		final Input<DataItem> storageListingInput = new StorageItemInput<>(
						listingDriver,
						BATCH_SIZE,
						new DataItemFactoryImpl<>(),
						"/bucket",
						"prefix/",
						36);
		final var listingGenerator = new LoadGeneratorImpl<>(
						storageListingInput, opsBuilder, List.of(), output,
						BATCH_SIZE, 0, 1000, false, false);
		try {
			final var terminal = assertThrows(
							IntegrityTerminalException.class, listingGenerator::doWork);
			assertEquals(IntegrityTerminalException.Category.INPUT, terminal.category());
			assertSame(listingFailure, terminal.getCause());
			assertSame(terminal, listingGenerator.terminalFailure());
		} finally {
			listingGenerator.close();
		}

		final Input<DataItem> ordinaryInput = mock(Input.class);
		when(ordinaryInput.toString()).thenReturn("OrdinaryInput");
		doThrow(new IllegalArgumentException("legacy input failure"))
						.when(ordinaryInput).get(anyList(), anyInt());
		final var ordinaryGenerator = new LoadGeneratorImpl<>(
						ordinaryInput, opsBuilder, List.of(), output, BATCH_SIZE, 0, 1000, false, false);
		try {
			ordinaryGenerator.doWork();
			assertNull(ordinaryGenerator.terminalFailure());
			assertTrue(ordinaryGenerator.isItemInputFinished());
		} finally {
			ordinaryGenerator.close();
		}
	}

	// --- helpers ---

	private static <I> Input<I> fixedBatchInput(final String name, final List<I> items) throws Exception {
		final var input = (Input<I>) mock(Input.class);
		when(input.toString()).thenReturn(name);
		final var inputReads = new AtomicInteger();
		doAnswer(invocation -> {
			final var buffer = invocation.<List<I>> getArgument(0);
			if (inputReads.getAndIncrement() == 0) {
				buffer.addAll(items);
				return items.size();
			}
			com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new java.io.EOFException());
			return 0;
		}).when(input).get(anyList(), anyInt());
		return input;
	}

	private static void assertAssemblyContractRejected(
					final int consumedIdentityCount,
					final int reportedOperationCount,
					final int appendedOperationCount)
					throws Exception {
		final var inputItems = List.<DataItem> of(
						new DataItemImpl("item-0", 0, 1024),
						new DataItemImpl("item-1", 0, 1024),
						new DataItemImpl("item-2", 0, 1024),
						new DataItemImpl("item-3", 0, 1024));
		final var input = fixedBatchInput("InvalidAssemblyInput", inputItems);
		final var assemblyCalls = new AtomicInteger();
		final var assembler = new OperationAssembler<DataItem, DataOperation<DataItem>>() {
			@Override
			public int originIndex() {
				return 0;
			}

			@Override
			public OpType opType() {
				return OpType.DELETE;
			}

			@Override
			public OperationAssemblyResult assemble(
							final List<DataItem> items, final List<DataOperation<DataItem>> operations) {
				assemblyCalls.incrementAndGet();
				for (var i = 0; i < appendedOperationCount; i++) {
					operations.add(new DataOperationImpl<>(
									0,
									OpType.DELETE,
									items.get(i % items.size()),
									"/bucket",
									null,
									null,
									List.of(),
									0));
				}
				return new OperationAssemblyResult(consumedIdentityCount, reportedOperationCount);
			}

			@Override
			public void close() {}
		};
		final var invalidOutput = new CollectingOutput<DataOperation<DataItem>>();
		final var invalidGenerator = new LoadGeneratorImpl<>(
						input, assembler, List.of(), invalidOutput, BATCH_SIZE, 0, 1000, false, false);
		try {
			invalidGenerator.start();
			assertTrue(invalidGenerator.await(5, TimeUnit.SECONDS), "invalid assembler should stop the generator");
			assertEquals(1, assemblyCalls.get());
			assertEquals(0, invalidGenerator.generatedOpCount());
			assertEquals(0, invalidGenerator.consumedItemCount());
			assertTrue(invalidOutput.received.isEmpty());
		} finally {
			invalidGenerator.close();
		}
	}

	private DataOperation<DataItem> newOp(final String name) {
		final DataItem item = new DataItemImpl(name, 0, 1024);
		return new DataOperationImpl<>(0, OpType.READ, item, "/bucket", null, null, List.of(), 0);
	}

	private void assertEventually(
					final java.util.function.BooleanSupplier condition,
					final long timeoutMs)
					throws InterruptedException {
		final long deadline = System.currentTimeMillis() + timeoutMs;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				fail("Condition not met within " + timeoutMs + "ms");
			}
			Thread.sleep(10);
		}
	}

	private static final class TerminalOutput<O> implements Output<O> {
		private final IntegrityTerminalException failure;
		private final AtomicInteger singleCalls = new AtomicInteger();
		private final AtomicInteger batchCalls = new AtomicInteger();

		private TerminalOutput(final IntegrityTerminalException failure) {
			this.failure = failure;
		}

		@Override
		public boolean put(final O item) {
			singleCalls.incrementAndGet();
			throw failure;
		}

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			batchCalls.incrementAndGet();
			throw failure;
		}

		@Override
		public int put(final List<O> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	/** Simple collecting output for single-threaded doWork() tests. */
	private static class CollectingOutput<O> implements Output<O> {
		final List<O> received = new ArrayList<>();

		@Override
		public boolean put(final O item) {
			if (item != null) {
				received.add(item);
			}
			return true;
		}

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			for (int i = from; i < to; i++) {
				received.add(buffer.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<O> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	private static final class PartialCollectingOutput<O> extends CollectingOutput<O> {
		private final List<Integer> requestedCounts = new ArrayList<>();
		private final List<Integer> bufferSizes = new ArrayList<>();
		private int callCount;

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			requestedCounts.add(to - from);
			bufferSizes.add(buffer.size());
			if (callCount++ == 1) {
				return 0;
			}
			final var accepted = callCount == 1 ? 1 : to - from;
			for (var i = from; i < from + accepted; i++) {
				received.add(buffer.get(i));
			}
			return accepted;
		}
	}

	/** Thread-safe collecting output with a latch for concurrent tests. */
	private static class ConcurrentCollectingOutput<O> implements Output<O> {
		final List<O> received = new CopyOnWriteArrayList<>();
		final CountDownLatch latch;

		ConcurrentCollectingOutput(final int expectedCount) {
			latch = new CountDownLatch(expectedCount);
		}

		@Override
		public boolean put(final O item) {
			if (item != null) {
				received.add(item);
				latch.countDown();
			}
			return true;
		}

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			for (int i = from; i < to; i++) {
				received.add(buffer.get(i));
				latch.countDown();
			}
			return to - from;
		}

		@Override
		public int put(final List<O> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
