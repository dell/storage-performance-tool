package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestAssembler;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DeleteRequestGeneratorTest {

	@Test
	void normalInputExhaustionFlushesOneTailAndUsesOneRequestPermit() throws Exception {
		final var throttlePermits = new ArrayList<Integer>();
		final var throttle = new com.github.akurilov.commons.concurrent.throttle.Throttle() {
			@Override
			public boolean tryAcquire() {
				return true;
			}

			@Override
			public int tryAcquire(final int count) {
				throttlePermits.add(count);
				return count;
			}
		};
		final var output = new CollectingOutput();
		final var generator = generator(input("tail-input", 4), output, List.of(throttle));

		try {
			generator.doWork();
			assertTrue(output.operations.isEmpty(), "partial tail must not dispatch before input exhaustion");
			generator.doWork();

			assertEquals(4, generator.consumedItemCount());
			assertEquals(1, generator.generatedOpCount());
			assertEquals(List.of(1), throttlePermits);
			assertEquals(1, output.operations.size());
			assertEquals(List.of("key-0", "key-1", "key-2", "key-3"),
							keys(output.operations.get(0)));
		} finally {
			generator.close();
		}
	}

	@Test
	void admissionClosureRecoversRetainedTailAsOneUnattemptedRequest() throws Exception {
		final var output = new CollectingOutput();
		final var generator = generator(input("stop-input", 3), output, List.of());
		final var lifecycle = new OperationLifecycleTracker<DeleteRequestOperation>();
		generator.operationLifecycle(lifecycle);

		try {
			generator.doWork();
			generator.closeAdmission();
			final var recovered = generator.recoverBufferedOperations();

			assertTrue(output.operations.isEmpty());
			assertEquals(1, recovered.size());
			assertEquals(List.of("key-0", "key-1", "key-2"), keys(recovered.get(0)));
			assertEquals(OperationLifecycleState.UNATTEMPTED, recovered.get(0).lifecycle().state());
			assertEquals(3, generator.consumedItemCount());
			assertEquals(1, generator.generatedOpCount());
			assertEquals(1, lifecycle.snapshot().unattempted());
		} finally {
			generator.close();
		}
	}

	@Test
	void assemblyAbortRecoversTheRetainedTailAndEveryIdentityFromTheFailedRead()
					throws Exception {
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(3) {
			private int selected;

			@Override
			public Credential nextCredential(final IntegrityManifestDataItem item)
							throws java.io.IOException {
				if (++selected == 3) {
					throw new java.io.IOException("injected builder failure");
				}
				return super.nextCredential(item);
			}
		};
		builder.opType(OpType.DELETE).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		final var output = new CollectingOutput();
		final var generator = generator(
						input("abort-input", 4), output, List.of(), builder, 2);
		final var lifecycle = new OperationLifecycleTracker<DeleteRequestOperation>();
		generator.operationLifecycle(lifecycle);

		try {
			generator.doWork();
			final var terminal = assertThrows(IntegrityTerminalException.class, generator::doWork);

			assertTrue(output.operations.isEmpty());
			assertSame(terminal, generator.terminalFailure());
			assertEquals(4, generator.consumedItemCount());
			assertEquals(1, generator.generatedOpCount());
			assertEquals(1, lifecycle.snapshot().unattempted());
			assertEquals(
							List.of("key-0", "key-1", "key-2", "key-3"),
							keys(lifecycle.snapshot().unattemptedOperations().get(0)));
		} finally {
			generator.close();
		}
	}

	@Test
	void arbitraryRuntimeAssemblyFailureIsTerminalAndRecoversTheFailedRead() throws Exception {
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(3) {
			private int selected;

			@Override
			public Credential nextCredential(final IntegrityManifestDataItem item) throws java.io.IOException {
				if (++selected == 3) {
					throw new NoSuchElementException("injected exhausted routing input");
				}
				return super.nextCredential(item);
			}
		};
		builder.opType(OpType.DELETE).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		final var generator = generator(input("runtime-abort-input", 4), new CollectingOutput(), List.of(), builder, 2);
		final var lifecycle = new OperationLifecycleTracker<DeleteRequestOperation>();
		generator.operationLifecycle(lifecycle);

		try {
			generator.doWork();
			final var terminal = assertThrows(IntegrityTerminalException.class, generator::doWork);

			assertSame(terminal, generator.terminalFailure());
			assertEquals(4, generator.consumedItemCount());
			assertEquals(List.of("key-0", "key-1", "key-2", "key-3"),
							keys(lifecycle.snapshot().unattemptedOperations().get(0)));
		} finally {
			generator.close();
		}
	}

	@Test
	void invalidTargetAbortAccountsTheInvalidIdentityAndRecoversLaterValidTargets()
					throws Exception {
		final var invalid = mock(IntegrityManifestDataItem.class);
		when(invalid.bucket()).thenReturn("bucket");
		when(invalid.name()).thenReturn("");
		when(invalid.size()).thenReturn(1L);
		final var generator = generator(
						input("invalid-target-input", List.of(item(0), invalid, item(2))),
						new CollectingOutput(),
						List.of());
		final var lifecycle = new OperationLifecycleTracker<DeleteRequestOperation>();
		generator.operationLifecycle(lifecycle);

		try {
			assertThrows(IntegrityTerminalException.class, generator::doWork);
			generator.closeAdmission();
			generator.recoverBufferedOperations();

			assertEquals(2, generator.consumedItemCount());
			assertEquals(1, generator.aggregateUnattemptedItemCount());
			assertEquals(List.of("key-0", "key-2"),
							keys(lifecycle.snapshot().unattemptedOperations().get(0)));
		} finally {
			generator.close();
		}
	}

	@Test
	void standaloneInputFailureIsTerminalInsteadOfTruncatingTheFrozenSelection() throws Exception {
		@SuppressWarnings("unchecked")
		final var failedInput = (RemainingItemCountInput<IntegrityManifestDataItem>) mock(RemainingItemCountInput.class);
		when(failedInput.toString()).thenReturn("failed-input");
		when(failedInput.remainingItemCount()).thenReturn(3L);
		doAnswer(invocation -> {
			throw new IllegalArgumentException("malformed manifest row");
		}).when(failedInput).get(anyList(), anyInt());
		final var generator = generator(failedInput, new CollectingOutput(), List.of());

		try {
			final var terminal = assertThrows(IntegrityTerminalException.class, generator::doWork);
			generator.closeAdmission();
			generator.recoverBufferedOperations();
			assertSame(terminal, generator.terminalFailure());
			assertEquals(IntegrityTerminalException.Category.INPUT, terminal.category());
			assertEquals(0, generator.consumedItemCount());
			assertEquals(3, generator.aggregateUnattemptedItemCount());
		} finally {
			generator.close();
		}
	}

	@Test
	void cancellationRecoveryDoesNotWaitForAnInterruptIgnoringInputRead() throws Exception {
		final var input = new InterruptIgnoringInput(4);
		final var generator = generator(input, new CollectingOutput(), List.of());
		final var failure = new AtomicReference<Throwable>();
		final var cancellation = new Thread(() -> {
			try {
				generator.closeAdmission();
				generator.recoverBufferedOperations();
			} catch (final Throwable t) {
				failure.set(t);
			}
		}, "standalone-delete-cancellation-test");

		try {
			generator.start();
			assertTrue(input.readStarted.await(5, TimeUnit.SECONDS));
			cancellation.start();
			cancellation.join(3_000);

			assertFalse(cancellation.isAlive(), "bounded cancellation must not invert input/admission locks");
			assertNull(failure.get());
			assertEquals(0, generator.consumedItemCount());
			assertEquals(4, generator.aggregateUnattemptedItemCount());
		} finally {
			input.release.countDown();
			cancellation.join(3_000);
			generator.close();
		}
	}

	private static LoadGeneratorImpl<IntegrityManifestDataItem, DeleteRequestOperation> generator(
					final Input<IntegrityManifestDataItem> input,
					final Output<DeleteRequestOperation> output,
					final List<Object> throttles) {
		final var builder = new OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>(3);
		builder.opType(OpType.DELETE).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		return generator(input, output, throttles, builder, 4);
	}

	private static LoadGeneratorImpl<IntegrityManifestDataItem, DeleteRequestOperation> generator(
					final Input<IntegrityManifestDataItem> input,
					final Output<DeleteRequestOperation> output,
					final List<Object> throttles,
					final OperationsBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>> builder,
					final int readBatchSize) {
		return new LoadGeneratorImpl<>(
						input,
						new DeleteRequestAssembler(builder, 100),
						throttles,
						output,
						readBatchSize,
						0,
						100,
						false,
						false);
	}

	@SuppressWarnings("unchecked")
	private static Input<IntegrityManifestDataItem> input(final String name, final int count)
					throws Exception {
		final var input = (RemainingItemCountInput<IntegrityManifestDataItem>) mock(RemainingItemCountInput.class);
		when(input.toString()).thenReturn(name);
		final var next = new AtomicInteger();
		when(input.remainingItemCount()).thenAnswer(invocation -> (long) count - next.get());
		doAnswer(invocation -> {
			final var start = next.get();
			if (start >= count) {
				throw new EOFException();
			}
			final var limit = invocation.<Integer> getArgument(1);
			final var end = Math.min(count, start + limit);
			final var target = invocation.<List<IntegrityManifestDataItem>> getArgument(0);
			for (var i = start; i < end; i++) {
				target.add(new IntegrityManifestDataItem("bucket", "key-" + i, i, null));
			}
			next.set(end);
			return end - start;
		}).when(input).get(anyList(), anyInt());
		return input;
	}

	@SuppressWarnings("unchecked")
	private static Input<IntegrityManifestDataItem> input(
					final String name, final List<IntegrityManifestDataItem> items) throws Exception {
		final var input = (RemainingItemCountInput<IntegrityManifestDataItem>) mock(RemainingItemCountInput.class);
		when(input.toString()).thenReturn(name);
		final var delivered = new AtomicInteger();
		when(input.remainingItemCount()).thenAnswer(invocation -> (long) items.size() - delivered.get());
		doAnswer(invocation -> {
			if (delivered.get() > 0) {
				throw new EOFException();
			}
			invocation.<List<IntegrityManifestDataItem>> getArgument(0).addAll(items);
			delivered.set(items.size());
			return items.size();
		}).when(input).get(anyList(), anyInt());
		return input;
	}

	private static IntegrityManifestDataItem item(final int index) {
		return new IntegrityManifestDataItem("bucket", "key-" + index, index, null);
	}

	private static List<String> keys(final DeleteRequestOperation operation) {
		return operation.deleteRequest().targets().stream().map(DeleteTarget::key).toList();
	}

	private static final class CollectingOutput implements Output<DeleteRequestOperation> {
		private final List<DeleteRequestOperation> operations = new ArrayList<>();

		@Override
		public boolean put(final DeleteRequestOperation operation) {
			operations.add(operation);
			return true;
		}

		@Override
		public int put(
						final List<DeleteRequestOperation> buffer, final int from, final int to) {
			operations.addAll(buffer.subList(from, to));
			return to - from;
		}

		@Override
		public int put(final List<DeleteRequestOperation> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	private static final class InterruptIgnoringInput
					implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final long count;
		private final CountDownLatch readStarted = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);

		private InterruptIgnoringInput(final long count) {
			this.count = count;
		}

		@Override
		public IntegrityManifestDataItem get() {
			throw new AssertionError();
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			readStarted.countDown();
			boolean interrupted = false;
			while (release.getCount() > 0) {
				try {
					release.await();
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
			if (interrupted) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new InterruptedException());
			}
			throw new AssertionError("test input must be cancelled before it is released");
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return count;
		}

		@Override
		public void reset() {}

		@Override
		public void close() {}
	}
}
