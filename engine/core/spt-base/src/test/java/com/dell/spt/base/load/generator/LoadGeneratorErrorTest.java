package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.collection.ListInput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** An {@link Error} from the operation output fails the step instead of being retried for ever. */
class LoadGeneratorErrorTest {

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	private static LoadGeneratorImpl<DataItem, Operation<DataItem>> generator(
					final Output<Operation<DataItem>> output, final int batchSize) {
		final var items = new ArrayList<DataItem>();
		for (var i = 0; i < 8; i++) {
			items.add(new DataItemImpl("obj-" + i, 0, 1));
		}
		final var builder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(0);
		builder.opType(OpType.READ).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		return new LoadGeneratorImpl<>(new ListInput<>(items), builder, List.of(), output, batchSize, 0, batchSize,
						false, false);
	}

	private static IntegrityTerminalException awaitTerminalFailure(final LoadGenerator<?, ?> generator) {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (generator.terminalFailure() == null && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		return generator.terminalFailure();
	}

	@Test
	@SuppressWarnings("unchecked")
	void anErrorFromABatchPutFailsTheStep() throws Exception {
		final Output<Operation<DataItem>> output = mock(Output.class);
		final var error = new LinkageError("attempted duplicate class definition");
		when(output.put(any(List.class), anyInt(), anyInt())).thenThrow(error);
		final var generator = generator(output, 4);
		try {
			generator.start();
			final var failure = awaitTerminalFailure(generator);
			assertNotNull(failure);
			assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
			assertEquals("Load generator stopped on an unexpected java.lang.LinkageError", failure.getMessage());
			assertSame(error, failure.getCause());
		} finally {
			generator.close();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void anErrorFromASinglePutFailsTheStep() throws Exception {
		final Output<Operation<DataItem>> output = mock(Output.class);
		when(output.put(any(Operation.class))).thenThrow(new LinkageError("attempted duplicate class definition"));
		final var generator = generator(output, 1);
		try {
			generator.start();
			final var failure = awaitTerminalFailure(generator);
			assertNotNull(failure);
			assertTrue(failure.getCause() instanceof LinkageError);
		} finally {
			generator.close();
		}
	}

	@Test
	void anErrorFromARetryRedispatchFailsTheStep() throws Exception {
		final var accepted = new CopyOnWriteArrayList<Operation<DataItem>>();
		final Output<Operation<DataItem>> output = new Output<>() {
			@Override
			public boolean put(final Operation<DataItem> op) {
				// The first operations may be offered one at a time; only the retry fails.
				if (accepted.size() < 4) {
					accepted.add(op);
					return true;
				}
				throw new LinkageError("attempted duplicate class definition");
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer, final int from, final int to) {
				for (var i = from; i < to; i++) {
					accepted.add(buffer.get(i));
				}
				return to - from;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer) {
				return put(buffer, 0, buffer.size());
			}

			@Override
			public Input<Operation<DataItem>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
		final var items = new ArrayList<DataItem>();
		for (var i = 0; i < 4; i++) {
			items.add(new DataItemImpl("obj-" + i, 0, 1));
		}
		final var builder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(0);
		builder.opType(OpType.READ).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		// With load-op-retry the generator stays up after its input ends, to redispatch retries.
		final var generator = new LoadGeneratorImpl<DataItem, Operation<DataItem>>(
						new ListInput<>(items), builder, List.of(), output, 4, 0, 4, false, false, true);
		try {
			generator.start();
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (accepted.size() < 4 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(4, accepted.size());
			generator.retry(accepted.get(0));
			final var failure = awaitTerminalFailure(generator);
			assertNotNull(failure);
			assertTrue(failure.getCause() instanceof LinkageError);
		} finally {
			generator.close();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void anExceptionFromABatchPutIsStillRetried() throws Exception {
		final Output<Operation<DataItem>> output = mock(Output.class);
		when(output.put(any(List.class), anyInt(), anyInt())).thenThrow(new IllegalStateException("transient"));
		final var generator = generator(output, 4);
		try {
			generator.start();
			verify(output, timeout(5_000).atLeast(3)).put(any(List.class), anyInt(), anyInt());
			assertNull(generator.terminalFailure());
		} finally {
			generator.close();
		}
	}
}
