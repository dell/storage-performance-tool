package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.dell.spt.base.config.ConstantValueInputImpl;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** An ordinary step's items input that fails is a failed run, not the end of its items. */
class LoadGeneratorInputFailureTest {

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	@SuppressWarnings("unchecked")
	private static LoadGeneratorImpl<DataItem, Operation<DataItem>> generator(final Input<DataItem> input) {
		final var builder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(0);
		builder.opType(OpType.READ).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		return new LoadGeneratorImpl<>(input, builder, List.of(), (Output<Operation<DataItem>>) mock(Output.class), 4, 0, 4, false, false);
	}

	@Test
	@SuppressWarnings("unchecked")
	void aReadThatFailsWhileRunningEndsTheRun() throws Exception {
		final Input<DataItem> input = mock(Input.class);
		when(input.get(any(), anyInt())).thenAnswer(call -> {
			throw new IOException("corrupt slice");
		});
		final var generator = generator(input);
		try {
			generator.start();
			verify(input, timeout(5_000).atLeast(1)).get(any(), anyInt());
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (generator.terminalFailure() == null && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			final var failure = generator.terminalFailure();
			assertNotNull(failure);
			assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
			assertEquals("failed to read the next items", failure.getMessage());
		} finally {
			generator.close();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void aReadThatFailsAfterAStopIsTheEndOfTheItems() throws Exception {
		final Input<DataItem> input = mock(Input.class);
		when(input.get(any(), anyInt())).thenAnswer(call -> {
			throw new IOException("stream closed");
		});
		final var generator = generator(input);
		try {
			// Never started, as one stopped under its read: the failure must not become terminal.
			generator.doWork();
			assertNull(generator.terminalFailure());
			verify(input, atLeast(1)).get(any(), anyInt());
			assertTrue(generator.generatedOpCount() == 0);
		} finally {
			generator.close();
		}
	}
}
