package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A recycle step's dry check ("no load operations to recycle") must not fire while the generator
 * holds polled operations it has not yet counted as generated. Two seams cover that window: the
 * recycle queue's first empty poll after it has handed out an operation, and the generated-count
 * update itself, probed just before the count rises.
 */
@SuppressWarnings({"unchecked", "rawtypes"
})
class RecycleDryCheckWindowTest {

	private static final int BATCH_SIZE = 4;

	@ParameterizedTest(name = "output duplicates {0}")
	@ValueSource(booleans = {true, false
	})
	void contextDoesNotReportNothingToRecycleWhileTheGeneratorHoldsPolledOps(final boolean outputDuplicates)
					throws Exception {
		final var generator = newGenerator();
		final var seam = installSeams(generator);
		final LoadGenerator<DataItem, Operation<DataItem>> generatorView = mock(
						LoadGenerator.class, withSettings().defaultAnswer(delegatesTo(generator)));
		// The test drives doWork() itself; the context must not start the generator's own thread.
		doNothing().when(generatorView).start();
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		when(driver.operationLifecycle()).thenReturn(OperationLifecycleTracker.disabled());
		final MetricsContext metrics = mock(MetricsContext.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
		final Config config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("load-op-type", "read");
		config.val("load-op-retry", false);
		config.val("load-op-recycle-mode", true);
		config.val("load-op-output-duplicates", outputDuplicates);
		config.val("load-op-limit-count", 0);
		final var ctx = new LoadStepContextImpl<>(
						"recycle-dry-check-window", generatorView, driver, metrics, config.configVal("load"), false);
		try {
			ctx.start();
			generator.doWork(); // exhausts the (empty) item input
			assertTrue(generator.isItemInputFinished());

			// The single circulating operation: generated once, then its success comes back.
			final var op = newOp("item-1");
			generator.recycle(op);
			generator.doWork();
			assertEquals(1, generator.generatedOpCount());
			op.status(Operation.Status.SUCC);
			assertTrue(ctx.put(op));
			assertFalse(ctx.isDone(), "the op is back in the recycle queue");

			// The generator polls the op, then finds the queue empty while still holding it.
			seam.probe = ctx::isDone;
			generator.doWork();
			assertNotNull(seam.atEmptyPoll, "the poll seam must have run");
			assertNotNull(seam.atCountUpdate, "the count seam must have run");
			assertFalse(seam.atEmptyPoll, "the step must not end while the generator holds the only op");
			assertFalse(seam.atCountUpdate, "the step must not end before the held op counts as generated");

			assertEquals(2, generator.generatedOpCount());
			assertFalse(ctx.isDone(), "the op is in flight again");
		} finally {
			ctx.close();
			generator.close();
		}
	}

	@Test
	void generatorReportsSomethingToRecycleUntilHeldOpsCountAsGenerated() throws Exception {
		final var generator = newGenerator();
		final var seam = installSeams(generator);
		try {
			generator.doWork(); // exhausts the (empty) item input
			generator.recycle(newOp("item-1"));
			generator.recycle(newOp("item-2"));
			seam.probe = () -> !generator.isNothingToRecycle();
			generator.doWork();
			assertNotNull(seam.atEmptyPoll);
			assertNotNull(seam.atCountUpdate);
			assertTrue(seam.atEmptyPoll, "polled ops not yet counted as generated are still to recycle");
			assertTrue(seam.atCountUpdate, "ops are still to recycle until they count as generated");
			assertEquals(2, generator.generatedOpCount());
			assertTrue(generator.isNothingToRecycle(), "once counted, nothing is left to recycle");
		} finally {
			generator.close();
		}
	}

	private static LoadGeneratorImpl<DataItem, Operation<DataItem>> newGenerator() {
		final Input<DataItem> itemInput = mock(Input.class);
		when(itemInput.toString()).thenReturn("EmptyInput");
		final OperationsBuilder<DataItem, Operation<DataItem>> opsBuilder = mock(OperationsBuilder.class);
		when(opsBuilder.opType()).thenReturn(OpType.READ);
		when(opsBuilder.originIndex()).thenReturn(0);
		final Output<Operation<DataItem>> opOutput = mock(Output.class);
		when(opOutput.put(org.mockito.ArgumentMatchers.<Operation<DataItem>> any())).thenReturn(true);
		when(opOutput.put(org.mockito.ArgumentMatchers.<List<Operation<DataItem>>> any(),
						org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt()))
						.thenAnswer(call -> (int) call.getArgument(2) - (int) call.getArgument(1));
		return new LoadGeneratorImpl<>(itemInput, opsBuilder, List.of(), opOutput, BATCH_SIZE, 0, 1000, true, false);
	}

	private static DataOperationImpl<DataItem> newOp(final String name) {
		return new DataOperationImpl<>(0, OpType.READ, new DataItemImpl(name, 0, 1024), "/bucket", null, null,
						List.of(), 0);
	}

	private static Seams installSeams(final LoadGeneratorImpl<DataItem, Operation<DataItem>> generator)
					throws ReflectiveOperationException {
		final var seams = new Seams();
		final Field queueField = LoadGeneratorImpl.class.getDeclaredField("recycleQueue");
		queueField.setAccessible(true);
		assertTrue(((java.util.Queue<?>) queueField.get(generator)).isEmpty());
		queueField.set(generator, new SeamQueue(seams));
		final Field counterField = LoadGeneratorImpl.class.getDeclaredField("recycledOpCounter");
		counterField.setAccessible(true);
		assertEquals(0, ((LongAdder) counterField.get(generator)).sum());
		counterField.set(generator, new SeamCounter(seams));
		return seams;
	}

	/** The probe to run and what it returned at each seam, once each. */
	private static final class Seams {
		volatile BooleanSupplier probe;
		volatile Boolean atEmptyPoll;
		volatile Boolean atCountUpdate;
	}

	/** Runs the probe on the first empty poll after the queue has handed out an operation. */
	private static final class SeamQueue extends ConcurrentLinkedQueue<Operation<DataItem>> {
		private static final long serialVersionUID = 1L;
		private final transient Seams seams;
		private transient boolean handedOut;

		SeamQueue(final Seams seams) {
			this.seams = seams;
		}

		@Override
		public Operation<DataItem> poll() {
			final var op = super.poll();
			if (op != null) {
				handedOut = seams.probe != null;
			} else if (handedOut && seams.atEmptyPoll == null) {
				seams.atEmptyPoll = seams.probe.getAsBoolean();
			}
			return op;
		}
	}

	/** Runs the probe just before the recycled-operation count rises. */
	private static final class SeamCounter extends LongAdder {
		private static final long serialVersionUID = 1L;
		private final transient Seams seams;

		SeamCounter(final Seams seams) {
			this.seams = seams;
		}

		@Override
		public void add(final long x) {
			if (seams.probe != null && seams.atCountUpdate == null) {
				seams.atCountUpdate = seams.probe.getAsBoolean();
			}
			super.add(x);
		}
	}
}
