package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * A multipart operation reaches the step context as its driver publishes it: the initiation (or a
 * multi-range read's first pass), every part attempt, then the operation's own outcome. The step
 * counts the operation once, by that outcome; a part attempt is only part accounting, so a failed one
 * is neither a failure, a count-limit slot nor a retry. Under {@code load-op-retry}, a failed
 * operation is retried as a whole. Each case runs through both {@code put} forms, with and without
 * {@code load-op-retry}.
 */
final class LoadStepContextMultipartResultsTest {

	private static final int PARTS = 5;
	private static final long PART_SIZE = 1024;

	private final Config config = TestConfigBuilder.config();
	private final MetricsContext<AllMetricsSnapshot> metrics = mockMetrics();
	private final LoadGenerator<DataItem, Operation<DataItem>> generator = retryCapableGenerator();
	/** The step's count of results toward the count limit, after the last {@link #put}. */
	private long counted;
	/** Whether the next {@link #context} has {@code load-op-recycle-mode} on. */
	private boolean recycle;

	static Stream<Arguments> forms() {
		return Stream.of(
						Arguments.of(false, false),
						Arguments.of(false, true),
						Arguments.of(true, false),
						Arguments.of(true, true));
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aPartThatFailsAndSucceedsOnItsRetryCountsOnlyTheUploadsSuccess(final boolean batch, final boolean retry) {
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC).partFails(0);
		for (var part = 0; part < PARTS; part++) {
			upload.partSucceeds(part);
		}
		upload.ends(Status.SUCC);

		put(context(retry), batch, upload);

		assertOutcome(1, 0, 0);
		verify(metrics, times(PARTS)).markPartSucc(anyLong(), anyLong(), anyLong(), anyLong());
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(final boolean batch, final boolean retry) {
		// The abort's result leaves four parts unsettled.
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC).partFails(0).partFails(0).partFails(0)
						.partRunsOut(0).ends(Status.FAIL_UNKNOWN);

		put(context(retry), batch, upload);

		assertFailedOnceOrRetriedAsAWhole(retry);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void anUploadWhoseLastPartRunsOutOfRetriesIsOneFailure(final boolean batch, final boolean retry) {
		// Every part is settled when the abort's result comes.
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC);
		for (var part = 0; part < PARTS - 1; part++) {
			upload.partSucceeds(part);
		}
		upload.partFails(PARTS - 1).partFails(PARTS - 1).partFails(PARTS - 1).partRunsOut(PARTS - 1).ends(Status.FAIL_UNKNOWN);

		put(context(retry), batch, upload);

		assertFailedOnceOrRetriedAsAWhole(retry);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aFailedInitiationIsOneFailure(final boolean batch, final boolean retry) {
		// The S3 driver reports an initiation answered without an upload ID as RESP_FAIL_NOT_FOUND, which
		// is not retryable.
		final var upload = new Published(OpType.CREATE).starts(Status.RESP_FAIL_NOT_FOUND);

		put(context(retry), batch, upload);

		assertOutcome(0, 1, 0);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void anInitiationThatCannotConnectIsOneFailureOrIsRetried(final boolean batch, final boolean retry) {
		final var upload = new Published(OpType.CREATE).starts(Status.FAIL_IO);

		put(context(retry), batch, upload);

		assertFailedOnceOrRetriedAsAWhole(retry);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aFailedCompletionIsStillOneFailure(final boolean batch, final boolean retry) {
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC);
		for (var part = 0; part < PARTS; part++) {
			upload.partSucceeds(part);
		}
		upload.ends(Status.RESP_FAIL_SVC);

		put(context(retry), batch, upload);

		assertFailedOnceOrRetriedAsAWhole(retry);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aRangeThatFailsAndSucceedsOnItsRetryCountsOnlyTheReadsSuccess(final boolean batch, final boolean retry) {
		final var read = new Published(OpType.READ).starts(Status.SUCC).partFails(2);
		for (var part = 0; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.SUCC);

		put(context(retry), batch, read);

		assertOutcome(1, 0, 0);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aReadWhoseRangeRunsOutOfRetriesIsOneFailure(final boolean batch, final boolean retry) {
		// The other ranges finish, and the finalization reports the range's failure.
		final var read = new Published(OpType.READ).starts(Status.SUCC).partFails(0).partFails(0).partFails(0).partRunsOut(0);
		for (var part = 1; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.RESP_FAIL_SVC);

		put(context(retry), batch, read);

		assertFailedOnceOrRetriedAsAWhole(retry);
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void anUnfinishedUploadThatIsInterruptedOrPendingIsNotCounted(final boolean batch, final boolean retry) {
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC).ends(Status.INTERRUPTED).ends(Status.PENDING);

		put(context(retry), batch, upload);

		assertOutcome(0, 0, 0);
		verify(generator, never()).recycle(any());
	}

	@ParameterizedTest(name = "batch={0}, retry={1}")
	@MethodSource("forms")
	void aRecycledUploadStartsAgainAsANewUpload(final boolean batch, final boolean retry) {
		final var upload = new Published(OpType.CREATE).starts(Status.SUCC);
		for (var part = 0; part < PARTS; part++) {
			upload.partSucceeds(part);
		}
		upload.ends(Status.SUCC);
		recycle = true;

		put(context(retry), batch, upload);

		assertOutcome(1, 0, 0);
		@SuppressWarnings("unchecked")
		final ArgumentCaptor<Operation<DataItem>> recycled = ArgumentCaptor.forClass(Operation.class);
		verify(generator).recycle(recycled.capture());
		final var whole = assertInstanceOf(CompositeOperation.class, recycled.getValue(), "the operation, not a part, is recycled");
		// It starts again: no part settled, no upload ID, and every part again.
		assertFalse(whole.allSubOperationsDone(), "the recycled upload starts from the beginning");
		assertNull(whole.get("uploadId"));
		assertEquals(PARTS, whole.subOperations().size());
		assertFalse(whole.allSubOperationsDone(), "every part is unsettled again");
	}

	private void assertFailedOnceOrRetriedAsAWhole(final boolean retry) {
		if (!retry) {
			assertOutcome(0, 1, 0);
			return;
		}
		assertOutcome(0, 0, 1);
		@SuppressWarnings("unchecked")
		final ArgumentCaptor<Operation<DataItem>> retried = ArgumentCaptor.forClass(Operation.class);
		verify(generator).retry(retried.capture());
		final var whole = assertInstanceOf(CompositeOperation.class, retried.getValue(), "the operation, not a part, is retried");
		assertEquals(1, whole.opRetryCount());
		// It starts again: no part settled, nothing kept from its earlier attempt, and every part again.
		assertFalse(whole.allSubOperationsDone(), "the retry starts from the beginning");
		assertNull(whole.get("uploadId"));
		assertEquals(PARTS, whole.subOperations().size());
		assertFalse(whole.allSubOperationsDone(), "every part is unsettled again");
	}

	/** {@code successes} and {@code failures} counted once each, and {@code retries} whole-operation retries. */
	private void assertOutcome(final long successes, final long failures, final int retries) {
		verify(metrics, times((int) successes)).markSucc(anyLong(), anyLong(), anyLong(), anyLong());
		verify(metrics, times((int) failures)).markFail();
		verify(generator, times(retries)).retry(any());
		assertEquals(successes + failures, counted, "results counted toward the count limit");
	}

	private void put(final LoadStepContextImpl<DataItem, Operation<DataItem>> context, final boolean batch,
					final Published published) {
		if (batch) {
			assertEquals(published.results.size(), context.put(published.results, 0, published.results.size()));
		} else {
			published.results.forEach(result -> assertTrue(context.put(result)));
		}
		counted = counterResults(context);
	}

	private LoadStepContextImpl<DataItem, Operation<DataItem>> context(final boolean retry) {
		config.val("load-op-retry", retry);
		config.val("load-op-retryLimit", 2);
		config.val("load-op-recycle-mode", recycle);
		@SuppressWarnings("unchecked")
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		final var context = new LoadStepContextImpl<>(
						"multipart-results", generator, driver, metrics, config.configVal("load"), false);
		// Runs the retry at once, whatever its back-off.
		context.setRetryScheduler((delayMillis, task) -> {
			task.run();
			return CompletableFuture.completedFuture(null);
		});
		return context;
	}

	private static long counterResults(final LoadStepContextImpl<?, ?> context) {
		try {
			final var field = LoadStepContextImpl.class.getDeclaredField("counterResults");
			field.setAccessible(true);
			return ((LongAdder) field.get(context)).sum();
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private static MetricsContext<AllMetricsSnapshot> mockMetrics() {
		return mock(MetricsContext.class);
	}

	@SuppressWarnings("unchecked")
	private static LoadGenerator<DataItem, Operation<DataItem>> retryCapableGenerator() {
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		when(generator.supportsRetry()).thenReturn(true);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		return generator;
	}

	/**
	 * The results a driver publishes for one multipart operation, in order: each is the operation's or
	 * the part's result copy at that moment, with the parent's count of unsettled parts as the driver
	 * leaves it (a part attempt settles its part; a part retry unsettles it again).
	 */
	private static final class Published {

		private final CompositeDataOperationImpl<DataItem> operation;
		private final List<? extends PartialDataOperation<DataItem>> parts;
		private final List<Operation<DataItem>> results = new ArrayList<>();

		Published(final OpType type) {
			operation = new CompositeDataOperationImpl<>(0, type, new DataItemImpl("multipart", 0, PARTS * PART_SIZE), null,
							"/bucket", null, null, 0, PART_SIZE);
			parts = operation.subOperations();
		}

		/** The initiation, or a multi-range read's first pass. */
		Published starts(final Status status) {
			return publishOperation(status);
		}

		Published partSucceeds(final int part) {
			return publishPart(part, Status.SUCC, false);
		}

		/** A part attempt fails and the driver retries the part. */
		Published partFails(final int part) {
			return publishPart(part, Status.RESP_FAIL_SVC, true);
		}

		/** A part's last attempt fails. */
		Published partRunsOut(final int part) {
			return publishPart(part, Status.RESP_FAIL_SVC, false);
		}

		/** The completion, the abort or a multi-range read's finalization. */
		Published ends(final Status status) {
			return publishOperation(status);
		}

		private Published publishOperation(final Status status) {
			operation.status(status);
			results.add(operation.result());
			return this;
		}

		@SuppressWarnings("unchecked")
		private Published publishPart(final int index, final Status status, final boolean retried) {
			final var part = parts.get(index);
			part.status(status);
			operation.markSubTaskCompleted();
			results.add((Operation<DataItem>) (Operation<?>) part.result());
			if (retried) {
				part.incrementRetryCount();
				operation.undoMarkSubTaskCompleted();
			}
			return this;
		}
	}
}
