package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The failure a composite records from its parts that run out of retries, through the driver's
 * completion path with the parts' completions in a set order: the order that a step against a
 * target can only arrange by timing.
 */
@SuppressWarnings("unchecked")
final class CompositeFailureStatusDriverTest {

	private DataInput dataInput;
	private CompositeReadFailureDriverTest.D driver;

	@BeforeEach
	void startDriver() throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		driver = new CompositeReadFailureDriverTest.D(dataInput, CompositeReadFailureDriverTest.cfg());
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(out);
	}

	@AfterEach
	void closeDriver() throws Exception {
		driver.close();
	}

	private CompositeDataOperationImpl<DataItem> composite(final OpType type) {
		final var item = new DataItemImpl("c", 0, 3072);
		item.dataInput(dataInput);
		return new CompositeDataOperationImpl<>(0, type, item, null, "/bucket", null, null, 0, 1024);
	}

	private static List<PartialDataOperationImpl<DataItem>> parts(final CompositeDataOperationImpl<DataItem> composite) {
		composite.subOperations();
		return (List<PartialDataOperationImpl<DataItem>>) (List<?>) composite.nextSubOperations(3);
	}

	/** {@code part}'s last attempt ends with {@code status} and reaches the driver's completion. */
	private void runOutOfRetries(
					final CompositeDataOperationImpl<DataItem> composite,
					final PartialDataOperationImpl<DataItem> part,
					final Operation.Status status) {
		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
		part.status(status);
		composite.markSubTaskCompleted();
		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) part));
	}

	/**
	 * Part 1 runs out of retries on 503 and the upload is aborted. Part 2 then runs out of retries on
	 * 404, as S3 answers a part of an aborted upload. The upload keeps part 1's 503, the reason it was
	 * aborted, so {@code load-op-retry} retries it.
	 */
	@Test
	void anUploadKeepsTheFailureOfItsFirstPartToRunOutOfRetries() {
		final var upload = composite(OpType.CREATE);
		final var parts = parts(upload);

		runOutOfRetries(upload, parts.get(0), Operation.Status.RESP_FAIL_SVC);
		assertEquals("true", upload.get("mpuAbort"));
		runOutOfRetries(upload, parts.get(1), Operation.Status.RESP_FAIL_NOT_FOUND);

		assertEquals(Operation.Status.RESP_FAIL_SVC.name(), upload.get("mpuFailure"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, CoopStorageDriverBase.abortedUploadStatus(upload));
	}

	/** A part's failed attempt with retries left records nothing, so a later part's failure is the first. */
	@Test
	void anUploadIgnoresAFailedAttemptThatIsRetried() {
		final var upload = composite(OpType.CREATE);
		final var parts = parts(upload);
		final var retried = parts.get(1);

		retried.status(Operation.Status.RESP_FAIL_NOT_FOUND);
		upload.markSubTaskCompleted();
		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) retried));
		assertEquals(1, retried.retryCount());
		assertNull(upload.get("mpuFailure"));
		runOutOfRetries(upload, parts.get(0), Operation.Status.RESP_FAIL_SVC);

		assertEquals(Operation.Status.RESP_FAIL_SVC, CoopStorageDriverBase.abortedUploadStatus(upload));
	}

	/** A read has nothing to abort: its other ranges finish, and it reports its last range's failure. */
	@Test
	void aRangeReadKeepsTheFailureOfItsLastRangeToRunOutOfRetries() {
		final var read = composite(OpType.READ);
		final var parts = parts(read);

		runOutOfRetries(read, parts.get(0), Operation.Status.FAIL_IO);
		runOutOfRetries(read, parts.get(1), Operation.Status.RESP_FAIL_SVC);

		assertEquals(Operation.Status.RESP_FAIL_SVC.name(), read.get("mpuFailure"));
		assertNull(read.get("mpuAbort"));
	}
}
