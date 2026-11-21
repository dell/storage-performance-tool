package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsMultipartTimingTest.MultipartStub;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Multipart writes on the {@code s3-aws} driver against a loopback S3 that fails chosen part
 * requests, so that parts are really sent, refused and retried by the driver. A multipart upload
 * is counted once, by its own outcome: a part attempt takes no count-limit slot and is no failure.
 * An upload aborted after a part ran out of the driver's three retries ends with that part's
 * status, so {@code load-op-retry} retries the whole upload, from a new initiation, only when the
 * failure is transient.
 *
 * <p>The CRT client retries a 5xx answer itself, five times with a back-off, and then reports the
 * request as failed, which the driver classifies as {@code RESP_FAIL_SVC}. So one driver attempt of a
 * part answered 503 is six requests at the target and takes several seconds, and a 403, which the
 * client does not retry, is one request per driver attempt.
 *
 * <p>Each step runs one upload of three 1 KB parts at a time, so its part requests come in order.
 */
@Timeout(480)
final class S3AwsMultipartStepCountsTest {

	private static final int PART_SIZE = 1024;
	private static final int PARTS = 3;
	/** A driver attempt of a part: the first request and the CRT client's five retries of a 5xx answer. */
	private static final int CRT_REQUESTS_PER_5XX_ATTEMPT = 6;
	/** The first attempt of a part and the driver's three retries. */
	private static final int DRIVER_ATTEMPTS_PER_PART = 4;
	/** A count-limited step that hangs runs to this limit, then waits out {@link #WAIT_LIMIT_SECONDS}. */
	private static final String TIME_LIMIT = "200s";
	/**
	 * Above the slowest test, whose upload's part is answered 503 on 24 requests (about 35 s of the
	 * CRT client's back-off), with room for a loaded runner; below a hang.
	 */
	private static final long ENDS_WITHIN_MILLIS = TimeUnit.SECONDS.toMillis(150);
	/**
	 * The step's {@code load-op-wait-limit}, above {@link #ENDS_WITHIN_MILLIS}: a step that ends its
	 * count with an operation unresolved waits out this limit, so it fails {@link #assertEnded()}.
	 */
	private static final int WAIT_LIMIT_SECONDS = 160;
	private static final long RESULT_TIMEOUT_SECONDS = 60;

	private final MultipartStub stub = new MultipartStub();
	private long stepMillis;
	/** The last step's own elapsed time, from its last metrics snapshot, by operation type and concurrency limit. */
	private final Map<String, Long> stepElapsedMillis = new ConcurrentHashMap<>();

	@TempDir
	Path home;

	@AfterEach
	void stop() {
		stub.close();
	}

	/**
	 * At driver level: a part the target refuses on every attempt is sent four times, and its
	 * upload is aborted and ends with the part's {@code RESP_FAIL_AUTH}, which is not retryable.
	 */
	@Test
	void anUploadWhosePartIsRefusedForGoodEndsWithThePartsStatus() throws Exception {
		stub.failPart = n -> true;
		stub.failureStatus = 403;

		final var upload = upload("refused");

		final var parent = upload.get(upload.size() - 1);
		assertTrue(parent instanceof CompositeDataOperation<?>, upload::toString);
		assertEquals(Operation.Status.RESP_FAIL_AUTH, parent.status(), upload::toString);
		final var parts = upload.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
		assertEquals(DRIVER_ATTEMPTS_PER_PART, parts.size(), upload::toString);
		for (var i = 0; i < parts.size(); i++) {
			final var part = parts.get(i);
			assertEquals(Operation.Status.RESP_FAIL_AUTH, part.status(), part::toString);
			assertEquals(parts.get(0).partNumber(), part.partNumber(), "the same part, retried");
			assertEquals(i, part.retryCount(), part::toString);
		}
		assertEquals(DRIVER_ATTEMPTS_PER_PART, stub.partRequests.get(), "the part and its three retries, sent");
		assertRequests(1, 0, 1);
	}

	@Test
	void uploadsWhosePartsAreRefusedForGoodAreEachOneFailure() throws Exception {
		uploadsWhosePartsAreRefusedForGoodAreEachOneFailure(false);
	}

	@Test
	void uploadsWhosePartsAreRefusedForGoodAreEachOneFailureWithRetry() throws Exception {
		uploadsWhosePartsAreRefusedForGoodAreEachOneFailure(true);
	}

	/**
	 * Every part request is answered 403. Each upload's first part is sent four times, then the
	 * upload is aborted and counted once as a failure; with {@code load-op-retry} it is not started
	 * again, because a 403 is permanent.
	 */
	private void uploadsWhosePartsAreRefusedForGoodAreEachOneFailure(final boolean retry) throws Exception {
		stub.failPart = n -> true;
		stub.failureStatus = 403;

		final var outcomes = write(2, retry);

		assertEquals(List.of(0L, 2L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(2 * DRIVER_ATTEMPTS_PER_PART, stub.partRequests.get(), "each upload's first part and its three retries");
		assertRequests(2, 0, 2);
		assertEnded();
	}

	@Test
	void aPartThatFailsTransientlyOnceIsRetriedAndTakesNoCountSlot() throws Exception {
		aPartThatFailsTransientlyOnceIsRetriedAndTakesNoCountSlot(false);
	}

	@Test
	void aPartThatFailsTransientlyOnceIsRetriedAndTakesNoCountSlotWithRetry() throws Exception {
		aPartThatFailsTransientlyOnceIsRetriedAndTakesNoCountSlot(true);
	}

	/**
	 * The first upload's first part is answered 503 until the CRT client gives up on it, once. The
	 * driver sends the part again, the target accepts it, and both uploads complete and count as
	 * successes; the failed attempt takes no count slot.
	 */
	private void aPartThatFailsTransientlyOnceIsRetriedAndTakesNoCountSlot(final boolean retry) throws Exception {
		stub.failPart = n -> n <= CRT_REQUESTS_PER_5XX_ATTEMPT;
		stub.failureStatus = 503;

		final var outcomes = write(2, retry);

		assertEquals(List.of(2L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		// The failed attempt's requests, the driver's retry of that part, then every other part once.
		assertEquals(CRT_REQUESTS_PER_5XX_ATTEMPT + 2 * PARTS, stub.partRequests.get(), "one driver retry, sent");
		assertRequests(2, 2, 0);
		assertEnded();
	}

	/**
	 * Every part request of the first upload is answered 503, so its first part runs out of the
	 * driver's retries and the upload is aborted with a retryable status. {@code load-op-retry}
	 * starts the whole upload again from a new initiation, which completes, and the step counts one
	 * success.
	 */
	@Test
	void anUploadWhosePartRunsOutOfTransientRetriesIsRetriedWholeWithRetry() throws Exception {
		stub.failUpload = "upload-1"::equals;
		stub.failureStatus = 503;

		final var outcomes = write(1, true);

		assertEquals(List.of(1L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(DRIVER_ATTEMPTS_PER_PART * CRT_REQUESTS_PER_5XX_ATTEMPT + PARTS, stub.partRequests.get(),
						"the first upload's part on every attempt, then the retried upload's parts");
		assertRequests(2, 1, 1);
		assertTrue(stub.aborts.get(0).contains("uploadId=upload-1"), stub.aborts::toString);
		assertEnded();
	}

	/** Without any failure, retry on or off, the counts are those of every run before. */
	@Test
	void aStepWithoutFailuresIsUnchanged() throws Exception {
		aStepWithoutFailuresIsUnchanged(false);
	}

	@Test
	void aStepWithoutFailuresIsUnchangedWithRetry() throws Exception {
		aStepWithoutFailuresIsUnchanged(true);
	}

	private void aStepWithoutFailuresIsUnchanged(final boolean retry) throws Exception {
		final var outcomes = write(2, retry);

		assertEquals(List.of(2L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(2 * PARTS, stub.partRequests.get());
		assertRequests(2, 2, 0);
		assertEnded();
	}

	/**
	 * A recycled CREATE of one saved item: each circulation is a new upload, with its own initiation,
	 * every part and a completion under its own upload ID, and is counted once.
	 */
	@Test
	void aRecycledUploadIsANewUploadOnEachCirculation() throws Exception {
		final var outcomes = recycle(1, 3, TIME_LIMIT, 1, 0);

		assertEquals(List.of(3L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(3 * PARTS, stub.partRequests.get(), "every part of every circulation");
		assertRequests(3, 3, 0);
		assertEquals(List.of("upload-1", "upload-2", "upload-3"), stub.completedUploads);
		assertEquals(List.of(), stub.nullUploadIdRequests);
		assertEnded();
	}

	/**
	 * Four saved items recycled until the time limit, one upload open at a time: the step runs to its
	 * limit without a failure, every success is a completed upload, and the recycled uploads take an
	 * upload permit as new ones do.
	 */
	@Test
	void aTimeLimitedRecycledUploadStepRunsToItsLimitWithinTheUploadLimit() throws Exception {
		final var outcomes = recycle(4, 0, "4s", 4, 1);

		final var createOutcome = outcomes.get("CREATE-4");
		assertNotNull(createOutcome, outcomes::toString);
		assertEquals(0L, createOutcome.get(1), outcomes::toString);
		assertTrue(createOutcome.get(0) > 4, () -> "each item circulates more than once: " + outcomes);
		assertEquals(createOutcome.get(0), (long) stub.completions.get(), "a completion for each success");
		assertEquals(stub.completions.get(), stub.completedUploads.stream().distinct().count(), "each under its own upload ID");
		assertEquals(List.of(), stub.nullUploadIdRequests);
		assertEquals(1, stub.maxOpenUploads.get(), "uploads open at once");
		final long elapsedMillis = stepElapsedMillis.get("CREATE-4");
		assertTrue(elapsedMillis >= TimeUnit.SECONDS.toMillis(4), () -> "the step ended after " + elapsedMillis + " ms");
	}

	/**
	 * Runs a recycling CREATE step over {@code items} saved items of three 1 KB parts, up to {@code count}
	 * results (0: no count limit) or {@code timeLimit}, and returns its successes and failures as
	 * {@link #write} does.
	 */
	private Map<String, List<Long>> recycle(final int items, final int count, final String timeLimit,
					final int concurrency, final int multipartObjects) throws Exception {
		final var itemsFile = home.resolve("items.csv");
		final var lines = new StringBuilder();
		for (var i = 0; i < items; i++) {
			lines.append("/bucket/recycled-").append(i).append(',').append(Long.toHexString(0x7a42d9c483244167L + i))
							.append(',').append(PARTS * PART_SIZE).append(",0/\n");
		}
		Files.writeString(itemsFile, lines);
		final var scenario = """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "recycle": {"mode": true}, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": %d, "multipart": {"objects": %d}}}},
							"item": {"input": {"file": "%s"}, "data": {"size": "%dKB", "ranges": {"threshold": "%dKB"}}}
						}).run();
						""".formatted(timeLimit, count, WAIT_LIMIT_SECONDS, concurrency, multipartObjects,
						itemsFile.toString().replace("\\", "/"), PARTS, PART_SIZE / 1024);
		return run("s3-aws-multipart-recycle-", scenario);
	}

	/**
	 * Runs a step of {@code count} uploads of three 1 KB parts at concurrency 1, as the engine runs a
	 * scenario, and returns its successes and failures by operation type and concurrency limit.
	 */
	private Map<String, List<Long>> write(final int count, final boolean retry) throws Exception {
		final var scenario = """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "retry": %s, "retryLimit": 1, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}},
							"item": {"data": {"size": "%dKB", "ranges": {"threshold": "%dKB"}}}
						}).run();
						""".formatted(TIME_LIMIT, count, retry, WAIT_LIMIT_SECONDS, PARTS, PART_SIZE / 1024);
		return run("s3-aws-multipart-counts-", scenario);
	}

	/** Runs {@code scenario} as the engine runs it and returns its successes and failures by operation type and concurrency limit. */
	private Map<String, List<Long>> run(final String stepIdPrefix, final String scenario) throws Exception {
		final var stepId = stepIdPrefix + System.nanoTime();
		final var config = S3AwsMergedConfig.create(home, stub.port(), Map.of(
						"load-step-id", stepId,
						"load-step-idAutoGenerated", false,
						"item-output-path", "/bucket"));
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = Extension.load(classLoader);
		final Map<String, List<Long>> outcomes = new ConcurrentHashMap<>();
		final var started = System.nanoTime();
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			public void unregister(final MetricsContext metricsCtx) {
				super.unregister(metricsCtx);
				if (!(metricsCtx instanceof DistributedMetricsContext) && stepId.equals(metricsCtx.loadStepId())) {
					final var snapshot = metricsCtx.lastSnapshot();
					outcomes.put(metricsCtx.opType() + "-" + metricsCtx.concurrencyLimit(),
									List.of(snapshot.successSnapshot().count(), snapshot.failsSnapshot().count()));
					stepElapsedMillis.put(metricsCtx.opType() + "-" + metricsCtx.concurrencyLimit(), snapshot.elapsedTimeMillis());
				}
			}
		}) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("s3-aws multipart counts", scenario, engine, 1L).run();
		}
		stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		return outcomes;
	}

	/** Every result of one upload of {@code name} through a started driver, in order, up to the parent's last. */
	private List<Operation<?>> upload(final String name) throws Exception {
		final var results = new LinkedBlockingQueue<Operation<?>>();
		final var driver = driver();
		driver.operationResultOutput(new ResultOutput(results));
		driver.start();
		try {
			final DataItem item = new DataItemImpl(name, 0, (long) PARTS * PART_SIZE);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
			assertTrue(driver.put(new CompositeDataOperationImpl<DataItem>(
							0, OpType.CREATE, item, null, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0, PART_SIZE)));
			final List<Operation<?>> upload = new ArrayList<>();
			while (true) {
				final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
				assertNotNull(result, () -> "the upload did not finish: " + upload);
				upload.add(result);
				if (result instanceof CompositeDataOperation<?> composite
								&& (composite.allSubOperationsDone() || result.status() != Operation.Status.SUCC)) {
					return upload;
				}
			}
		} finally {
			driver.close();
		}
	}

	@SuppressWarnings("unchecked")
	private S3AwsStorageDriver<DataItem, Operation<DataItem>> driver() throws Exception {
		final var storage = S3AwsMergedConfig.create(home, stub.port(), Map.of()).configVal("storage");
		return (S3AwsStorageDriver<DataItem, Operation<DataItem>>) new S3AwsStorageDriverFactory<DataItem, Operation<DataItem>>()
						.create("s3-aws-multipart-counts",
										DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false), storage, false, 16);
	}

	private void assertRequests(final int initiations, final int completions, final int aborts) {
		assertEquals(initiations, stub.initiations.get(), "initiations");
		assertEquals(completions, stub.completions.get(), "completions");
		assertEquals(aborts, stub.aborts.size(), stub.aborts::toString);
		assertEquals(Set.of(), stub.openUploads, "no upload stays open");
	}

	/** The step ended by its count, not its time limit or a wait for an unresolved operation. */
	private void assertEnded() {
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
	}

	private static final class ResultOutput implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<?>> results;

		ResultOutput(final LinkedBlockingQueue<Operation<?>> results) {
			this.results = results;
		}

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
