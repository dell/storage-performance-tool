package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsMultipartTimingTest.MultipartStub;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code Duration} and {@code Latency} meters of {@code s3-aws} multipart write steps, run as
 * the engine runs a scenario, against a loopback S3 that delays parts and completions by known
 * times. Each part and each completed upload adds one sample, and the upload's sample is its
 * completion request: no sample covers the whole upload, none is computed from a cleared start,
 * and the samples' sum fits in the step's time multiplied by its concurrency.
 */
@Timeout(300)
final class S3AwsMultipartStepTimingTest {

	private static final int PART_SIZE = 1024;
	private static final int PARTS = 3;
	private static final int UPLOADS = 4;
	private static final int CONCURRENCY = 2;
	private static final long PART_DELAY_MILLIS = 150;
	private static final long COMPLETION_DELAY_MILLIS = 100;
	private static final int WAIT_LIMIT_SECONDS = 120;

	private final MultipartStub stub = new MultipartStub();
	private long stepMillis;

	@TempDir
	Path home;

	@AfterEach
	void stop() {
		stub.close();
	}

	/**
	 * Every upload completes. The step records one sample per part and one per upload, the longest
	 * of them shorter than a part and a completion together, which any upload takes at least.
	 */
	@Test
	void aStepRecordsOneSamplePerPartAndPerCompletedUploadAndNoneForAWholeUpload() throws Exception {
		stub.partDelayMillis = PART_DELAY_MILLIS;
		stub.completionDelayMillis = COMPLETION_DELAY_MILLIS;

		final var metrics = write("""
						Load.config({
							"load": {"op": {"limit": {"count": %d}, "wait": {"limit": %d}}},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"size": "%dKB", "ranges": {"threshold": "%dKB"}}}
						}).run();
						""".formatted(UPLOADS, WAIT_LIMIT_SECONDS, CONCURRENCY, PARTS, PART_SIZE / 1024));

		assertEquals(UPLOADS, metrics.successSnapshot().count());
		assertEquals(0, metrics.failsSnapshot().count());
		assertEquals(UPLOADS, stub.completions.get());
		final var duration = metrics.durationSnapshot();
		final var latency = metrics.latencySnapshot();
		assertEquals(UPLOADS * PARTS + UPLOADS, duration.count(), "a sample per part and per completed upload");
		assertEquals(UPLOADS * PARTS + UPLOADS, latency.count());
		// The shortest sample is a completion, the longest a part; an upload's own time is at least
		// a part's and its completion's.
		final var upload = (PART_DELAY_MILLIS + COMPLETION_DELAY_MILLIS) * 1_000;
		assertTrue(duration.min() >= COMPLETION_DELAY_MILLIS * 1_000, () -> "DurationMin " + duration.min());
		assertTrue(duration.max() < upload, () -> "DurationMax " + duration.max() + " µs against an upload's " + upload);
		assertTrue(latency.max() <= duration.max(), () -> "LatencyMax " + latency.max());
		assertDurationSumFits(metrics, CONCURRENCY);
	}

	/**
	 * Every completion is refused: each upload is one failure and is aborted, and only its parts
	 * add samples.
	 */
	@Test
	void aStepWhoseCompletionsFailRecordsOnlyTheParts() throws Exception {
		stub.failCompletion = true;

		final var metrics = write("""
						Load.config({
							"load": {"op": {"limit": {"count": %d}, "wait": {"limit": %d}}},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"size": "%dKB", "ranges": {"threshold": "%dKB"}}}
						}).run();
						""".formatted(UPLOADS, WAIT_LIMIT_SECONDS, CONCURRENCY, PARTS, PART_SIZE / 1024));

		assertEquals(0, metrics.successSnapshot().count());
		assertEquals(UPLOADS, metrics.failsSnapshot().count());
		assertEquals(UPLOADS, stub.aborts.size(), stub.aborts::toString);
		assertEquals(UPLOADS * PARTS, metrics.durationSnapshot().count(), "the parts' samples only");
		assertEquals(UPLOADS * PARTS, metrics.latencySnapshot().count());
		assertDurationSumFits(metrics, CONCURRENCY);
	}

	/**
	 * Queue pressure: a 1-slot driver input queue, batch size 1, a 300 ms child enqueue timeout and
	 * parts that take 1 s, so a part, completion or abort may not be queued, and its upload fails
	 * without that request. Whichever uploads fail so, no sample is computed from a parent's cleared
	 * start.
	 */
	@Test
	void underQueuePressureNoSampleIsComputedFromAClearedStart() throws Exception {
		final var property = "spt.mpu.child.enqueue.timeout.millis";
		final var previous = System.getProperty(property);
		System.setProperty(property, "300");
		stub.partDelayMillis = 1_000;
		final AllMetricsSnapshot metrics;
		try {
			metrics = write("""
							Load.config({
								"load": {
									"batch": {"size": 1},
									"op": {"limit": {"count": %d}, "wait": {"limit": %d}}
								},
								"storage": {"driver": {"limit": {"concurrency": 5, "queue": {"input": 1}}}},
								"item": {"data": {"size": "4KB", "ranges": {"threshold": "1KB"}}}
							}).run();
							""".formatted(UPLOADS, WAIT_LIMIT_SECONDS));
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}

		assertEquals(UPLOADS, metrics.successSnapshot().count() + metrics.failsSnapshot().count(), metrics::toString);
		final var duration = metrics.durationSnapshot();
		assertTrue(duration.count() >= metrics.successSnapshot().count());
		assertTrue(duration.max() < stepMillis * 1_000, () -> "DurationMax " + duration.max() + " µs in a " + stepMillis + " ms step");
		assertTrue(metrics.latencySnapshot().max() < stepMillis * 1_000, () -> "LatencyMax " + metrics.latencySnapshot().max());
		assertDurationSumFits(metrics, 5);
	}

	/** Request times cannot add up to more than the step's time at every concurrency slot. */
	private void assertDurationSumFits(final AllMetricsSnapshot metrics, final int concurrency) {
		final var sum = metrics.durationSnapshot().sum();
		final var bound = TimeUnit.MILLISECONDS.toMicros(stepMillis) * concurrency;
		assertTrue(sum <= bound, () -> "DurationSum " + sum + " µs against " + bound + " µs of step time x concurrency");
	}

	/** Runs {@code scenario}'s one CREATE step and returns its last metrics snapshot. */
	private AllMetricsSnapshot write(final String scenario) throws Exception {
		final var stepId = "s3-aws-multipart-timing-" + System.nanoTime();
		final var config = S3AwsMergedConfig.create(home, stub.port(), Map.of(
						"load-step-id", stepId,
						"load-step-idAutoGenerated", false,
						"item-output-path", "/bucket"));
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = Extension.load(classLoader);
		final Map<String, AllMetricsSnapshot> snapshots = new ConcurrentHashMap<>();
		final var started = System.nanoTime();
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			public void unregister(final MetricsContext metricsCtx) {
				super.unregister(metricsCtx);
				if (!(metricsCtx instanceof DistributedMetricsContext) && stepId.equals(metricsCtx.loadStepId())) {
					snapshots.put(metricsCtx.opType().name(), metricsCtx.lastSnapshot());
				}
			}
		}) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("s3-aws multipart timing", scenario, engine, 1L).run();
		}
		stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		final var create = snapshots.get("CREATE");
		assertNotNull(create, () -> "the CREATE step's metrics: " + snapshots.keySet());
		return create;
	}
}
