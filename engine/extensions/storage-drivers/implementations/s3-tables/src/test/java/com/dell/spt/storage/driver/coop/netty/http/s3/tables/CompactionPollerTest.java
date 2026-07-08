package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CompactionPoller}.
 *
 * <p>Uses a real {@link S3TablesStorageDriver} subclass (same package) whose
 * {@code executeHttpRequest} replays pre-seeded {@link FullHttpResponse} bodies in order.
 * Each poll cycle consumes two responses: one for GetTableMetadataLocation (control-plane)
 * and one for the data-plane GET of the metadata JSON.
 * The maintenance-configuration call consumes one response at the start.
 */
public class CompactionPollerTest {

	// -----------------------------------------------------------------------
	// metadataLocationToS3Path — pure static utility, no driver needed
	// -----------------------------------------------------------------------

	@Test
	void metadataLocationToS3Path_s3Scheme() {
		assertEquals(
						"/my-bucket/warehouse/metadata/v1.metadata.json",
						CompactionPoller.metadataLocationToS3Path("s3://my-bucket/warehouse/metadata/v1.metadata.json"));
	}

	@Test
	void metadataLocationToS3Path_s3aScheme() {
		assertEquals(
						"/my-bucket/warehouse/metadata/v1.metadata.json",
						CompactionPoller.metadataLocationToS3Path("s3a://my-bucket/warehouse/metadata/v1.metadata.json"));
	}

	@Test
	void metadataLocationToS3Path_httpPath_passThrough() {
		assertEquals("/bucket/key", CompactionPoller.metadataLocationToS3Path("/bucket/key"));
	}

	// -----------------------------------------------------------------------
	// poll() — convergence
	// -----------------------------------------------------------------------

	@Test
	void poll_convergesWhenFilesDropBelowTarget() throws Exception {
		// ingest=10×1024 bytes, target=1024 bytes/file, tolerance=1.0 → targetFileCount=10
		// sequence: 50 → 10 (stable=1) → 10 (stable=2 → converge)
		final TestDriver drv = makeDriver("tableCompactionPoll");

		enqueueMaintenanceOk(drv);                                  // PutTableMaintenanceConfiguration
		enqueueMetaPoll(drv, "s3://bucket/metadata/v1.json", 50);  // poll 1
		enqueueMetaPoll(drv, "s3://bucket/metadata/v2.json", 10);  // poll 2 stable=1
		enqueueMetaPoll(drv, "s3://bucket/metadata/v3.json", 10);  // poll 3 stable=2 → converge

		final CompactionPoller poller = new CompactionPoller(
						drv, 10L * 1024, 1024, 1.0, 2, 1L, 60_000L);
		assertTrue(poller.poll());

		assertEquals(50, drv.compactionFilesBefore);
		assertEquals(10, drv.compactionFilesAfter);
		assertTrue(drv.compactionCompleteMs > 0);
	}

	@Test
	void poll_timesOutWhenFilesNeverConverge() throws Exception {
		final TestDriver drv = makeDriver("tableCompactionPoll");
		final AtomicLong clock = new AtomicLong();

		enqueueMaintenanceOk(drv);
		for (int i = 0; i < 5; i++) {
			enqueueMetaPoll(drv, "s3://bucket/metadata/v1.json", 50);
		}

		final CompactionPoller poller = new CompactionPoller(
						drv, 10L * 1024, 1024, 1.0, 2, 1L, 5L, clock::get, clock::addAndGet);
		assertFalse(poller.poll());

		assertEquals(50, drv.compactionFilesBefore);
		assertEquals(50, drv.compactionFilesAfter);
		assertEquals(0, drv.compactionCompleteMs); // timeout sets completeMs=0
	}

	@Test
	void poll_stableCountResetsWhenFileCountRises() throws Exception {
		// 50 → 5 (stable=1) → 100 (reset) → 5 (stable=1) → 5 (stable=2 → converge)
		final TestDriver drv = makeDriver("tableCompactionPoll");

		enqueueMaintenanceOk(drv);
		enqueueMetaPoll(drv, "s3://b/v1.json", 50);   // before
		enqueueMetaPoll(drv, "s3://b/v2.json", 5);    // stable=1
		enqueueMetaPoll(drv, "s3://b/v3.json", 100);  // reset
		enqueueMetaPoll(drv, "s3://b/v4.json", 5);    // stable=1
		enqueueMetaPoll(drv, "s3://b/v5.json", 5);    // stable=2 → converge

		final CompactionPoller poller = new CompactionPoller(
						drv, 10L * 1024, 1024, 1.0, 2, 1L, 60_000L);
		assertTrue(poller.poll());
		assertEquals(50, drv.compactionFilesBefore);
		assertEquals(5, drv.compactionFilesAfter);
	}

	@Test
	void poll_fetchErrorIsRetriedNotFatal() throws Exception {
		// poll 1: control-plane returns 500 → error swallowed, retry
		// poll 2: normal convergence path (10 files, stable=1)
		// poll 3: 10 files, stable=2 → converge
		final TestDriver drv = makeDriver("tableCompactionPoll");

		enqueueMaintenanceOk(drv);

		// poll 1: GetTableMetadataLocation returns 500 → fetchCurrentFileCount throws, loop continues
		drv.enqueueResponse(resp(HttpResponseStatus.INTERNAL_SERVER_ERROR, "{}"));
		// poll 2 and 3: normal
		enqueueMetaPoll(drv, "s3://bucket/metadata/v2.json", 10);
		enqueueMetaPoll(drv, "s3://bucket/metadata/v3.json", 10);

		final CompactionPoller poller = new CompactionPoller(
						drv, 10L * 1024, 1024, 1.0, 2, 1L, 60_000L);
		assertTrue(poller.poll());
		// filesBefore is recorded on first successful poll (poll 2), so 10
		assertEquals(10, drv.compactionFilesBefore);
		assertEquals(10, drv.compactionFilesAfter);
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

	/** One poll cycle: GetTableMetadataLocation (control-plane) + data-plane GET (metadata JSON). */
	private static void enqueueMetaPoll(
					final TestDriver drv,
					final String metaS3Uri,
					final long totalDataFiles) {
		// Control-plane GET → metadataLocation response
		final String locJson = "{\"metadataLocation\":\"" + metaS3Uri + "\",\"versionToken\":\"tok\"}";
		drv.enqueueResponse(resp(HttpResponseStatus.OK, locJson));
		// Data-plane GET → Iceberg metadata JSON with snapshot summary
		final String metaJson = "{\"current-snapshot-id\":1,"
						+ "\"snapshots\":[{\"snapshot-id\":1,"
						+ "\"summary\":{\"total-data-files\":\"" + totalDataFiles + "\"}}]}";
		drv.enqueueResponse(resp(HttpResponseStatus.OK, metaJson));
	}

	/**
	 * Enqueue responses for the maintenance call sequence:
	 * 1. ListTableBuckets (consumed by effectiveArn() lazy fetchBucketArn on first control-plane call)
	 * 2. PutTableMaintenanceConfiguration 200 OK
	 */
	private static void enqueueMaintenanceOk(final TestDriver drv) {
		// ListTableBuckets — effectiveArn() fetches ARN lazily before first URI construction
		drv.enqueueResponse(resp(HttpResponseStatus.OK,
						"{\"tableBuckets\":[{\"name\":\"test-bucket\",\"arn\":\"arn:aws:s3tables:us-east-1:123456789:bucket/test-bucket\"}]}"));
		// PutTableMaintenanceConfiguration
		drv.enqueueResponse(resp(HttpResponseStatus.OK, "{}"));
	}

	private static FullHttpResponse resp(final HttpResponseStatus status, final String body) {
		return new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1,
						status,
						Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
	}

	// -----------------------------------------------------------------------
	// TestDriver — same pattern as S3TablesStorageDriverTest.TestDriver
	// -----------------------------------------------------------------------

	private static TestDriver makeDriver(final String opMode) throws Exception {
		return new TestDriver(baseConfig(opMode, "127.0.0.1"));
	}

	private static Config baseConfig(final String opMode, final String host) {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(sp -> {
								try {
									return sp.schema();
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream().findFirst().ifPresent(configSchemas::add);
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);
			config.val("load-batch-size", 1024);
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1024);
			config.val("storage-net-transport", "nio");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", List.of());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-writeSpinCount", 1);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 0);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", List.of(host));
			config.val("storage-net-node-port", 9024);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-http-headers", new HashMap<String, String>());
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", false);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", "testuser");
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", false);
			config.val("storage-s3tables-controlPlaneEndpoint", "");
			config.val("storage-s3tables-bucket", "test-bucket");
			config.val("storage-s3tables-namespace", "test-ns");
			config.val("storage-s3tables-tableName", "test-table");
			config.val("storage-s3tables-opMode", opMode);
			config.val("storage-s3tables-targetFileSizeBytes", 1024);
			config.val("storage-s3tables-ingestFileSizeBytes", 1024);
			config.val("storage-s3tables-totalIngestBytes", 10 * 1024);
			config.val("storage-s3tables-commitFreqMs", 500);
			config.val("storage-s3tables-compactionPollIntervalMs", 1);
			config.val("storage-s3tables-compactionPollTimeoutMs", 60000);
			config.val("storage-s3tables-compactionToleranceFactor", 1.0);
			config.val("storage-s3tables-compactionStabilizationPolls", 2);
			config.val("storage-s3tables-namespaceCount", 10);
			config.val("storage-s3tables-tablesPerNamespace", 10);
			return config;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static final class TestDriver extends S3TablesStorageDriver<Item, Operation<Item>> {
		private final ArrayDeque<FullHttpResponse> stubResponses = new ArrayDeque<>();

		// Expose compaction stats recorded by recordCompactionStats()
		long compactionFilesBefore = -1;
		long compactionFilesAfter = -1;
		long compactionCompleteMs = -1;

		TestDriver(final Config cfg) throws Exception {
			super("test-compaction",
							DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1KB"), 16, false),
							cfg.configVal("storage"),
							false,
							cfg.intVal("load-batch-size"));
			operationResultOutput(new com.github.akurilov.commons.io.Output<Operation<Item>>() {
				@Override
				public boolean put(final Operation<Item> item) {
					return true;
				}

				@Override
				public int put(final List<Operation<Item>> buf, final int from, final int to) {
					return to - from;
				}

				@Override
				public int put(final List<Operation<Item>> buf) {
					return buf.size();
				}

				@Override
				public com.github.akurilov.commons.io.Input<Operation<Item>> getInput() {
					return null;
				}

				@Override
				public void close() {}
			});
		}

		void enqueueResponse(final FullHttpResponse resp) {
			stubResponses.add(resp);
		}

		@Override
		protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
			final FullHttpResponse next = stubResponses.poll();
			if (next != null)
				return next;
			return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		}

		@Override
		void recordCompactionStats(
						final long startMs,
						final long completeMs,
						final long filesBefore,
						final long filesAfter) {
			super.recordCompactionStats(startMs, completeMs, filesBefore, filesAfter);
			this.compactionFilesBefore = filesBefore;
			this.compactionFilesAfter = filesAfter;
			this.compactionCompleteMs = completeMs;
		}
	}
}
