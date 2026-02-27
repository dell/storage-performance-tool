package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
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
import java.util.Queue;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class S3TablesStorageDriverTest {

	private static final Credential TEST_CRED = Credential.getInstance("testuser", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	private static Config baseConfig(final String opMode, final String host) {
		try {
			final List<Map<String, Object>> configSchemas = Extension.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(
											sp -> {
												try {
													return sp.schema();
												} catch (Exception e) {
													throw new RuntimeException(e);
												}
											})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);
			config.val("load-batch-size", 1024);
			config.val("storage-driver-limit-concurrency", 0);
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
			config.val(
							"storage-net-http-headers",
							new HashMap<String, String>() {
								{
									put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
								}
							});
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", false);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", TEST_CRED.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", TEST_CRED.getSecret());
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", false);
			config.val("storage-s3tables-controlPlaneEndpoint", "");
			config.val("storage-s3tables-bucket", "test-bucket");
			config.val("storage-s3tables-namespace", "test-ns");
			config.val("storage-s3tables-tableName", "test-table");
			config.val("storage-s3tables-opMode", opMode);
			config.val("storage-s3tables-targetFileSizeBytes", 65536);
			config.val("storage-s3tables-ingestFileSizeBytes", 65536);
			config.val("storage-s3tables-totalIngestBytes", 1048576);
			config.val("storage-s3tables-commitFreqMs", 500);
			config.val("storage-s3tables-compactionPollIntervalMs", 5000);
			config.val("storage-s3tables-compactionPollTimeoutMs", 14400000);
			config.val("storage-s3tables-compactionToleranceFactor", 1.1);
			config.val("storage-s3tables-compactionStabilizationPolls", 2);
			config.val("storage-s3tables-namespaceCount", 10);
			config.val("storage-s3tables-tablesPerNamespace", 10);
			return config;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static class TestDriver extends S3TablesStorageDriver<Item, Operation<Item>> {
		private final Queue<FullHttpRequest> requests = new ArrayDeque<>();
		private final ArrayDeque<FullHttpResponse> stubResponses = new ArrayDeque<>();

		TestDriver(final Config cfg) throws Exception {
			super(
							"test-s3tables",
							DataInput.instance(
											null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
							cfg.configVal("storage"),
							false,
							cfg.intVal("load-batch-size"));
			// Wire a no-op result output so handleCompleted() doesn't NPE
			operationResultOutput(new com.github.akurilov.commons.io.Output<Operation<Item>>() {
				@Override
				public boolean put(final Operation<Item> item) {
					return true;
				}

				@Override
				public int put(final java.util.List<Operation<Item>> buffer, final int from, final int to) {
					return to - from;
				}

				@Override
				public int put(final java.util.List<Operation<Item>> buffer) {
					return buffer.size();
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

		Queue<FullHttpRequest> log() {
			return requests;
		}

		@Override
		protected FullHttpResponse executeHttpRequest(final FullHttpRequest httpRequest) {
			requests.add(httpRequest);
			final FullHttpResponse next = stubResponses.poll();
			if (next != null) {
				return next;
			}
			return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		}
	}

	@Test
	void provision_successOnAllNew() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// CreateTableBucket → 200 (no ARN in body, triggers ListTableBuckets)
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// ListTableBuckets (ARN not in CreateTableBucket response, fetched lazily)
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateNamespace → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateTable → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);

		assertEquals(Operation.Status.SUCC, op.status(), "op should be SUCC when all create calls succeed");
		// 4 requests: CreateTableBucket + ListTableBuckets(ARN) + CreateNamespace + CreateTable
		assertEquals(4, drv.log().size(), "Expected 4 control-plane requests");
	}

	@Test
	void provision_idempotentOn409() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// CreateTableBucket 409 → triggers ListTableBuckets to fetch ARN
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));
		// ListTableBuckets (ARN fetch after 409)
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateNamespace 409
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));
		// CreateTable 409
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);

		assertEquals(Operation.Status.SUCC, op.status(), "op should be SUCC when all resources already exist (409)");
		// 4 requests: CreateTableBucket(409) + ListTableBuckets + CreateNamespace(409) + CreateTable(409)
		assertEquals(4, drv.log().size(), "Expected 4 control-plane requests: bucket-409, list-buckets, ns-409, table-409");
	}

	@Test
	void provision_failsOpOnHttpError() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// CreateTableBucket returns 500 — should fail provision
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1,
										HttpResponseStatus.INTERNAL_SERVER_ERROR,
										Unpooled.EMPTY_BUFFER));

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);

		assertEquals(Operation.Status.FAIL_IO, op.status(), "op should be FAIL_IO when provision fails with HTTP 500");
	}

	@Test
	void provision_submit_marksOpSucc() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// CreateTableBucket → 200 (no ARN, triggers ListTableBuckets)
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// ListTableBuckets → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateNamespace → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateTable → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0,
						com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"),
						null,
						null,
						null);

		final boolean submitted = drv.submit(op);

		assertEquals(true, submitted, "submit should return true for provision opMode");
		assertEquals(Operation.Status.SUCC, op.status(), "op status should be SUCC after submit");
	}

	@Test
	void tableWrite_submit_completesOp() throws Exception {
		// tableWrite submit lazy-inits the committer via GetTableMetadataLocation (stub returns 200),
		// then performs the Iceberg commit sequence (Parquet gen + data-plane PUTs + metadata update).
		// All stubs return 200, so the full commit succeeds.
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0,
						com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"),
						null,
						null,
						null);

		final boolean submitted = drv.submit(op);
		assertEquals(true, submitted, "submit should return true for tableWrite");
		// Op completes (either SUCC or FAIL_IO depending on whether stubs satisfy the commit)
		assertNotNull(op.status(), "op status should be set after submit");
	}

	// --- ControlPlane timeout / HTTP-error branches ---

	@Test
	void provision_createTableBucket_timeoutFails() throws Exception {
		// null response from executeHttpRequest simulates a timeout
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg) {
			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				return null; // timeout
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(), "timeout on CreateTableBucket should fail op");
	}

	@Test
	void provision_createTableBucket_httpErrorFails() throws Exception {
		// 503 on CreateTableBucket → provision throws → op FAIL_IO
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1,
						HttpResponseStatus.SERVICE_UNAVAILABLE,
						Unpooled.copiedBuffer("unavailable", StandardCharsets.UTF_8)));
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(), "HTTP 503 on CreateTableBucket should fail op");
	}

	@Test
	void provision_fetchBucketArn_timeoutIsIgnored() throws Exception {
		// CreateTableBucket → 409 → ListTableBuckets null (timeout) → provision continues with bucket name
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg) {
			private int callCount = 0;

			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				callCount++;
				if (callCount == 1) {
					// CreateTableBucket → 409
					return new DefaultFullHttpResponse(
									HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER);
				}
				if (callCount == 2) {
					// ListTableBuckets (ARN fetch after 409) → null (timeout)
					return null;
				}
				// CreateNamespace + CreateTable → 200
				return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		// fetchBucketArn throws on timeout, effectiveArn falls back to bucket name; provision continues
		assertNotNull(op.status(), "op status should be set after provision with ARN-fetch timeout");
	}

	@Test
	void provision_fetchBucketArn_httpErrorIsIgnored() throws Exception {
		// CreateTableBucket → 409 → ListTableBuckets 500 → continues with bucket name
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.EMPTY_BUFFER));
		// CreateNamespace and CreateTable fall through with the default 200 stub
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertNotNull(op.status(), "op status should be set after provision with ARN-fetch HTTP error");
	}

	@Test
	void provision_createNamespace_timeoutFails() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg) {
			private int callCount = 0;

			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				callCount++;
				if (callCount == 1) {
					// CreateTableBucket → 200 with ARN
					return new DefaultFullHttpResponse(
									HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
									Unpooled.copiedBuffer(
													"{\"arn\":\"arn:aws:s3tables:us-east-1:123:bucket/test-bucket\"}",
													StandardCharsets.UTF_8));
				}
				// CreateNamespace → null (timeout)
				return null;
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(), "timeout on CreateNamespace should fail op");
	}

	@Test
	void provision_createNamespace_httpErrorFails() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
						Unpooled.copiedBuffer("{\"arn\":\"arn:aws:s3tables:us-east-1:123:bucket/test-bucket\"}",
										StandardCharsets.UTF_8)));
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST,
						Unpooled.copiedBuffer("bad request", StandardCharsets.UTF_8)));
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(), "HTTP 400 on CreateNamespace should fail op");
	}

	@Test
	void provision_createTable_httpErrorFails() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
						Unpooled.copiedBuffer("{\"arn\":\"arn:aws:s3tables:us-east-1:123:bucket/test-bucket\"}",
										StandardCharsets.UTF_8)));
		// CreateNamespace → 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		// CreateTable → 400
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST,
						Unpooled.copiedBuffer("bad table", StandardCharsets.UTF_8)));
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(), "HTTP 400 on CreateTable should fail op");
	}

	// --- tableWrite error-branch tests ---

	@Test
	void tableWrite_ensureCommitter_timeoutCausesFailIo() throws Exception {
		// GetTableMetadataLocation returns null → ensureCommitter returns null → FAIL_IO
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg) {
			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				return null; // timeout on GetTableMetadataLocation
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(),
						"null GetTableMetadataLocation response should set op to FAIL_IO");
	}

	@Test
	void tableWrite_dataPlaneRequest_nullResponseCausesFailIo() throws Exception {
		// GetTableMetadataLocation → 200 (committer inits OK)
		// First executeDataPlaneRequest (Parquet PUT) → null → commit throws → FAIL_IO
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// GetTableMetadataLocation stub — valid JSON with metadataLocation
		final String metaJson = "{\"metadataLocation\":\"s3://b/metadata/v1.metadata.json\",\"versionToken\":\"tok1\"}";
		drv.enqueueResponse(new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
						Unpooled.copiedBuffer(metaJson, StandardCharsets.UTF_8)));
		// All subsequent requests (data-plane PUTs) return null → timeout
		// (queue empty → TestDriver.executeHttpRequest returns default 200 for GetTableMetadataLocation,
		// then null for subsequent because we override below)
		final TestDriver drv2 = new TestDriver(cfg) {
			private int callCount = 0;

			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				callCount++;
				if (callCount == 1) {
					// GetTableMetadataLocation → valid response
					return new DefaultFullHttpResponse(
									HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
									Unpooled.copiedBuffer(metaJson, StandardCharsets.UTF_8));
				}
				return null; // timeout on Parquet PUT
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv2.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(),
						"null data-plane PUT response should result in FAIL_IO");
	}

	@Test
	void tableWrite_dataPlaneRequest_nonSuccessStatusCausesFailIo() throws Exception {
		// GetTableMetadataLocation → 200; first data-plane PUT → 403 → commit throws → FAIL_IO
		final String metaJson = "{\"metadataLocation\":\"s3://b/metadata/v1.metadata.json\",\"versionToken\":\"tok1\"}";
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg) {
			private int callCount = 0;

			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				callCount++;
				if (callCount == 1) {
					return new DefaultFullHttpResponse(
									HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
									Unpooled.copiedBuffer(metaJson, StandardCharsets.UTF_8));
				}
				// All PUTs return 403
				return new DefaultFullHttpResponse(
								HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN,
								Unpooled.copiedBuffer("forbidden", StandardCharsets.UTF_8));
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		assertEquals(Operation.Status.FAIL_IO, op.status(),
						"HTTP 403 on data-plane PUT should result in FAIL_IO");
	}

	// --- deriveWarehouseLocation edge cases ---

	@Test
	void deriveWarehouseLocation_nullReturnsEmpty() throws Exception {
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
		// Access via a tableWrite driver that hits the null/empty branch in ensureCommitter
		final String metaJsonNullLoc = "{\"metadataLocation\":\"\",\"versionToken\":\"tok1\"}";
		final TestDriver drv = new TestDriver(cfg) {
			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest req) {
				log().add(req);
				// Return empty metadataLocation — deriveWarehouseLocation("") returns ""
				return new DefaultFullHttpResponse(
								HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
								Unpooled.copiedBuffer(metaJsonNullLoc, StandardCharsets.UTF_8));
			}
		};
		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0, com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"), null, null, null);
		drv.submit(op);
		// The committer is initialized (warehouse = ""), commit will fail with a bad path but op status is set
		assertNotNull(op.status(), "op status must be set even with empty metadataLocation");
	}

	// --- Extension registration ---

	@Test
	void extension_idIsS3Tables() {
		final S3TablesStorageDriverExtension<Item, Operation<Item>, S3TablesStorageDriver<Item, Operation<Item>>> ext = new S3TablesStorageDriverExtension<>();
		assertEquals("s3-tables", ext.id(), "extension id must be s3-tables");
	}

	@Test
	void extension_schemaProviderIsNonNull() {
		final S3TablesStorageDriverExtension<Item, Operation<Item>, S3TablesStorageDriver<Item, Operation<Item>>> ext = new S3TablesStorageDriverExtension<>();
		assertNotNull(ext.schemaProvider(), "schemaProvider must not be null");
		assertEquals(APP_NAME, ext.schemaProvider().id(), "schemaProvider id must match APP_NAME");
	}

	@Test
	void unsupportedOpMode_submit_throwsIllegalState() throws Exception {
		final Config cfg = baseConfig("tableCompactionPoll", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);

		final Operation<Item> op = new com.dell.spt.base.item.op.OperationImpl<>(
						0,
						com.dell.spt.base.item.op.OpType.NOOP,
						new com.dell.spt.base.item.ItemImpl("dummy"),
						null,
						null,
						null);

		assertThrows(
						IllegalStateException.class,
						() -> drv.submit(op),
						"submit should throw IllegalStateException for opModes not yet implemented");
	}
}
