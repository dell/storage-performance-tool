package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
		// All three control-plane calls return 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));

		final String result = drv.requestNewPath("/test-bucket/test-ns/test-table");

		assertNotNull(result, "requestNewPath should return the path on success");
		assertEquals(3, drv.log().size(), "Expected 3 control-plane requests: CreateTableBucket, CreateNamespace, CreateTable");
	}

	@Test
	void provision_idempotentOn409() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// All three calls return 409 (already exists) — should still succeed
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT, Unpooled.EMPTY_BUFFER));

		final String result = drv.requestNewPath("/test-bucket/test-ns/test-table");

		assertNotNull(result, "requestNewPath should return the path when all resources already exist (409)");
		assertEquals(3, drv.log().size(), "Expected 3 control-plane requests even when all 409");
	}

	@Test
	void provision_returnsNullOnHttpError() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);
		// CreateTableBucket returns 500 — should fail provision
		drv.enqueueResponse(
						new DefaultFullHttpResponse(
										HttpVersion.HTTP_1_1,
										HttpResponseStatus.INTERNAL_SERVER_ERROR,
										Unpooled.EMPTY_BUFFER));

		final String result = drv.requestNewPath("/test-bucket/test-ns/test-table");

		assertNull(result, "requestNewPath should return null when provision fails");
	}

	@Test
	void provision_submit_marksOpSucc() throws Exception {
		final Config cfg = baseConfig("provision", "127.0.0.1");
		final TestDriver drv = new TestDriver(cfg);

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
	void unsupportedOpMode_submit_throwsIllegalState() throws Exception {
		final Config cfg = baseConfig("tableWrite", "127.0.0.1");
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
						"submit should throw IllegalStateException for unsupported Phase 1 opModes");
	}
}
