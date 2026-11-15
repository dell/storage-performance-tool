package com.dell.spt.storage.driver.coop.netty.http.atmos;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.storage.driver.coop.netty.http.DellConstants.KEY_X_Dell_SIGNATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Atmos keeps signing the bare path when {@code storage-net-http-uri-args} is set. */
class AtmosUriArgsSigningTest {

	private static final Credential CRED = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	@Test
	void dataRequestWithUriArgsSignsTheBarePath() throws Exception {
		final var drv = new TestAtmosDriver(config(Map.of("tri", "probe")));
		try {
			for (final var opType : List.of(OpType.CREATE, OpType.READ)) {
				final var op = dataOp(opType);
				final var req = drv.requestFor(op);
				final var path = drv.dataUriPathFor(op);
				assertEquals(path + "?tri=probe", req.uri());
				final var signature = req.headers().get(KEY_X_Dell_SIGNATURE);
				assertNotNull(signature);
				assertEquals(signature, drv.signatureOver(req.headers(), req.method(), path));
				assertNotEquals(signature, drv.signatureOver(req.headers(), req.method(), req.uri()),
								"the check must tell the bare path from the URI with its arguments");
			}
		} finally {
			drv.close();
		}
	}

	private static DataOperation<DataItem> dataOp(final OpType opType) throws Exception {
		final var item = new DataItemImpl("obj1", 0, opType == OpType.CREATE ? 1024 : 0);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return new DataOperationImpl<>(0, opType, item, null, "/bucket", CRED, null, 0);
	}

	private static final class TestAtmosDriver extends AtmosStorageDriver<DataItem, DataOperation<DataItem>> {

		TestAtmosDriver(final Config cfg) throws Exception {
			super("test-atmos", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true),
							cfg.configVal("storage"), false, cfg.intVal("load-batch-size"));
		}

		io.netty.handler.codec.http.HttpRequest requestFor(final DataOperation<DataItem> op) throws Exception {
			return httpRequest(op, "127.0.0.1:9024");
		}

		String dataUriPathFor(final DataOperation<DataItem> op) {
			return dataUriPath(op.item(), op.srcPath(), op.dstPath(), op.type());
		}

		String signatureOver(
						final HttpHeaders sent, final io.netty.handler.codec.http.HttpMethod method, final String uri) {
			final HttpHeaders headers = new DefaultHttpHeaders().add(sent);
			headers.remove(KEY_X_Dell_SIGNATURE);
			applyAuthHeaders(headers, method, uri, CRED);
			return headers.get(KEY_X_Dell_SIGNATURE);
		}
	}

	private static Config config(final Map<String, String> uriArgs) {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(sp -> {
								try {
									return sp.schema();
								} catch (final Exception e) {
									throw new RuntimeException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);
			config.val("load-batch-size", 1024);
			config.val("storage-driver-type", "atmos");
			config.val("storage-driver-limit-concurrency", 0);
			config.val("storage-integrity-mode", "none");
			config.val("storage-integrity-algorithm", "sha256");
			config.val("storage-integrity-input-provenance", "none");
			config.val("storage-integrity-input-expectedProducerId", "");
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
			config.val("storage-net-node-addrs", List.of("127.0.0.1"));
			config.val("storage-net-node-port", 9024);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-http-headers", new HashMap<>(Map.of("Date", "Thu, 21 Sep 2023 12:00:00 GMT")));
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", uriArgs);
			config.val("storage-net-http-fsAccess", false);
			config.val("storage-auth-uid", CRED.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CRED.getSecret());
			return config;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}
}
