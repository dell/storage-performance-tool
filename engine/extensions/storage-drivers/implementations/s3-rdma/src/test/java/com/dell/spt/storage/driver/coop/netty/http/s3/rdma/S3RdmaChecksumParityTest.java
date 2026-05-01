package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.env.Extension;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

class S3RdmaChecksumParityTest {

	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	private static Config baseConfig(final boolean checksumEnabled, final String checksumAlg, final String host) {
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
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
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
			config.val("storage-net-http-headers", new HashMap<String, String>() {
				{
					put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
				}
			});
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", TEST_CRED.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", TEST_CRED.getSecret());
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", checksumEnabled);
			if (checksumEnabled) {
				config.val("storage-checksum-algorithm", checksumAlg);
			}
			config.val("storage-rdma-enabled", true);
			config.val("storage-rdma-thresholdBytes", 0L);
			config.val("storage-rdma-fallback", true);
			config.val("storage-rdma-device", "auto");
			config.val("storage-rdma-localIp", "");
			config.val("storage-rdma-logLevel", "WARN");
			config.val("storage-rdma-timeoutMs", 30000L);
			return config;
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static DataItem mockDataItemFromBytes(final byte[] payload) throws Exception {
		final DataItem dataItem = Mockito.mock(DataItem.class);
		final int[] readOffset = new int[]{0
		};
		Mockito.when(dataItem.size()).thenReturn((long) payload.length);
		Mockito.doAnswer(invocation -> {
			final ByteBuffer dst = invocation.getArgument(0);
			if (readOffset[0] >= payload.length) {
				return 0;
			}
			final int n = Math.min(dst.remaining(), payload.length - readOffset[0]);
			dst.put(payload, readOffset[0], n);
			readOffset[0] += n;
			return n;
		}).when(dataItem).read(Mockito.any(ByteBuffer.class));
		Mockito.doAnswer(invocation -> {
			readOffset[0] = 0;
			return null;
		}).when(dataItem).reset();
		return dataItem;
	}

	private static String checksumHeaderName(final String algorithm) {
		if ("md5".equals(algorithm)) {
			return HttpHeaderNames.CONTENT_MD5.toString();
		}
		if ("crc64-nvme".equals(algorithm)) {
			return "x-amz-checksum-crc64nvme";
		}
		return "x-amz-checksum-" + algorithm;
	}

	private static String checksumHeaderFor(final String algorithm, final byte[] payload) throws Exception {
		final Config cfg = baseConfig(true, algorithm, "s3.us-east-1.amazonaws.com:443");
		final TestS3RdmaDriver drv = new TestS3RdmaDriver(cfg);
		final DataItem dataItem = mockDataItemFromBytes(payload);
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, dataItem, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksumForTest(headers, op);
		return headers.get(checksumHeaderName(algorithm));
	}

	private static String reference32BitChecksum(final Checksum checksum, final byte[] payload) {
		checksum.reset();
		checksum.update(payload, 0, payload.length);
		final byte[] checksumBytes = ByteBuffer.allocate(Integer.BYTES).putInt((int) (checksum.getValue() & 0xFFFFFFFFL)).array();
		return Base64.getEncoder().encodeToString(checksumBytes);
	}

	private static class TestS3RdmaDriver extends S3RdmaStorageDriver<Item, Operation<Item>> {

		TestS3RdmaDriver(final Config cfg) throws Exception {
			super("test-s3-rdma",
							DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
							cfg.configVal("storage"), false, cfg.intVal("load-batch-size"));
		}

		void applyChecksumForTest(final HttpHeaders headers, final Operation<Item> op) {
			super.applyChecksum(headers, op);
		}
	}

	@Test
	void applyChecksum_crc32_knownVector_123456789_matchesReference() throws Exception {
		final byte[] payload = "123456789".getBytes(StandardCharsets.UTF_8);
		final String expected = reference32BitChecksum(new CRC32(), payload);
		assertEquals(expected, checksumHeaderFor("crc32", payload));
	}

	@Test
	void applyChecksum_crc32c_knownVector_123456789_matchesReference() throws Exception {
		final byte[] payload = "123456789".getBytes(StandardCharsets.UTF_8);
		final String expected = reference32BitChecksum(new CRC32C(), payload);
		assertEquals(expected, checksumHeaderFor("crc32c", payload));
	}

	@Test
	void applyChecksum_crc64nvme_knownVector_123456789_matchesReference() throws Exception {
		final byte[] payload = "123456789".getBytes(StandardCharsets.UTF_8);
		final String expected = "rosUhgp5mIg=";
		assertEquals(expected, checksumHeaderFor("crc64-nvme", payload));
	}

	@Test
	void applyChecksum_crc64nvme_emptyPayload_matchesReference() throws Exception {
		final byte[] payload = new byte[0];
		final String expected = "AAAAAAAAAAA=";
		assertEquals(expected, checksumHeaderFor("crc64-nvme", payload));
	}
}
