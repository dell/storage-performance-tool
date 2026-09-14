package com.dell.spt.storage.driver.coop.netty.http.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.storage.Credential;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class S3WireEncodingTest {
	private static final Credential CREDENTIAL = Credential.getInstance("test-user", "test-secret");

	@BeforeAll
	static void checkRequestedHostCharset() {
		final var expected = System.getProperty("spt.test.expectedCharset");
		if (expected != null)
			assertEquals(expected, Charset.defaultCharset().name());
	}

	@Test
	void taggingUsesUtf8BytesAndByteLength() throws Exception {
		var config = S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1");
		config.val("storage-object-tagging-tags", Map.of("label", "caf\u00e9"));
		try (var driver = new S3StorageDriverTest.TestS3Driver(config)) {
			var op = new OperationImpl<Item>(0, OpType.UPDATE, new DataItemImpl("object", 0, 8),
							"/bucket", "/bucket", CREDENTIAL);
			var request = driver.objectTaggingRequest(op, "127.0.0.1");
			try {
				assertTrue(request.content().toString(UTF_8).contains("<Value>caf\u00e9</Value>"));
				assertEquals(request.content().readableBytes(), request.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));
			} finally {
				assertTrue(request.release());
			}
		}
	}

	@Test
	void multipartXmlLengthCountsEncodedBytes() throws Exception {
		var config = S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1");
		try (var driver = new S3StorageDriverTest.TestS3Driver(config)) {
			var parent = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE,
							new DataItemImpl("object", 0, 8), "/bucket", null, CREDENTIAL, null, 0, 4);
			parent.put(S3Api.KEY_UPLOAD_ID, "test-upload");
			parent.put("1", "\"caf\u00e9\"");
			var request = driver.completeMultipartUploadRequest(parent, "127.0.0.1");
			try {
				assertTrue(request.content().toString(UTF_8).contains("<ETag>\"caf\u00e9\"</ETag>"));
				assertEquals(request.content().readableBytes(), request.headers().getInt(HttpHeaderNames.CONTENT_LENGTH));
			} finally {
				assertTrue(request.release());
			}
		}
	}

	@Test
	void signatureV2SignsUtf8CanonicalBytes() throws Exception {
		var config = S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1");
		try (var driver = new S3StorageDriverTest.TestS3Driver(config)) {
			var headers = new DefaultHttpHeaders();
			headers.set(HttpHeaderNames.DATE, "Mon, 01 Jan 2024 00:00:00 GMT");
			driver.applyAuthHeaders(headers, HttpMethod.GET, "/bucket/caf\u00e9", CREDENTIAL);
			var mac = Mac.getInstance("HmacSHA1");
			mac.init(new SecretKeySpec("test-secret".getBytes(UTF_8), "HmacSHA1"));
			var canonical = "GET\n\n\nMon, 01 Jan 2024 00:00:00 GMT\n/bucket/caf\u00e9";
			assertEquals("AWS test-user:" + Base64.getEncoder().encodeToString(mac.doFinal(canonical.getBytes(UTF_8))),
							headers.get(HttpHeaderNames.AUTHORIZATION));
		}
	}
}
