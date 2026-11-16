package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The object tagging requests the driver builds when {@code storage-object-tagging-enabled} is set:
 * an UPDATE sends {@code PUT ?tagging} with the configured tag set, a READ {@code GET ?tagging} and a
 * DELETE {@code DELETE ?tagging}, each signed under V2 over the {@code ?tagging} sub-resource.
 *
 * <p>Ported from {@code S3ObjectTaggingTest}, which was tagged {@code integration}, so no Gradle task
 * ran it, and could not start.
 */
final class S3ObjectTaggingRequestTest {

	private static final String NODE = "127.0.0.1";
	private static final String BUCKET = "/bucket2";
	private static final String ITEM_ID = "00003brre8lgz";
	private static final String URI = BUCKET + "/" + ITEM_ID + "?tagging";
	private static final Credential CREDENTIAL = Credential.getInstance(SigV2TestSupport.UID, SigV2TestSupport.SECRET);
	private static final String TAGGING_BODY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					+ "<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
					+ "\t<TagSet>\n"
					+ "\t\t<Tag><Key>tag1</Key><Value>value1</Value></Tag>\n"
					+ "\t\t<Tag><Key>tag2</Key><Value>value2</Value></Tag>\n"
					+ "\t</TagSet>\n"
					+ "</Tagging>\n";
	/** The Content-MD5 of {@link #TAGGING_BODY}, as the dead test had it. */
	private static final String TAGGING_BODY_MD5 = "sH5WWyhmHz+o5yoXD5DChg==";

	private S3StorageDriverTest.TestS3Driver driver;
	private FullHttpRequest request;

	@BeforeEach
	void setUp() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 2, false, null, NODE);
		config.val("storage-object-tagging-enabled", true);
		final var tags = new LinkedHashMap<String, String>();
		tags.put("tag1", "value1");
		tags.put("tag2", "value2");
		config.val("storage-object-tagging-tags", tags);
		// No Date, as in the dead test, so the canonical strings below are exact.
		config.val("storage-net-http-headers", new HashMap<String, String>());
		driver = new S3StorageDriverTest.TestS3Driver(config);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (request != null) {
			request.release();
		}
		driver.close();
	}

	@Test
	void anUpdateSendsPutTaggingWithTheTagSetAndItsMd5() throws Exception {
		request = taggingRequest(OpType.UPDATE);

		assertEquals(HttpMethod.PUT, request.method());
		assertEquals(URI, request.uri());
		final var headers = request.headers();
		assertEquals(NODE, headers.get(HttpHeaderNames.HOST));
		assertEquals(TAGGING_BODY, request.content().toString(StandardCharsets.UTF_8));
		assertEquals(TAGGING_BODY.getBytes(StandardCharsets.UTF_8).length, headers.getInt(HttpHeaderNames.CONTENT_LENGTH));
		assertEquals(TAGGING_BODY_MD5, headers.get(HttpHeaderNames.CONTENT_MD5));
		assertEquals(md5(TAGGING_BODY), TAGGING_BODY_MD5, "the expected Content-MD5 is the body's");
		assertSignedOver("PUT\n" + TAGGING_BODY_MD5 + "\n\n\n" + URI);
	}

	@Test
	void aReadSendsGetTagging() throws Exception {
		request = taggingRequest(OpType.READ);

		assertEquals(HttpMethod.GET, request.method());
		assertEquals(URI, request.uri());
		assertEquals(NODE, request.headers().get(HttpHeaderNames.HOST));
		assertNull(request.headers().get(HttpHeaderNames.CONTENT_MD5));
		assertEquals(0, request.content().readableBytes());
		assertSignedOver("GET\n\n\n\n" + URI);
	}

	@Test
	void aDeleteSendsDeleteTagging() throws Exception {
		request = taggingRequest(OpType.DELETE);

		assertEquals(HttpMethod.DELETE, request.method());
		assertEquals(URI, request.uri());
		assertEquals(NODE, request.headers().get(HttpHeaderNames.HOST));
		assertNull(request.headers().get(HttpHeaderNames.CONTENT_MD5));
		assertEquals(0, request.content().readableBytes());
		assertSignedOver("DELETE\n\n\n\n" + URI);
	}

	@SuppressWarnings("unchecked")
	private FullHttpRequest taggingRequest(final OpType type) throws Exception {
		final DataItem item = new DataItemImpl(ITEM_ID, Long.parseLong(ITEM_ID, Character.MAX_RADIX), 10_240);
		final var op = new DataOperationImpl<DataItem>(0, type, item, null, BUCKET, CREDENTIAL, null, 0);
		return (FullHttpRequest) driver.httpRequest((Operation<Item>) (Operation<?>) op, NODE);
	}

	/**
	 * Asserts the driver's V2 canonical request is {@code expected} and the Authorization header is
	 * the HMAC of exactly that string.
	 */
	private void assertSignedOver(final String expected) throws Exception {
		final var headers = request.headers();
		assertEquals(expected, driver.getCanonical(headers, request.method(), request.uri()));
		final var auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		assertTrue(auth.startsWith("AWS " + SigV2TestSupport.UID + ":"), auth);
		assertEquals("AWS " + SigV2TestSupport.UID + ":" + SigV2TestSupport.sign(SigV2TestSupport.SECRET, expected), auth);
	}

	private static String md5(final String text) throws Exception {
		return Base64.getEncoder().encodeToString(
						MessageDigest.getInstance("MD5").digest(text.getBytes(StandardCharsets.UTF_8)));
	}
}
