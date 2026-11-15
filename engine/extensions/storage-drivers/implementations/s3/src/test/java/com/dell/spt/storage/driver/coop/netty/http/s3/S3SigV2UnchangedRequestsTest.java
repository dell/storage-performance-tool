package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SigV2 requests that signed correctly before the canonical-resource fix, built through the
 * driver's own request builders: each must sign exactly the bytes it signed on the base revision.
 * The expected canonical resources are literals and the signatures are recomputed by
 * {@link SigV2TestSupport}; this class uses no API the base revision lacks, so it runs there too.
 */
class S3SigV2UnchangedRequestsTest {

	private static final Credential CREDENTIAL = Credential.getInstance(SigV2TestSupport.UID, SigV2TestSupport.SECRET);
	private static final String NODE = "127.0.0.1";

	private static S3StorageDriverTest.TestS3Driver driver(final boolean versioning) throws Exception {
		return new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(versioning, 2, false, null, NODE));
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void multipartRequestsSignTheirSubResources() throws Exception {
		final var drv = driver(false);
		try {
			final var item = new DataItemImpl("k0", 0, 20_971_520);
			final var parent = new CompositeDataOperationImpl<DataItem>(
							0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 10_485_760);

			final var init = drv.initMultipartUploadRequest((Operation) parent, NODE);
			assertEquals("/b/k0?uploads", init.uri());
			SigV2TestSupport.assertSignedOver(init, "/b/k0?uploads");

			parent.result(); // as StorageDriverBase.handleCompleted() does after the init
			parent.put(S3Api.KEY_UPLOAD_ID, "2~upload.ID_1-x");
			final var parts = parent.subOperations();
			parent.put("1", "\"etag-1\"");
			parent.put("2", "\"etag-2\"");

			final var part = drv.partUploadRequest((PartialDataOperation) parts.get(1), NODE);
			assertEquals("/b/k0?partNumber=2&uploadId=2~upload.ID_1-x", part.uri());
			SigV2TestSupport.assertSignedOver(part, "/b/k0?partNumber=2&uploadId=2~upload.ID_1-x");

			final var complete = drv.completeMultipartUploadRequest(parent, NODE);
			assertEquals(HttpMethod.POST, complete.method());
			assertEquals("/b/k0?uploadId=2~upload.ID_1-x", complete.uri());
			SigV2TestSupport.assertSignedOver(complete, "/b/k0?uploadId=2~upload.ID_1-x");

			final var abort = drv.abortMultipartUploadRequest(parent, NODE);
			assertEquals(HttpMethod.DELETE, abort.method());
			assertEquals("/b/k0?uploadId=2~upload.ID_1-x", abort.uri());
			SigV2TestSupport.assertSignedOver(abort, "/b/k0?uploadId=2~upload.ID_1-x");
		} finally {
			drv.close();
		}
	}

	@Test
	void aVersionIdReadSignsTheVersionId() throws Exception {
		final var drv = driver(true);
		try {
			final Operation<Item> op = new OperationImpl<>(1, OpType.READ, new ItemImpl("/bv/key"), null, "/bv", CREDENTIAL);
			op.requestedVersionId("3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY");

			final HttpRequest request = drv.httpRequest(op, NODE);
			assertEquals(HttpMethod.GET, request.method());
			assertEquals("/bv/key?versionId=3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY", request.uri());
			SigV2TestSupport.assertSignedOver(request, "/bv/key?versionId=3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY");
		} finally {
			drv.close();
		}
	}

	@Test
	void aVersionIdDeleteSignsTheVersionIdAsSent() throws Exception {
		final var drv = driver(true);
		try {
			final Operation<Item> op = new OperationImpl<>(1, OpType.DELETE, new ItemImpl("/bv/key~VER+1="), null, "/bv", CREDENTIAL);

			final HttpRequest request = drv.httpRequest(op, NODE);
			assertEquals(HttpMethod.DELETE, request.method());
			assertEquals("/bv/key?versionId=VER%2B1%3D", request.uri());
			SigV2TestSupport.assertSignedOver(request, "/bv/key?versionId=VER%2B1%3D");
		} finally {
			drv.close();
		}
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aTaggingRequestSignsTheTaggingSubResource() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 2, false, null, NODE);
		config.val("storage-object-tagging-enabled", true);
		config.val("storage-object-tagging-tags", Map.of("tag1", "value1"));
		final var drv = new S3StorageDriverTest.TestS3Driver(config);
		try {
			final var item = new DataItemImpl("00003brre8lgz", Long.parseLong("00003brre8lgz", Character.MAX_RADIX), 10240);
			final var op = new DataOperationImpl<DataItem>(0, OpType.UPDATE, item, null, "/bt", CREDENTIAL, null, 0);

			final HttpRequest request = drv.httpRequest((Operation) op, NODE);
			assertEquals(HttpMethod.PUT, request.method());
			assertEquals("/bt/00003brre8lgz?tagging", request.uri());
			assertNotNull(request.headers().get("Content-MD5"), "a tagging PUT signs its Content-MD5");
			SigV2TestSupport.assertSignedOver(request, "/bt/00003brre8lgz?tagging");
		} finally {
			drv.close();
		}
	}

	@Test
	void theBucketVersioningGetAndPutSignTheVersioningSubResource() throws Exception {
		final var drv = driver(true);
		try {
			drv.respondWith(request -> request.method() == HttpMethod.GET
							? new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(
											"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>",
											StandardCharsets.US_ASCII))
							: new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));

			drv.requestNewPath("/bv/p");

			final FullHttpRequest head = drv.log().poll();
			assertEquals(HttpMethod.HEAD, head.method());
			SigV2TestSupport.assertSignedOver(head, "/bv");
			final FullHttpRequest get = drv.log().poll();
			assertEquals(HttpMethod.GET, get.method());
			assertEquals("/bv?versioning", get.uri());
			SigV2TestSupport.assertSignedOver(get, "/bv?versioning");
			final FullHttpRequest put = drv.log().poll();
			assertNotNull(put, "a suspended bucket is switched to versioning enabled");
			assertEquals(HttpMethod.PUT, put.method());
			assertEquals("/bv?versioning", put.uri());
			SigV2TestSupport.assertSignedOver(put, "/bv?versioning");
		} finally {
			drv.close();
		}
	}
}
