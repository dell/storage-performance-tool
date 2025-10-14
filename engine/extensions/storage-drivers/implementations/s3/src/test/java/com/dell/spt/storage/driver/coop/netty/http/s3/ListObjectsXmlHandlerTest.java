package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.SAXParserFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ListObjectsXmlHandlerTest {

	private SAXParserFactory parserFactory;

	@BeforeEach
	void setUp() {
		parserFactory = SAXParserFactory.newInstance();
		parserFactory.setNamespaceAware(false);
	}

	@Test
	void parsesListObjectsV2Response() throws Exception {
		final var item = new PathItemImpl("/bucket/prefix");
		final var op = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, item, Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());

		final var handler = new ListObjectsXmlHandler(op, false, true);
		parserFactory
						.newSAXParser()
						.parse(new ByteArrayInputStream(LIST_V2_RESPONSE.getBytes(StandardCharsets.UTF_8)), handler);

		assertEquals(2, op.objectsListed());
		assertEquals(579L, op.bytesListed());
		assertTrue(op.truncated());
		assertEquals("token-123", op.options().continuationToken());
		assertEquals("token-123", op.continuationToken());
	}

	@Test
	void parsesListObjectVersionsResponse() throws Exception {
		final var item = new PathItemImpl("/bucket/prefix");
		final var op = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, item, Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).includeVersions(true).build());

		final var handler = new ListObjectsXmlHandler(op, true, true);
		parserFactory
						.newSAXParser()
						.parse(new ByteArrayInputStream(LIST_VERSIONS_RESPONSE.getBytes(StandardCharsets.UTF_8)), handler);

		assertEquals(2, op.objectsListed());
		assertEquals(42L, op.bytesListed());
		assertTrue(op.truncated());
		assertEquals("key-marker", op.continuationToken());
		assertEquals("key-marker", op.options().keyMarker());
		assertEquals("version-marker", op.options().versionIdMarker());
	}

	private static final String LIST_V2_RESPONSE = "<ListBucketResult>" +
					"<IsTruncated>true</IsTruncated>" +
					"<Contents><Key>a.txt</Key><Size>123</Size></Contents>" +
					"<Contents><Key>b.txt</Key><Size>456</Size></Contents>" +
					"<NextContinuationToken>token-123</NextContinuationToken>" +
					"</ListBucketResult>";

	private static final String LIST_VERSIONS_RESPONSE = "<ListVersionsResult>" +
					"<IsTruncated>true</IsTruncated>" +
					"<Version><Key>a.txt</Key><Size>42</Size></Version>" +
					"<DeleteMarker><Key>a.txt</Key></DeleteMarker>" +
					"<NextKeyMarker>key-marker</NextKeyMarker>" +
					"<NextVersionIdMarker>version-marker</NextVersionIdMarker>" +
					"</ListVersionsResult>";
}
