package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
		assertEquals(2, op.listedObjects().size());
		assertEquals("a.txt", op.listedObjects().get(0).key());
		assertEquals(123L, op.listedObjects().get(0).size());
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
		assertEquals(1, op.listedObjects().size());
		assertEquals("a.txt", op.listedObjects().get(0).key());
	}

	@Test
	void preservesExactWhitespaceUnicodeAndSeparatorKeyText() throws Exception {
		final var op = newOperation(false);
		parse(
						"<ListBucketResult><IsTruncated>false</IsTruncated>"
										+ "<Contents><Key>key</Key><Size>1</Size></Contents>"
										+ "<Contents><Key> key </Key><Size>2</Size></Contents>"
										+ "<Contents><Key>&#x9;雪/~&#xA;</Key><Size>3</Size></Contents>"
										+ "</ListBucketResult>",
						op,
						false);

		assertEquals(List.of("key", " key ", "\t雪/~\n"),
						op.listedObjects().stream().map(object -> object.key()).toList());
		assertEquals(List.of(1L, 2L, 3L),
						op.listedObjects().stream().map(object -> object.size()).toList());
	}

	@Test
	void rejectsSemanticallyInvalidListPagesWithoutMutatingOperation() {
		final var invalidPages = List.of(
						"<Error><Code>AccessDenied</Code></Error>",
						"<Other><IsTruncated>false</IsTruncated></Other>",
						"<ListBucketResult><Contents><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>-1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>nope</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>9223372036854775808</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>1</Size></Contents><IsTruncated>yes</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>1</Size></Contents></ListBucketResult>",
						"<ListBucketResult><Contents><Key>k</Key><Size>1</Size></Contents><IsTruncated>true</IsTruncated></ListBucketResult>");

		for (final String xml : invalidPages) {
			final var op = newOperation(false);
			assertThrows(Exception.class, () -> parse(xml, op, false), xml);
			assertEquals(0, op.objectsListed(), xml);
			assertEquals(0, op.bytesListed(), xml);
			assertTrue(op.listedObjects() == null || op.listedObjects().isEmpty(), xml);
			assertNull(op.continuationToken(), xml);
		}
	}

	@Test
	void rejectsTruncatedVersionPageWithoutBothMarkers() {
		final var op = newOperation(true);
		final var xml = "<ListVersionsResult><IsTruncated>true</IsTruncated>"
						+ "<Version><Key>k</Key><Size>1</Size></Version>"
						+ "<NextKeyMarker>next</NextKeyMarker></ListVersionsResult>";
		assertThrows(Exception.class, () -> parse(xml, op, true));
		assertEquals(0, op.objectsListed());
		assertTrue(op.listedObjects() == null || op.listedObjects().isEmpty());
	}

	private ListOperationImpl<PathItemImpl> newOperation(final boolean includeVersions) {
		final var item = new PathItemImpl("/bucket/prefix");
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, item, Credential.NONE);
		op.options(ListOptions.builder()
						.fetchMetadata(true)
						.includeVersions(includeVersions)
						.build());
		return op;
	}

	private void parse(
					final String xml,
					final ListOperationImpl<PathItemImpl> op,
					final boolean includeVersions)
					throws Exception {
		final var handler = new ListObjectsXmlHandler(op, includeVersions, true);
		parserFactory.newSAXParser().parse(
						new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), handler);
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
