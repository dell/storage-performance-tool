package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

final class BucketXmlListingHandlerTest {

	private static final String BUCKET_PATH = "/bucket";
	private static final int RADIX = Character.MAX_RADIX;

	@AfterEach
	void clearParser() {
		S3XmlParser.clearThreadLocalForTest();
	}

	@Test
	void parsesGeneratedLeafAfterFlatPrefix() throws Exception {
		final var items = parse("benchmark/1z", "benchmark/", 42);

		assertEquals(1, items.size());
		assertEquals("/bucket/benchmark/1z", items.get(0).name());
		assertEquals(Long.parseLong("1z", RADIX), items.get(0).offset());
		assertEquals(42, items.get(0).size());
	}

	@Test
	void parsesGeneratedLeafAfterPrefixShard() throws Exception {
		final var items = parse("benchmark/s000000f/1z", "benchmark/", 42);

		assertEquals(1, items.size());
		assertEquals(Long.parseLong("1z", RADIX), items.get(0).offset());
	}

	@Test
	void parsesGeneratedLeafAfterShardWithoutPrefix() throws Exception {
		final var items = parse("s000000f/1z", null, 42);

		assertEquals(1, items.size());
		assertEquals(Long.parseLong("1z", RADIX), items.get(0).offset());
	}

	@Test
	void canonicalizesLeadingSlashPrefixForReturnedKeys() throws Exception {
		final var items = parse("benchmark/1z", "/benchmark/", 42);
		assertEquals(1, items.size());
		assertEquals(Long.parseLong("1z", RADIX), items.get(0).offset());
	}

	@Test
	void rejectsObjectOutsideConfiguredPrefix() {
		assertThrows(SAXException.class, () -> parse("other/1z", "benchmark/", 42));
	}

	@Test
	void rejectsUnparseableLeafInsteadOfFallingBackToOffsetZero() {
		assertThrows(SAXException.class, () -> parse("benchmark/not-an-id", "benchmark/", 42));
	}

	@Test
	void accumulatesEntitySplitKeySizeAndTruncationText() throws Exception {
		final var items = parseXml(
						"<ListBucketResult><Contents><Key>benchmark/1&#x7A;</Key>"
										+ "<Size>4&#x32;</Size></Contents><IsTruncated>fal&#x73;e</IsTruncated>"
										+ "</ListBucketResult>",
						"benchmark/");
		assertEquals(1, items.size());
		assertEquals(Long.parseLong("1z", RADIX), items.get(0).offset());
		assertEquals(42, items.get(0).size());
	}

	@Test
	void rejectsMissingFieldsWithoutReusingPriorEntryState() {
		final var missingSize = "<ListBucketResult>"
						+ "<Contents><Key>benchmark/1</Key><Size>1</Size></Contents>"
						+ "<Contents><Key>benchmark/2</Key></Contents>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
		final var missingKey = "<ListBucketResult><Contents><Size>1</Size></Contents>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";
		assertThrows(SAXException.class, () -> parseXml(missingSize, "benchmark/"));
		assertThrows(SAXException.class, () -> parseXml(missingKey, "benchmark/"));
	}

	@Test
	void rejectsRecognizedFieldsAtTheWrongHierarchyWithoutPublishingPartialItems() {
		final var invalidPages = java.util.List.of(
						"<ListBucketResult><Wrapper><Contents><Key>benchmark/1</Key><Size>1</Size></Contents></Wrapper><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Owner><Key>benchmark/1</Key></Owner><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Key>benchmark/1</Key><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>1</Size><IsTruncated>false</IsTruncated></Contents></ListBucketResult>");
		for (final String xml : invalidPages) {
			final var items = new ArrayList<DataItem>();
			final var handler = new BucketXmlListingHandler<>(
							items, BUCKET_PATH, "benchmark/", new DataItemFactoryImpl<>(), RADIX);
			assertThrows(Exception.class, () -> parseXml(xml, handler), xml);
			assertEquals(0, items.size(), xml);
		}
	}

	@Test
	void rejectsInvalidSizeRootTruncationAndMalformedDocument() {
		final var invalidPages = java.util.List.of(
						"<Error><Code>Denied</Code></Error>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>-1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>bad</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>1</Size></Contents><IsTruncated>no</IsTruncated></ListBucketResult>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>1</Size></Contents></ListBucketResult>",
						"<ListBucketResult><Contents><Key>benchmark/1</Key><Size>1</Size></Contents>");
		for (final String xml : invalidPages) {
			assertThrows(Exception.class, () -> parseXml(xml, "benchmark/"), xml);
		}
	}

	private static ArrayList<DataItem> parse(
					final String key, final String prefix, final long size) throws Exception {
		final var items = new ArrayList<DataItem>();
		final var handler = new BucketXmlListingHandler<>(
						items, BUCKET_PATH, prefix, new DataItemFactoryImpl<>(), RADIX);
		final var xml = "<ListBucketResult><Contents><Key>" + key + "</Key><Size>" + size
						+ "</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
		parseXml(xml, handler);
		return items;
	}

	private static ArrayList<DataItem> parseXml(final String xml, final String prefix)
					throws Exception {
		final var items = new ArrayList<DataItem>();
		final var handler = new BucketXmlListingHandler<>(
						items, BUCKET_PATH, prefix, new DataItemFactoryImpl<>(), RADIX);
		parseXml(xml, handler);
		return items;
	}

	private static void parseXml(final String xml, final BucketXmlListingHandler<DataItem> handler)
					throws Exception {
		S3XmlParser.parse(
						new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), handler);
	}
}
