package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
		assertThrows(SAXException.class, () -> parse("other/1z", "benchmark/", 42, true));
	}

	/** Under item data verification only: without it such a key is kept with offset 0. */
	@Test
	void rejectsUnparseableLeafInsteadOfFallingBackToOffsetZero() {
		final var failure = assertThrows(
						SAXException.class, () -> parse("benchmark/not-an-id", "benchmark/", 42, true));
		assertTrue(failure.getMessage().contains("\"benchmark/not-an-id\""), failure.getMessage());
		assertTrue(
						failure.getMessage().contains("verification needs keys that SPT generated"),
						failure.getMessage());
	}

	@Test
	void withoutVerifyKeepsKeysWithoutAGeneratedLeafWithOffsetZero() throws Exception {
		for (final String key : List.of("dir/", "photo.jpg", "abcdefghijklmnopq", "a/b/readme.md")) {
			final var items = parse(key, null, 42, false);
			assertEquals(1, items.size(), key);
			assertEquals(BUCKET_PATH + "/" + key, items.get(0).name(), key);
			assertEquals(0, items.get(0).offset(), key);
			assertEquals(42, items.get(0).size(), key);
		}
	}

	@Test
	void withoutVerifyKeepsAKeyUnderANamingPrefixWithoutAGeneratedLeafWithOffsetZero() throws Exception {
		final var items = parse("pre.jpg", "pre", 42, false);
		assertEquals(1, items.size());
		assertEquals("/bucket/pre.jpg", items.get(0).name());
		assertEquals(0, items.get(0).offset());
	}

	@Test
	void withoutVerifyStillParsesGeneratedLeaves() throws Exception {
		assertEquals(Long.parseLong("1z", RADIX), parse("benchmark/1z", "benchmark/", 42, false).get(0).offset());
		assertEquals(
						Long.parseLong("1z", RADIX),
						parse("benchmark/s000000f/1z", "benchmark/", 42, false).get(0).offset());
	}

	@Test
	void withoutVerifyCountsThePagesKeysWithoutAGeneratedLeaf() throws Exception {
		final var items = new ArrayList<DataItem>();
		final var handler = new BucketXmlListingHandler<>(
						items, BUCKET_PATH, null, new DataItemFactoryImpl<>(), RADIX, false);
		parseXml(
						"<ListBucketResult><Contents><Key>k0</Key><Size>1</Size></Contents>"
										+ "<Contents><Key>dir/</Key><Size>0</Size></Contents>"
										+ "<Contents><Key>k1</Key><Size>1</Size></Contents>"
										+ "<Contents><Key>photo.jpg</Key><Size>1</Size></Contents>"
										+ "<IsTruncated>false</IsTruncated></ListBucketResult>",
						handler);
		assertEquals(4, items.size());
		assertEquals(2, handler.nonGeneratedKeyCount());
		assertEquals("dir/", handler.firstNonGeneratedKey());
		assertEquals(List.of(Long.parseLong("k0", RADIX), 0L, Long.parseLong("k1", RADIX), 0L),
						items.stream().map(DataItem::offset).toList());

		items.clear();
		parseXml(
						"<ListBucketResult><Contents><Key>k2</Key><Size>1</Size></Contents>"
										+ "<IsTruncated>false</IsTruncated></ListBucketResult>",
						handler);
		assertEquals(0, handler.nonGeneratedKeyCount(), "each page counts its own keys");
		assertNull(handler.firstNonGeneratedKey());
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

	private static List<DataItem> parse(
					final String key, final String prefix, final long size) throws Exception {
		return parse(key, prefix, size, true);
	}

	private static List<DataItem> parse(
					final String key, final String prefix, final long size, final boolean verify)
					throws Exception {
		final var items = new ArrayList<DataItem>();
		final var handler = new BucketXmlListingHandler<>(
						items, BUCKET_PATH, prefix, new DataItemFactoryImpl<>(), RADIX, verify);
		final var xml = "<ListBucketResult><Contents><Key>" + key + "</Key><Size>" + size
						+ "</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
		parseXml(xml, handler);
		return items;
	}

	private static List<DataItem> parseXml(final String xml, final String prefix)
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
