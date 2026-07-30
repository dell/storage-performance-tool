package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import javax.xml.parsers.SAXParserFactory;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

final class BucketXmlListingHandlerTest {

	private static final String BUCKET_PATH = "/bucket";
	private static final int RADIX = Character.MAX_RADIX;

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
	void rejectsObjectOutsideConfiguredPrefix() {
		assertThrows(SAXException.class, () -> parse("other/1z", "benchmark/", 42));
	}

	@Test
	void rejectsUnparseableLeafInsteadOfFallingBackToOffsetZero() {
		assertThrows(SAXException.class, () -> parse("benchmark/not-an-id", "benchmark/", 42));
	}

	private static ArrayList<DataItem> parse(
					final String key, final String prefix, final long size) throws Exception {
		final var items = new ArrayList<DataItem>();
		final var handler = new BucketXmlListingHandler<>(
						items, BUCKET_PATH, prefix, new DataItemFactoryImpl<>(), RADIX);
		final var xml = "<ListBucketResult><Contents><Key>" + key + "</Key><Size>" + size
						+ "</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
		SAXParserFactory.newInstance()
						.newSAXParser()
						.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), handler);
		return items;
	}
}
