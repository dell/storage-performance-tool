package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.SAXParserFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

final class S3XmlParserTest {

	@AfterEach
	void clearParser() {
		S3XmlParser.clearThreadLocalForTest();
	}

	@Test
	void rejectsDoctypeAndExternalEntitiesWithoutResolvingThem() throws Exception {
		final String hostile = "<!DOCTYPE ListBucketResult ["
						+ "<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
						+ "<ListBucketResult><CommonPrefixes><Prefix>&xxe;</Prefix></CommonPrefixes>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";

		assertThrows(SAXException.class, () -> parse(hostile, new CommonPrefixesXmlHandler()));
	}

	@Test
	void reappliesSecurityControlsToAReusedThreadLocalParser() throws Exception {
		S3StorageDriver.THREAD_LOCAL_XML_PARSER.set(
						SAXParserFactory.newInstance().newSAXParser());
		final String hostile = "<!DOCTYPE ListBucketResult ["
						+ "<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
						+ "<ListBucketResult><CommonPrefixes><Prefix>&xxe;</Prefix></CommonPrefixes>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>";

		assertThrows(SAXException.class, () -> parse(hostile, new CommonPrefixesXmlHandler()));
	}

	@Test
	void remainsUsableAfterARejectedDocument() throws Exception {
		final String hostile = "<!DOCTYPE ListBucketResult [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
						+ "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
		assertThrows(SAXException.class, () -> parse(hostile, new CommonPrefixesXmlHandler()));

		final var handler = new CommonPrefixesXmlHandler();
		parse("<ListBucketResult><CommonPrefixes><Prefix>p/</Prefix></CommonPrefixes>"
						+ "<IsTruncated>false</IsTruncated></ListBucketResult>", handler);

		assertEquals(java.util.List.of("p/"), handler.commonPrefixes());
	}

	private static void parse(final String xml, final org.xml.sax.helpers.DefaultHandler handler)
					throws Exception {
		S3XmlParser.parse(
						new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), handler);
	}
}
