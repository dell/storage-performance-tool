package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.op.deletion.DeleteRequest;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

final class DeleteObjectsXmlHandlerTest {

	@Test
	void rejectsEntryBeyondProtocolMaximumBeforeAllocatingAnotherResult() {
		final var handler = new DeleteObjectsXmlHandler();
		final StringBuilder xml = new StringBuilder("<DeleteResult>");
		for (int index = 0; index <= DeleteRequest.MAX_TARGET_COUNT; index++) {
			xml.append("<Deleted><Key>key-").append(index).append("</Key></Deleted>");
		}
		xml.append("</DeleteResult>");

		assertThrows(SAXException.class, () -> S3XmlParser.parse(
						new ByteArrayInputStream(xml.toString().getBytes(StandardCharsets.UTF_8)), handler));
		assertEquals(DeleteRequest.MAX_TARGET_COUNT, handler.results().size());
	}
}
