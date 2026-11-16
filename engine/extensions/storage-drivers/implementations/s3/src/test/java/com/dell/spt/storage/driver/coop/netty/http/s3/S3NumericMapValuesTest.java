package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InvalidTextValueException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A driver built from a YAML defaults document with numbers and booleans in its URI arguments,
 * request headers and object tags sends them as written, and tags as their canonical text, with
 * tagging on or off. Before, the constructor failed with a cast.
 */
class S3NumericMapValuesTest {

	private static final Credential CRED = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final String NODE = "127.0.0.1:9024";

	private static final String DOCUMENT = "storage:\n"
					+ "  net:\n"
					+ "    http:\n"
					+ "      uri:\n"
					+ "        args:\n"
					+ "          tid: 0012\n"
					+ "          on: true\n"
					+ "      headers:\n"
					+ "        X-Id: 7\n"
					+ "        X-Ratio: 1e3\n"
					+ "  object:\n"
					+ "    tagging:\n"
					+ "      enabled: %s\n"
					+ "      tags:\n"
					+ "        k: 7\n"
					+ "        t: True\n"
					+ "        n: ~\n";

	private static S3StorageDriverTest.TestS3Driver driver(final int authVersion, final boolean tagging) throws Exception {
		return driver(authVersion, DOCUMENT.formatted(tagging));
	}

	private static S3StorageDriverTest.TestS3Driver driver(final int authVersion, final String document) throws Exception {
		final Config base = S3StorageDriverTest.baseConfig(false, authVersion, false, null, "127.0.0.1");
		// The merge adds the document's entries to the base maps, so they must be mutable.
		base.val("storage-net-http-uri-args", new HashMap<>());
		base.val("storage-object-tagging-tags", new HashMap<>());
		final Config incoming = ConfigUtil.loadConfig(document, ConfigFormat.YAML, base.schema());
		return new S3StorageDriverTest.TestS3Driver(ConfigUtil.merge(base.pathSep(), List.of(base, incoming)));
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> dataOp(final OpType opType) throws Exception {
		final var item = new DataItemImpl("obj1", 0, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<DataItem>(
						0, opType, item, null, "/bucket", CRED, null, 0);
	}

	@Test
	void numericValuesReachTheRequestAsWrittenWithTaggingOff() throws Exception {
		for (final int authVersion : new int[]{2, 4
		}) {
			final var drv = driver(authVersion, false);
			final var req = drv.httpRequest(dataOp(OpType.CREATE), NODE);
			assertEquals("/bucket/obj1?tid=0012&on=true", req.uri(), "auth v" + authVersion);
			assertEquals("7", req.headers().get("X-Id"));
			assertEquals("1e3", req.headers().get("X-Ratio"));
		}
	}

	@Test
	void numericTagsAreTheirCanonicalTextAndANullTagIsKept() throws Exception {
		final var content = driver(4, true).taggingContentInput.get();
		assertTrue(content.contains("<Tag><Key>k</Key><Value>7</Value></Tag>"), content);
		assertTrue(content.contains("<Tag><Key>t</Key><Value>true</Value></Tag>"), content);
		// A tag with no value is sent as the text null, as before.
		assertTrue(content.contains("<Tag><Key>n</Key><Value>null</Value></Tag>"), content);
	}

	/**
	 * From a defaults document, a list or map URI argument or header, and a header with no value, load,
	 * and the driver refuses them with the setting's name, where it failed with a cast or a null before.
	 */
	@Test
	void aListMapOrMissingUriArgumentOrHeaderFromADocumentIsRefusedWithItsName() {
		final var http = "storage:\n  net:\n    http:\n";
		assertEquals("storage-net-http-uri-args-tid: the value is a list, not text",
						assertThrows(InvalidTextValueException.class,
										() -> driver(4, http + "      uri:\n        args:\n          tid: [1, 2]\n")).getMessage());
		assertEquals("storage-net-http-headers-X-Id: the value is a map, not text",
						assertThrows(InvalidTextValueException.class,
										() -> driver(4, http + "      headers:\n        X-Id: {a: b}\n")).getMessage());
		assertEquals("storage-net-http-headers-X-Id: a request header needs a value",
						assertThrows(InvalidTextValueException.class,
										() -> driver(2, http + "      headers:\n        X-Id: ~\n")).getMessage());
	}

	/**
	 * The configuration keeps a list or map tag, which {@code s3-aws} sends as text; this driver refuses
	 * it with the tag's name, with tagging on or off, where it failed with a cast before.
	 */
	@Test
	void aListOrMapTagIsRefusedWithItsName() {
		for (final var tagging : new boolean[]{false, true
		}) {
			final var tags = "storage:\n  object:\n    tagging:\n      enabled: " + tagging + "\n      tags:\n        k: ";
			assertEquals("storage-object-tagging-tags-k: the value is a list, not text",
							assertThrows(InvalidTextValueException.class, () -> driver(4, tags + "[1, 2]\n")).getMessage());
			assertEquals("storage-object-tagging-tags-k: the value is a map, not text",
							assertThrows(InvalidTextValueException.class, () -> driver(4, tags + "{a: b}\n")).getMessage());
		}
	}
}
