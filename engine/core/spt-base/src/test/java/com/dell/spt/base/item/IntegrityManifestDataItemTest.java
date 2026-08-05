package com.dell.spt.base.item;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

class IntegrityManifestDataItemTest {

	@Test
	void externalizationPreservesBucketArbitraryKeySizeAndVersion() throws Exception {
		final var item = new IntegrityManifestDataItem(
						"bucket", "prefix/line,with~tilde\nand-newline", 123, "version~1");
		final byte[] serialized;
		try (final var bytes = new ByteArrayOutputStream();
						final var out = new ObjectOutputStream(bytes)) {
			out.writeUnshared(item);
			out.flush();
			serialized = bytes.toByteArray();
		}

		final IntegrityManifestDataItem copy;
		try (final var in = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
			copy = (IntegrityManifestDataItem) in.readUnshared();
		}
		assertEquals(item.bucket(), copy.bucket());
		assertEquals(item.name(), copy.name());
		assertEquals(item.size(), copy.size());
		assertEquals(item.versionId(), copy.versionId());
	}

	@Test
	void operationUsesExplicitBucketAndVersionWithoutParsingTheKey() {
		final var item = new IntegrityManifestDataItem(
						"bucket", "prefix/key~which-is-not-a-version", 10, "actual-version");
		final var op = new OperationImpl<>(0, OpType.READ, item, "/ignored", null, null);

		assertEquals("/bucket", op.srcPath());
		assertEquals("prefix/key~which-is-not-a-version", op.item().name());
		assertEquals("actual-version", op.requestedVersionId());
		assertNull(op.returnedVersionId());

		final var result = op.result();
		assertEquals(
						"prefix/key~which-is-not-a-version",
						result.item().name(),
						"result copies must preserve canonical manifest identity");
	}
}
