package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class StreamingSha256Test {

	@Test
	void hashesExactFinalBytesAndAlwaysResetsTheItem() throws Exception {
		final byte[] bytes = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		final var item = new DataItemImpl("object", 0, bytes.length);
		item.dataInput(new FixedDataInput(bytes));
		item.position(2);

		try (final var hasher = new StreamingSha256(1)) {
			final var result = hasher.hash(item);
			assertEquals(
							"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
							result.metadata().digest());
			assertEquals(3, result.metadata().size());
			assertTrue(result.workerNanos() >= 0);
			assertEquals(0, item.position());
		}
	}

	@Test
	void usesAReusableBoundedWorkerSetAndRejectsUseAfterClose() {
		final var hasher = new StreamingSha256(2);
		assertEquals(2, hasher.workerCount());
		hasher.close();
		final var item = new DataItemImpl("empty", 0, 0);
		assertThrows(IllegalStateException.class, () -> hasher.hash(item));
	}

	private static final class FixedDataInput implements DataInput {
		private final ByteBuffer data;

		private FixedDataInput(final byte[] data) {
			this.data = ByteBuffer.wrap(data);
		}

		@Override
		public ByteBuffer getLayer(final int layerIdx) {
			return data.asReadOnlyBuffer();
		}

		@Override
		public int getSize() {
			return data.capacity();
		}

		@Override
		public void close() {}
	}
}
