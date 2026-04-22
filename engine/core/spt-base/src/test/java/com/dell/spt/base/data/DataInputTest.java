package com.dell.spt.base.data;

import com.github.akurilov.commons.system.SizeInBytes;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataInputTest {

	@Test
	public void testMemoryTypeAllocation()
					throws Exception {
		try (final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1"), 1, false)) {
			assertTrue(dataInput.getLayer(0).isDirect());
		}
		try (final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1"), 1, true)) {
			assertFalse(dataInput.getLayer(0).isDirect());
		}
	}

	@Test
	public void testCompressibility50Percent() throws Exception {
		try (final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("16KB"), 1, false, 50.0)) {
			final var layer = dataInput.getLayer(0).asReadOnlyBuffer();
			layer.position(0);
			for (int chunk = 0; chunk < 4; chunk++) {
				int nonZeroCount = 0;
				for (int i = 0; i < 2048; i++) {
					if (layer.get() != 0) {
						nonZeroCount++;
					}
				}
				assertTrue(nonZeroCount > 1900);
				int zeroCount = 0;
				for (int i = 0; i < 2048; i++) {
					if (layer.get() == 0) {
						zeroCount++;
					}
				}
				assertEquals(2048, zeroCount);
			}
		}
	}

	@Test
	public void testCompressibility75Percent() throws Exception {
		try (final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("16KB"), 1, false, 75.0)) {
			final var layer = dataInput.getLayer(0).asReadOnlyBuffer();
			layer.position(0);
			for (int chunk = 0; chunk < 4; chunk++) {
				int nonZeroCount = 0;
				for (int i = 0; i < 1024; i++) {
					if (layer.get() != 0) {
						nonZeroCount++;
					}
				}
				assertTrue(nonZeroCount > 900);
				int zeroCount = 0;
				for (int i = 0; i < 3072; i++) {
					if (layer.get() == 0) {
						zeroCount++;
					}
				}
				assertEquals(3072, zeroCount);
			}
		}
	}

	@Test
	public void testCompressibilityRangeValidation() {
		assertThrows(
						IllegalArgumentException.class,
						() -> DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1KB"), 1, false, -0.1));
		assertThrows(
						IllegalArgumentException.class,
						() -> DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("1KB"), 1, false, 100.1));
	}

	@Test
	public void testDedupableFlagForSeedInput() throws Exception {
		try (final var dedupableInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1KB"), 1, false, 0.0, true)) {
			assertTrue(dedupableInput.isDedupable());
		}
		try (final var nonDedupableInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1KB"), 1, false, 0.0, false)) {
			assertFalse(nonDedupableInput.isDedupable());
		}
	}

	@Test
	public void testFileInputRejectsDedupableFalse() throws Exception {
		final Path temp = Files.createTempFile("spt-datainput", ".bin");
		Files.write(temp, new byte[]{1, 2, 3, 4
		});
		assertThrows(
						IllegalArgumentException.class,
						() -> DataInput.instance(temp.toString(), "7a42d9c483244167", new SizeInBytes("1KB"), 1, false, 0.0, false));
	}
}
