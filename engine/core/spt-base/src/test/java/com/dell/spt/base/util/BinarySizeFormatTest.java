package com.dell.spt.base.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.jupiter.api.Test;

class BinarySizeFormatTest {

	@Test
	void formatFixedSizeUsesIecLabels() {
		assertEquals("0B", BinarySizeFormat.formatFixedSize(0));
		assertEquals("512B", BinarySizeFormat.formatFixedSize(512));
		assertEquals("1KiB", BinarySizeFormat.formatFixedSize(1024));
		assertEquals("1.500KiB", BinarySizeFormat.formatFixedSize(1536));
		assertEquals("12.50KiB", BinarySizeFormat.formatFixedSize(12800));
		assertEquals("123.5KiB", BinarySizeFormat.formatFixedSize(126464));
		assertEquals("1MiB", BinarySizeFormat.formatFixedSize(1024L * 1024));
		assertEquals("1GiB", BinarySizeFormat.formatFixedSize(1024L * 1024 * 1024));
		assertEquals("1TiB", BinarySizeFormat.formatFixedSize(1L << 40));
		assertEquals("1PiB", BinarySizeFormat.formatFixedSize(1L << 50));
		assertEquals("1EiB", BinarySizeFormat.formatFixedSize(1L << 60));
		assertEquals("-1.500KiB", BinarySizeFormat.formatFixedSize(-1536));
	}

	@Test
	void formatSizeUsesIecLabelsForSizeInBytes() {
		assertEquals("4MiB", BinarySizeFormat.formatSize(new SizeInBytes("4MB")));
		assertEquals("1KiB-2KiB", BinarySizeFormat.formatSize(new SizeInBytes("1KB-2KB")));
	}

	@Test
	void formatSizeHandlesNull() {
		assertEquals("", BinarySizeFormat.formatSize(null));
	}
}
