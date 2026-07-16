package com.dell.spt.base.util;

import com.github.akurilov.commons.system.SizeInBytes;

import java.util.Locale;
import java.util.regex.Pattern;

/** Parses and formats 1024-based byte values using IEC unit labels. */
public final class BinarySizeFormat {

	private static final long KIB = 1024L;
	private static final Pattern IEC_UNIT = Pattern.compile("(?i)([kmgtpe])i(?=b)");
	private static final String[] UNITS = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"
	};

	private BinarySizeFormat() {}

	/**
	 * Parses a size specification while accepting both IEC suffixes such as {@code KiB} and
	 * legacy binary aliases such as {@code KB}.
	 */
	public static SizeInBytes parseSize(final String value) {
		return new SizeInBytes(normalizeInput(value));
	}

	/** Parses a fixed-size specification using the same suffix compatibility as {@link #parseSize(String)}. */
	public static long parseFixedSize(final String value) {
		return SizeInBytes.toFixedSize(normalizeInput(value));
	}

	public static String formatFixedSize(final long bytes) {
		if (bytes < KIB && bytes > -KIB) {
			return bytes + "B";
		}
		final double absBytes = Math.abs((double) bytes);
		int unitIndex = 0;
		double value = bytes;
		double absValue = absBytes;
		while (absValue >= KIB && unitIndex < UNITS.length - 1) {
			value /= KIB;
			absValue /= KIB;
			unitIndex++;
		}
		return formatValue(value) + UNITS[unitIndex];
	}

	public static String formatSize(final SizeInBytes size) {
		if (size == null) {
			return "";
		}
		final long min = size.getMin();
		final long max = size.getMax();
		if (min == max) {
			return formatFixedSize(min);
		}
		return formatFixedSize(min) + "-" + formatFixedSize(max);
	}

	private static String formatValue(final double value) {
		if (value == Math.rint(value)) {
			return String.format(Locale.ROOT, "%.0f", value);
		}
		final double abs = Math.abs(value);
		if (abs < 10) {
			return String.format(Locale.ROOT, "%.3f", value);
		}
		if (abs < 100) {
			return String.format(Locale.ROOT, "%.2f", value);
		}
		return String.format(Locale.ROOT, "%.1f", value);
	}

	private static String normalizeInput(final String value) {
		return IEC_UNIT.matcher(value).replaceAll("$1");
	}
}
