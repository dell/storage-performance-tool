package com.dell.spt.base.item.op.data.range;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/** Immutable, construction-time policy for fixed-length single-range READs. */
// @formatter:off
public record RangeReadPolicy(long length, Long fixedOffset, long alignment) {
// @formatter:on

public enum SelectionError {
	SIZE_UNAVAILABLE, EMPTY_OBJECT, UNDERSIZED_OBJECT

	}

	/** Exactly one of range and error is present. Selection failures are outcomes, not skipped work. */
	public record Selection(ByteRange range, SelectionError error) {
		public Selection {
			if ((range == null) == (error == null)) {
				throw new IllegalArgumentException("Selection must contain either a range or an error");
			}
		}
	}

	private static final Selection SIZE_UNAVAILABLE = new Selection(null, SelectionError.SIZE_UNAVAILABLE);
	private static final Selection EMPTY_OBJECT = new Selection(null, SelectionError.EMPTY_OBJECT);
	private static final Selection UNDERSIZED_OBJECT = new Selection(null, SelectionError.UNDERSIZED_OBJECT);

	public RangeReadPolicy
	{
		if (length <= 0 || alignment < 0) {
			throw new IllegalArgumentException("Range size must be positive and alignment nonnegative");
		}
		alignment = Math.max(1, alignment);
		if (fixedOffset != null) {
			new ByteRange(fixedOffset, length);
			if (fixedOffset % alignment != 0) {
				throw new IllegalArgumentException("Range offset must be divisible by effective alignment");
			}
		}
	}

	/** Null size disables the policy; even an explicitly supplied default alignment requires size. */
	public static RangeReadPolicy parse(final String size, final String offset, final String align) {
		if (size == null) {
			if (offset != null || align != null) {
				throw new IllegalArgumentException("Range offset/alignment requires range size");
			}
			return null;
		}
		return new RangeReadPolicy(parseBytes(size), offset == null ? null : parseBytes(offset),
						align == null ? 1 : parseBytes(align));
	}

	/** Checked integer binary-size grammar shared with the CLI's sizeparse package. */
	public static long parseBytes(final String value) {
		final String text = Objects.requireNonNull(value, "size").strip();
		int digits = 0;
		while (digits < text.length() && text.charAt(digits) >= '0' && text.charAt(digits) <= '9') {
			digits++;
		}
		if (digits == 0) {
			throw new IllegalArgumentException("Byte size must start with an unsigned decimal integer");
		}
		final long count = Long.parseLong(text.substring(0, digits));
		final int exponent = switch (text.substring(digits).strip().toUpperCase(Locale.ROOT)) {
		case "", "B" -> 0;
		case "K", "KB", "KIB" -> 1;
		case "M", "MB", "MIB" -> 2;
		case "G", "GB", "GIB" -> 3;
		case "T", "TB", "TIB" -> 4;
		case "P", "PB", "PIB" -> 5;
		case "E", "EB", "EIB" -> 6;
		default -> throw new IllegalArgumentException("Unknown binary byte-size suffix");
		};
		final long multiplier = 1L << (10 * exponent);
		if (count > Long.MAX_VALUE / multiplier) {
			throw new IllegalArgumentException("Byte size exceeds signed-64-bit bytes");
		}
		return count * multiplier;
	}

	/** Negative metadata represents unavailable/invalid size. Fixed mode does not consult it. */
	public Selection select(final long objectSize) {
		return select(objectSize, bound -> ThreadLocalRandom.current().nextLong(bound));
	}

	/** Bounded draws allow deterministic edge coverage without seeded replay as a product feature. */
	public Selection select(final long objectSize, final LongUnaryOperator boundedDraw) {
		if (fixedOffset != null) {
			return new Selection(new ByteRange(fixedOffset, length), null);
		}
		if (objectSize < 0) {
			return SIZE_UNAVAILABLE;
		}
		if (objectSize == 0) {
			return EMPTY_OBJECT;
		}
		if (objectSize < length) {
			return UNDERSIZED_OBJECT;
		}
		final long maxIndex = (objectSize - length) / alignment;
		// length >= 1 ensures maxIndex + 1 cannot overflow even when objectSize is MAX_VALUE.
		final long index = maxIndex == 0 ? 0 : boundedDraw.applyAsLong(maxIndex + 1);
		if (index < 0 || index > maxIndex) {
			throw new IllegalArgumentException("Random draw is outside its exclusive bound");
		}
		return new Selection(new ByteRange(index * alignment, length), null);
	}
}
