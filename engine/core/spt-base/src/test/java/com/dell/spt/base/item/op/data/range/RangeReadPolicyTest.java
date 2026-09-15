package com.dell.spt.base.item.op.data.range;

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadPolicyTest {
	@Test
	void presenceAlignmentAndSizeParsing() {
		assertNull(RangeReadPolicy.parse(null, null, null));
		assertThrows(IllegalArgumentException.class, () -> RangeReadPolicy.parse(null, "0", null));
		assertThrows(IllegalArgumentException.class, () -> RangeReadPolicy.parse(null, null, "1"));
		assertNull(RangeReadPolicy.parse("64KiB", null, null).fixedOffset());
		assertEquals(Long.valueOf(0), RangeReadPolicy.parse("64KiB", "0", null).fixedOffset());
		assertEquals(RangeReadPolicy.parse("64KiB", null, "0"), RangeReadPolicy.parse("65536", null, "1"));
		assertEquals(4096, RangeReadPolicy.parseBytes(" 4 kIb "));
		assertEquals(1L << 60, RangeReadPolicy.parseBytes("1EB"));
		assertEquals(Long.MAX_VALUE, RangeReadPolicy.parseBytes(Long.toString(Long.MAX_VALUE)));
		for (final String invalid : new String[]{"", " ", "-1", "+1", "1.5KiB", "1XB", "8EiB", "9223372036854775808"
		}) {
			assertThrows(IllegalArgumentException.class, () -> RangeReadPolicy.parseBytes(invalid), invalid);
		}
	}

	@Test
	void rejectsInvalidPoliciesWithoutRounding() {
		assertThrows(IllegalArgumentException.class, () -> new RangeReadPolicy(0, null, 1));
		assertThrows(IllegalArgumentException.class, () -> new RangeReadPolicy(1, null, -1));
		assertThrows(IllegalArgumentException.class, () -> new RangeReadPolicy(1, -1L, 1));
		assertThrows(IllegalArgumentException.class, () -> new RangeReadPolicy(3, 4L, 3));
		assertThrows(IllegalArgumentException.class, () -> new RangeReadPolicy(2, Long.MAX_VALUE, 1));
		assertEquals(new ByteRange(3, 2), new RangeReadPolicy(2, 3L, 3).select(0).range());
	}

	@Test
	void coversEveryLegalOffsetIncludingLastOnNonPowerOfTwoGrid() {
		final var policy = new RangeReadPolicy(4, null, 3);
		final Set<Long> observed = new HashSet<>();
		for (int i = 0; i < 4; i++) {
			final long draw = i;
			final var selection = policy.select(14, bound -> {
				assertEquals(4, bound);
				return draw;
			});
			assertNull(selection.error());
			assertEquals(4, selection.range().length());
			observed.add(selection.range().offset());
		}
		assertEquals(Set.of(0L, 3L, 6L, 9L), observed);
	}

	@Test
	void soleOffsetAndInvalidSizesNeverDraw() {
		for (final var policy : new RangeReadPolicy[]{new RangeReadPolicy(10, null, 1), new RangeReadPolicy(9, null, Long.MAX_VALUE)
		}) {
			assertEquals(0, policy.select(10, bound -> fail("Sole offset must not draw")).range().offset());
		}
		final var policy = new RangeReadPolicy(10, null, 1);
		for (final long size : new long[]{-1, Long.MIN_VALUE, 0, 9
		}) {
			final var selection = policy.select(size, bound -> fail("Invalid selection must not draw"));
			assertNull(selection.range());
			assertNotNull(selection.error());
		}
		assertEquals(RangeReadPolicy.SelectionError.SIZE_UNAVAILABLE, policy.select(-1).error());
		assertEquals(RangeReadPolicy.SelectionError.EMPTY_OBJECT, policy.select(0).error());
		assertEquals(RangeReadPolicy.SelectionError.UNDERSIZED_OBJECT, policy.select(9).error());
	}

	@Test
	void fixedSelectionIgnoresStoredBounds() {
		final var policy = new RangeReadPolicy(64, 1024L, 1);
		for (final long size : new long[]{-1, 0, 1, Long.MAX_VALUE
		}) {
			assertEquals("bytes=1024-1087", policy.select(size, bound -> fail("Fixed mode must not draw")).range().requestHeader());
		}
	}

	@Test
	void signedLongBoundariesDoNotOverflow() {
		final var finalByte = new RangeReadPolicy(1, null, 1).select(Long.MAX_VALUE, bound -> {
			assertEquals(Long.MAX_VALUE, bound);
			return bound - 1;
		}).range();
		assertEquals(Long.MAX_VALUE - 1, finalByte.endInclusive());
		assertEquals(Long.MAX_VALUE, new ByteRange(Long.MAX_VALUE, 1).endInclusive());
		assertEquals(Long.MAX_VALUE - 1, new RangeReadPolicy(Long.MAX_VALUE, null, 1).select(Long.MAX_VALUE).range().endInclusive());
		assertThrows(IllegalArgumentException.class, () -> new ByteRange(1, 0));
		assertThrows(IllegalArgumentException.class, () -> new ByteRange(-1, 1));
		assertThrows(IllegalArgumentException.class, () -> new ByteRange(2, Long.MAX_VALUE));
	}

	@Test
	void eachSelectionDrawsAgainAndRejectsInvalidSamplerOutput() {
		final var policy = new RangeReadPolicy(2, null, 1);
		final AtomicInteger draws = new AtomicInteger();
		assertEquals(0, policy.select(10, bound -> draws.getAndIncrement()).range().offset());
		assertEquals(1, policy.select(10, bound -> draws.getAndIncrement()).range().offset());
		assertThrows(IllegalArgumentException.class, () -> policy.select(10, bound -> bound));
		assertThrows(IllegalArgumentException.class, () -> policy.select(10, bound -> -1));
	}
}
