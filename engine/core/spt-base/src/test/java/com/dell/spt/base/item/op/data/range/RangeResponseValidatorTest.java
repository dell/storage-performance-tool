package com.dell.spt.base.item.op.data.range;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RangeResponseValidatorTest {
	private static RangeResponseValidator validator() {
		return new RangeResponseValidator(new ByteRange(4, 3));
	}

	private static boolean headers(final RangeResponseValidator validator, final String range) {
		return validator.headers(206, List.of(range), List.of(), List.of(), false);
	}

	@Test
	void requiresStreamCompletionAndIgnoresLateCallbacks() {
		final var validator = validator();
		assertTrue(validator.headers(206, List.of("bytes 4-6/10"), List.of("3"), List.of(), false));
		assertTrue(validator.bodyBytes(1));
		assertTrue(validator.bodyBytes(2));
		assertFalse(validator.succeeded());
		assertTrue(validator.finish(true));
		assertTrue(validator.succeeded());
		assertFalse(validator.finish(true));
		assertFalse(validator.bodyBytes(2));
		assertEquals(3, validator.receivedBytes());
	}

	@Test
	void acceptsMutationUnknownTotalAndHttpWhitespace() {
		for (final String range : List.of("bytes 4-6/7", "bytes 4-6/100", "bytes 4-6/*", "\t BYTES 004-006/010 \t")) {
			final var validator = validator();
			assertTrue(headers(validator, range), range);
			assertTrue(validator.bodyBytes(3));
			assertTrue(validator.finish(true));
		}
	}

	@Test
	void rejectsMalformedMismatchedAndOverflowRanges() {
		for (final String range : List.of("bytes 0-2/10", "bytes 4-7/10", "bytes 4-6/6", "bytes */10",
						"bytes 4-/10", "bytes -6/10", "bytes +4-6/10", "bytes 4-6/9223372036854775808",
						"bytes 4-6/10, bytes 4-6/10", "bytes  4-6/10", "bytes\t4-6/10", "bytes 4-6/10\n",
						"bytes 4 -6/10", "bytes 4-6/", "byteſ 4-6/10")) {
			final var validator = validator();
			assertFalse(headers(validator, range), range);
			assertEquals(RangeResponseValidator.Failure.CONTENT_RANGE, validator.failure(), range);
		}
		for (final List<String> values : List.of(List.<String> of(), List.of("bytes 4-6/10", "bytes 4-6/10"))) {
			assertFalse(validator().headers(206, values, List.of(), List.of(), false));
		}
	}

	@Test
	void distinguishesHttpErrorsFromIgnoredRanges() {
		for (final int status : new int[]{301, 403, 404, 416, 500, 504
		}) {
			final var validator = validator();
			assertFalse(validator.headers(status, List.of(), List.of(), List.of(), false));
			assertEquals(RangeResponseValidator.Failure.HTTP_STATUS, validator.failure());
			assertFalse(validator.bodyBytes(100));
			assertEquals(0, validator.receivedBytes());
		}
		for (final int status : new int[]{200, 201, 204
		}) {
			final var validator = validator();
			assertFalse(validator.headers(status, List.of(), List.of(), List.of(), false));
			assertEquals(RangeResponseValidator.Failure.UNEXPECTED_STATUS, validator.failure());
		}
	}

	@Test
	void rejectsInvalidContentLengthAndConflictingFraming() {
		for (final List<String> lengths : List.of(List.of("2"), List.of("4"), List.of("3", "3"), List.of("3,3"), List.of("+3"), List.of(""))) {
			final var validator = validator();
			assertFalse(validator.headers(206, List.of("bytes 4-6/10"), lengths, List.of(), false));
			assertEquals(RangeResponseValidator.Failure.CONTENT_LENGTH, validator.failure());
		}
		final var conflict = validator();
		assertFalse(conflict.headers(206, List.of("bytes 4-6/10"), List.of("3"), List.of(), true));
		assertEquals(RangeResponseValidator.Failure.FRAMING, conflict.failure());
		final var chunked = validator();
		assertTrue(chunked.headers(206, List.of("bytes 4-6/10"), List.of(), List.of(), true));
		assertTrue(chunked.bodyBytes(3));
		assertTrue(chunked.finish(true));
	}

	@Test
	void rejectsMultipart() {
		final var validator = validator();
		assertFalse(validator.headers(206, List.of("bytes 4-6/10"), List.of(), List.of("Multipart/Byteranges; boundary=x"), false));
		assertEquals(RangeResponseValidator.Failure.MULTIPART, validator.failure());
	}

	@Test
	void rejectsShortOversizedAndInvalidFramingAndRetainsFirstFailure() {
		final var shortBody = validator();
		assertTrue(headers(shortBody, "bytes 4-6/10"));
		assertTrue(shortBody.bodyBytes(2));
		assertFalse(shortBody.finish(true));
		assertEquals(RangeResponseValidator.Failure.SHORT_BODY, shortBody.failure());
		final var oversized = validator();
		assertTrue(headers(oversized, "bytes 4-6/10"));
		assertFalse(oversized.bodyBytes(100));
		assertEquals(100, oversized.receivedBytes());
		assertFalse(oversized.finish(false));
		assertEquals(RangeResponseValidator.Failure.OVERSIZED_BODY, oversized.failure());
		final var invalidFraming = validator();
		assertTrue(headers(invalidFraming, "bytes 4-6/10"));
		assertTrue(invalidFraming.bodyBytes(3));
		assertFalse(invalidFraming.finish(false));
		assertEquals(RangeResponseValidator.Failure.FRAMING, invalidFraming.failure());
	}

	@Test
	void invalidCallOrderCannotSucceed() {
		final var earlyBody = validator();
		assertFalse(earlyBody.bodyBytes(0));
		assertFalse(headers(earlyBody, "bytes 4-6/10"));
		final var duplicateHeaders = validator();
		assertTrue(headers(duplicateHeaders, "bytes 4-6/10"));
		assertFalse(headers(duplicateHeaders, "bytes 4-6/10"));
		assertThrows(IllegalArgumentException.class, () -> validator().bodyBytes(-1));
	}

	@Test
	void fullSpanAndSignedLongEndAreValid() {
		final var full = new RangeResponseValidator(new ByteRange(0, 3));
		assertTrue(headers(full, "bytes 0-2/3"));
		assertTrue(full.bodyBytes(3));
		assertTrue(full.finish(true));
		final var maxEnd = new RangeResponseValidator(new ByteRange(Long.MAX_VALUE, 1));
		assertTrue(headers(maxEnd, "bytes 9223372036854775807-9223372036854775807/*"));
		assertTrue(maxEnd.bodyBytes(1));
		assertTrue(maxEnd.finish(true));
	}
}
