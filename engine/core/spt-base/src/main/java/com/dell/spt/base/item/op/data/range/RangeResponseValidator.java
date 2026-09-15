package com.dell.spt.base.item.op.data.range;

import java.util.List;
import java.util.Objects;

/**
 * Transport-neutral, streaming validation for one attempt. The attempt owner serializes calls.
 * No body is retained. Drivers must cancel/close on rejection and release their own buffers.
 */
public final class RangeResponseValidator {

	public enum Failure {
		HTTP_STATUS, UNEXPECTED_STATUS, CONTENT_RANGE, CONTENT_LENGTH, MULTIPART, FRAMING, SHORT_BODY, OVERSIZED_BODY
	}

	private final ByteRange requested;
	private boolean headersAccepted;
	private boolean finished;
	private Failure failure;
	private long receivedBytes;
	private boolean receivedBytesOverflow;

	public RangeResponseValidator(final ByteRange requested) {
		this.requested = Objects.requireNonNull(requested);
	}

	/** Lists must preserve raw header multiplicity; do not pass SDK-normalized scalar values. */
	public boolean headers(
					final int status, final List<String> contentRanges, final List<String> contentLengths,
					final List<String> contentTypes, final boolean transferEncoding) {
		if (finished || failure != null) {
			return false;
		}
		if (headersAccepted) {
			return reject(Failure.FRAMING);
		}
		if (status < 200 || status >= 300) {
			return reject(Failure.HTTP_STATUS);
		}
		if (status != 206) {
			return reject(Failure.UNEXPECTED_STATUS);
		}
		if (contentRanges.size() != 1 || !matchesRange(contentRanges.get(0))) {
			return reject(Failure.CONTENT_RANGE);
		}
		if (transferEncoding && !contentLengths.isEmpty()) {
			return reject(Failure.FRAMING);
		}
		if (!contentLengths.isEmpty()) {
			if (contentLengths.size() != 1) {
				return reject(Failure.CONTENT_LENGTH);
			}
			try {
				if (decimal(ows(contentLengths.get(0))) != requested.length()) {
					return reject(Failure.CONTENT_LENGTH);
				}
			} catch (final IllegalArgumentException invalid) {
				return reject(Failure.CONTENT_LENGTH);
			}
		}
		for (final String type : contentTypes) {
			final String mediaType = ows(type.split(";", 2)[0]);
			if (asciiEqualsIgnoreCase(mediaType, "multipart/byteranges")) {
				return reject(Failure.MULTIPART);
			}
		}
		headersAccepted = true;
		return true;
	}

	/** Includes the whole delivered oversized chunk in received-byte accounting, without retaining it. */
	public boolean bodyBytes(final int count) {
		if (count < 0) {
			throw new IllegalArgumentException("Negative body chunk size");
		}
		if (finished || failure != null) {
			return false;
		}
		if (!headersAccepted) {
			return reject(Failure.FRAMING);
		}
		final long remaining = requested.length() - receivedBytes;
		if (receivedBytes > Long.MAX_VALUE - count) {
			receivedBytes = Long.MAX_VALUE;
			receivedBytesOverflow = true;
		} else {
			receivedBytes += count;
		}
		return count <= remaining || reject(Failure.OVERSIZED_BODY);
	}

	/** Only transport framing completion can make an exact-length body successful. */
	public boolean finish(final boolean framingValid) {
		if (finished || failure != null) {
			return false;
		}
		if (!headersAccepted || !framingValid) {
			return reject(Failure.FRAMING);
		}
		if (receivedBytes != requested.length()) {
			return reject(Failure.SHORT_BODY);
		}
		finished = true;
		return true;
	}

	public boolean succeeded() {
		return finished && failure == null;
	}

	public Failure failure() {
		return failure;
	}

	public long receivedBytes() {
		return receivedBytes;
	}

	public boolean receivedBytesOverflow() {
		return receivedBytesOverflow;
	}

	private boolean reject(final Failure reason) {
		failure = reason;
		return false;
	}

	private boolean matchesRange(final String header) {
		final String value = ows(header);
		if (value.length() < 6 || !asciiEqualsIgnoreCase(value.substring(0, 5), "bytes") || value.charAt(5) != ' ') {
			return false;
		}
		final int dash = value.indexOf('-', 6);
		final int slash = value.indexOf('/', dash + 1);
		if (dash < 0 || slash < 0) {
			return false;
		}
		try {
			final long start = decimal(value.substring(6, dash));
			final long end = decimal(value.substring(dash + 1, slash));
			final String total = value.substring(slash + 1);
			return start == requested.offset() && end == requested.endInclusive()
							&& ("*".equals(total) || decimal(total) > end);
		} catch (final IllegalArgumentException invalid) {
			return false;
		}
	}

	private static long decimal(final String value) {
		if (value.isEmpty()) {
			throw new IllegalArgumentException("Empty decimal");
		}
		long result = 0;
		for (int i = 0; i < value.length(); i++) {
			final char c = value.charAt(i);
			if (c < '0' || c > '9' || result > (Long.MAX_VALUE - (c - '0')) / 10) {
				throw new IllegalArgumentException("Invalid signed-64-bit unsigned decimal");
			}
			result = result * 10 + (c - '0');
		}
		return result;
	}

	private static String ows(final String value) {
		int start = 0;
		int end = value.length();
		while (start < end && (value.charAt(start) == ' ' || value.charAt(start) == '\t')) {
			start++;
		}
		while (end > start && (value.charAt(end - 1) == ' ' || value.charAt(end - 1) == '\t')) {
			end--;
		}
		return value.substring(start, end);
	}

	private static boolean asciiEqualsIgnoreCase(final String actual, final String expected) {
		if (actual.length() != expected.length()) {
			return false;
		}
		for (int i = 0; i < actual.length(); i++) {
			final char c = actual.charAt(i);
			if ((c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c) != expected.charAt(i)) {
				return false;
			}
		}
		return true;
	}
}
