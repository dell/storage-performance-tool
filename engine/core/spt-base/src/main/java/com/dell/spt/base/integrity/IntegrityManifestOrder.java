package com.dell.spt.base.integrity;

/** Cross-language ordering for canonical integrity-manifest identities. */
public final class IntegrityManifestOrder {

	private IntegrityManifestOrder() {}

	/**
	 * Compares {@code (bucket,key,version_id)} by Unicode code point. UTF-8 byte ordering has the
	 * same result for valid Unicode scalar values, so this matches the Go canonicalizer.
	 */
	public static int compareIdentity(
					final String leftBucket,
					final String leftKey,
					final String leftVersion,
					final String rightBucket,
					final String rightKey,
					final String rightVersion) {
		int compared = compareText(leftBucket, rightBucket);
		if (compared == 0) {
			compared = compareText(leftKey, rightKey);
		}
		if (compared == 0) {
			compared = compareText(leftVersion, rightVersion);
		}
		return compared;
	}

	private static int compareText(final String left, final String right) {
		int leftOffset = 0;
		int rightOffset = 0;
		while (leftOffset < left.length() && rightOffset < right.length()) {
			final int leftCodePoint = left.codePointAt(leftOffset);
			final int rightCodePoint = right.codePointAt(rightOffset);
			if (leftCodePoint != rightCodePoint) {
				return Integer.compare(leftCodePoint, rightCodePoint);
			}
			leftOffset += Character.charCount(leftCodePoint);
			rightOffset += Character.charCount(rightCodePoint);
		}
		return Integer.compare(left.length() - leftOffset, right.length() - rightOffset);
	}
}
