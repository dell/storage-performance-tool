package com.dell.spt.base.buildinfo;

import java.util.regex.Pattern;

/** Strict SemVer 2.0 validation for published Engine Build Information. */
final class SemanticVersion {

	private static final Pattern PATTERN = Pattern.compile(
					"^(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)"
									+ "(?:-[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?"
									+ "(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$");

	private SemanticVersion() {}

	static boolean isValid(final String value) {
		if (value == null || !PATTERN.matcher(value).matches()) {
			return false;
		}
		final int buildStart = value.indexOf('+');
		final int prereleaseStart = value.indexOf('-');
		if (prereleaseStart < 0 || (buildStart >= 0 && prereleaseStart > buildStart)) {
			return true;
		}
		final var prerelease = value.substring(
						prereleaseStart + 1, buildStart < 0 ? value.length() : buildStart);
		for (final var identifier : prerelease.split("\\.")) {
			if (identifier.length() > 1 && identifier.charAt(0) == '0' && onlyDigits(identifier)) {
				return false;
			}
		}
		return true;
	}

	private static boolean onlyDigits(final String value) {
		for (int i = 0; i < value.length(); i++) {
			if (!Character.isDigit(value.charAt(i))) {
				return false;
			}
		}
		return true;
	}
}
