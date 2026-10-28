package com.dell.spt.base.nic;

import java.util.Comparator;

/** Orders interface names with their digit runs compared as numbers: {@code eth2} before {@code eth10}. */
public final class NaturalOrder implements Comparator<String> {

	public static final NaturalOrder INSTANCE = new NaturalOrder();

	private NaturalOrder() {}

	@Override
	public int compare(final String a, final String b) {
		int i = 0;
		int j = 0;
		while (i < a.length() && j < b.length()) {
			final char ca = a.charAt(i);
			final char cb = b.charAt(j);
			if (Character.isDigit(ca) && Character.isDigit(cb)) {
				final int endA = digitRunEnd(a, i);
				final int endB = digitRunEnd(b, j);
				final var runA = stripLeadingZeros(a.substring(i, endA));
				final var runB = stripLeadingZeros(b.substring(j, endB));
				if (runA.length() != runB.length()) {
					return Integer.compare(runA.length(), runB.length());
				}
				final int byValue = runA.compareTo(runB);
				if (byValue != 0) {
					return byValue;
				}
				i = endA;
				j = endB;
			} else {
				if (ca != cb) {
					return Character.compare(ca, cb);
				}
				i++;
				j++;
			}
		}
		final int byRemainder = Integer.compare(a.length() - i, b.length() - j);
		return byRemainder != 0 ? byRemainder : a.compareTo(b);
	}

	private static int digitRunEnd(final String text, final int start) {
		int end = start;
		while (end < text.length() && Character.isDigit(text.charAt(end))) {
			end++;
		}
		return end;
	}

	private static String stripLeadingZeros(final String digits) {
		int start = 0;
		while (start < digits.length() - 1 && digits.charAt(start) == '0') {
			start++;
		}
		return digits.substring(start);
	}
}
