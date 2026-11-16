package com.dell.spt.base.item.op.deletion;

/**
 * Why a DELETE verification probe left an identity unresolved, from a fixed set of categories. The
 * text is built only from a category, a numeric HTTP status or an exception's simple class name,
 * never from an exception message, URL or reason phrase, so it is safe in an ordinary WARN.
 *
 * <p>A driver whose {@link DeleteVerificationProbe#presence} knows the cause reports it with
 * {@link #report}; the default {@link DeleteVerificationProbe#observe} collects it for that one call.
 */
public final class DeleteVerificationCause {
	private static final int MAX_CLASS_NAME_LENGTH = 64;
	private static final DeleteVerificationCause NO_RESPONSE = new DeleteVerificationCause(
					"no HEAD response (the connection closed or the response timed out)");
	private static final ThreadLocal<Capture> CAPTURE = new ThreadLocal<>();

	private final String text;

	private DeleteVerificationCause(final String text) {
		this.text = text;
	}

	/** The target answered with an HTTP status that classifies neither presence nor absence. */
	public static DeleteVerificationCause httpStatus(final int code) {
		return code >= 100 && code <= 999
						? new DeleteVerificationCause("HTTP " + code)
						: new DeleteVerificationCause("HTTP status out of range");
	}

	/** No response arrived: the connection closed or the response timed out. */
	public static DeleteVerificationCause noResponse() {
		return NO_RESPONSE;
	}

	/** The connection could not be made or failed before a response. */
	public static DeleteVerificationCause connectionFailure(final Throwable failure) {
		return new DeleteVerificationCause("connection failure (" + className(failure) + ")");
	}

	/** An I/O failure other than a connection failure. */
	public static DeleteVerificationCause ioError(final Throwable failure) {
		return new DeleteVerificationCause("I/O error (" + className(failure) + ")");
	}

	/** Any other failure of the probe or its client library. */
	public static DeleteVerificationCause probeError(final Throwable failure) {
		return new DeleteVerificationCause("probe error (" + className(failure) + ")");
	}

	/**
	 * Records {@code cause} for the {@link DeleteVerificationProbe#observe} call running on this
	 * thread, if any; otherwise does nothing. It works only on the thread running
	 * {@link DeleteVerificationProbe#presence}: a cause reported from another thread, such as a
	 * request's completion callback, is dropped, and the observation reads "not reported by the
	 * storage driver".
	 */
	public static void report(final DeleteVerificationCause cause) {
		final Capture capture = CAPTURE.get();
		if (capture != null) {
			capture.cause = cause;
		}
	}

	/** Calls {@code probe.presence(target)} once and returns it with the cause that call reported. */
	static DeleteVerificationProbe.Observation observeThroughPresence(
					final DeleteVerificationProbe probe, final DeleteTarget target) {
		final Capture outer = CAPTURE.get();
		final Capture capture = new Capture();
		CAPTURE.set(capture);
		final DeleteVerificationProbe.Presence presence;
		try {
			presence = probe.presence(target);
		} finally {
			if (outer == null) {
				CAPTURE.remove();
			} else {
				CAPTURE.set(outer);
			}
		}
		return presence == null || presence == DeleteVerificationProbe.Presence.UNRESOLVED
						? new DeleteVerificationProbe.Observation(presence, capture.cause)
						: DeleteVerificationProbe.Observation.of(presence);
	}

	private static String className(final Throwable failure) {
		if (failure == null) {
			return "unknown";
		}
		final String simple = failure.getClass().getSimpleName();
		final StringBuilder safe = new StringBuilder(Math.min(simple.length(), MAX_CLASS_NAME_LENGTH));
		for (int i = 0; i < simple.length() && safe.length() < MAX_CLASS_NAME_LENGTH; i++) {
			final char ch = simple.charAt(i);
			if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_') {
				safe.append(ch);
			}
		}
		return safe.isEmpty() ? "unnamed" : safe.toString();
	}

	@Override
	public String toString() {
		return text;
	}

	@Override
	public boolean equals(final Object other) {
		return other instanceof DeleteVerificationCause cause && cause.text.equals(text);
	}

	@Override
	public int hashCode() {
		return text.hashCode();
	}

	private static final class Capture {
		private DeleteVerificationCause cause;
	}
}
