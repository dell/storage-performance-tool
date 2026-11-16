package com.dell.spt.base.item.op.deletion;

/** Storage-driver capability for checking one current-key or exact-version DELETE identity. */
@FunctionalInterface
public interface DeleteVerificationProbe {

	/** A target is either observable, absent, or not conclusively classified. */
	enum Presence {
		PRESENT((byte) 0), ABSENT((byte) 1), UNRESOLVED((byte) 2);

		private final byte code;

		Presence(final byte code) {
			this.code = code;
		}

		/** Stable single-byte encoding used by the verification ledger. */
		byte code() {
			return code;
		}
	}

	/**
	 * One probe result and, for an unresolved one, why the target could not be classified.
	 *
	 * @param presence the classification; {@code null} counts as {@link Presence#UNRESOLVED}
	 * @param unresolvedCause why an unresolved result was not classified, or {@code null} when unknown
	 */
	record Observation(Presence presence, DeleteVerificationCause unresolvedCause) {

		/** A classified or unresolved result without a reported cause. */
		public static Observation of(final Presence presence) {
			return new Observation(presence, null);
		}

		/** An unresolved result with the reason the target could not be classified. */
		public static Observation unresolved(final DeleteVerificationCause cause) {
			return new Observation(Presence.UNRESOLVED, cause);
		}
	}

	/**
	 * Checks the exact identity represented by {@code target}. This remains the method a driver
	 * implements or overrides; it may report why a result is unresolved with
	 * {@link DeleteVerificationCause#report}.
	 */
	Presence presence(DeleteTarget target);

	/**
	 * Checks {@code target} and reports why an unresolved result was not classified. Verification
	 * calls this method. The default calls {@link #presence} exactly once and collects the cause it
	 * reported; a probe that overrides this method is used as it is, and must not call it from
	 * {@link #presence}.
	 */
	default Observation observe(final DeleteTarget target) {
		return DeleteVerificationCause.observeThroughPresence(this, target);
	}
}
